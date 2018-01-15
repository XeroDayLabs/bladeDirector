using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.ServiceModel;
using System.Threading;
using bladeDirectorWCF.Properties;

namespace bladeDirectorWCF
{
    public class hostDB : IDisposable
    {
        public SQLiteConnection conn;
        private readonly string dbFilename;

        public hostDB(string basePath)
        {
            // Juuuust to make sure
            string sqliteOpts = SQLiteConnection.SQLiteCompileOptions;
            if (!sqliteOpts.Contains("THREADSAFE=1"))
                throw new Exception("This build of SQLite is not threadsafe");

            dbFilename = Path.Combine(basePath, "hoststate.sqlite");

            // If we're making a new file, remember that, since we'll have to create a new schema.
            bool needToCreateSchema = !File.Exists(dbFilename);
            conn = new SQLiteConnection("Data Source=" + dbFilename);
            conn.Open();

            if (needToCreateSchema)
                createTables();
        }

        public hostDB()
        {
            dbFilename = ":memory:";

            conn = new SQLiteConnection("Data Source=" + dbFilename);
            conn.Open();

            createTables();
        }

        private void createTables()
        {
            string[] sqlCommands = Resources.DBCreation.Split(';');

            foreach (string sqlCommand in sqlCommands)
            {
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }
        }

        private void dropDB()
        {
            if (conn != null)
            {
                conn.Close();
                conn.Dispose();
            }

            if (dbFilename != ":memory:")
            {
                DateTime deadline = DateTime.Now + TimeSpan.FromMinutes(1);
                while (true)
                {
                    try
                    {
                        File.Delete(dbFilename);
                        break;
                    }
                    catch (UnauthorizedAccessException)
                    {
                        if (deadline < DateTime.Now)
                            throw;
                        Thread.Sleep(TimeSpan.FromSeconds(2));
                    }
                }
            }

            conn = new SQLiteConnection("Data Source=" + dbFilename);
            conn.Open();
        }

        public string[] getAllBladeIP()
        {
            string sqlCommand = "select bladeIP from bladeConfiguration";
            using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
            {
                using (SQLiteDataReader reader = cmd.ExecuteReader())
                {
                    List<string> toRet = new List<string>();
                    while (reader.Read())
                        toRet.Add((string)reader[0]);
                    return toRet.ToArray();
                }
            }
        }

        public string[] getAllVMIP()
        {
            string sqlCommand = "select VMIP from VMConfiguration";
            using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
            {
                using (SQLiteDataReader reader = cmd.ExecuteReader())
                {
                    List<string> toRet = new List<string>();
                    while (reader.Read())
                        toRet.Add((string)reader[0]);
                    return toRet.ToArray();
                }
            }
        }

        public disposingList<lockableBladeSpec> getAllBladeInfo(Func<bladeSpec, bool> filter, bladeLockType lockTypeRead, bladeLockType lockTypeWrite, bool permitAccessDuringBIOS = false, bool permitAccessDuringDeployment = false, int max = Int32.MaxValue)
        {
            disposingList<lockableBladeSpec> toRet = new disposingList<lockableBladeSpec>();
            foreach (string bladeIP in getAllBladeIP())
            {
                lockableBladeSpec blade = getBladeByIP(bladeIP, lockTypeRead, lockTypeWrite, true, true);
                // Filter out anything as requested
                if (!filter(blade.spec))
                {
                    blade.Dispose();
                    continue;
                }
                // Filter out anything we don't have access to right now, due to BIOS or VM deployments
                if ((!permitAccessDuringDeployment) &&
                    blade.spec.vmDeployState != VMDeployStatus.notBeingDeployed &&
                    blade.spec.vmDeployState != VMDeployStatus.failed &&
                    blade.spec.vmDeployState != VMDeployStatus.readyForDeployment)
                {
                    blade.Dispose();
                    continue;
                }
                if ((!permitAccessDuringBIOS) && blade.spec.currentlyHavingBIOSDeployed)
                {
                    blade.Dispose();
                    continue;
                }

                // Have we hit our maximum yet?
                if (toRet.Count == max)
                {
                    blade.Dispose();
                    continue;
                }

                // Otherwise, okay.
                toRet.Add(blade);
            }
            return toRet;
        }

        public disposingList<lockableVMSpec> getAllVMInfo(Func<vmSpec, bool> filter, bladeLockType lockTypeRead, bladeLockType lockTypeWrite)
        {
            disposingList<lockableVMSpec> toRet = new disposingList<lockableVMSpec>();
            foreach (string bladeIP in getAllVMIP())
            {
                lockableVMSpec VM = getVMByIP(bladeIP, lockTypeRead, lockTypeWrite);
                if (filter(VM.spec))
                    toRet.Add(VM);
                else
                    VM.Dispose();
            }
            return toRet;
        }

        // FIXME: code duplication
        public lockableBladeSpec getBladeByIP(string IP, bladeLockType readLock, bladeLockType writeLock, bool permitAccessDuringBIOS = false, bool permitAccessDuringDeployment = false)
        {
            bladeLockType origReadLock = readLock | writeLock;
            readLock = origReadLock;

            // We need to lock IP addressess, since we're searching by them.
            readLock = readLock | bladeLockType.lockIPAddresses;
            readLock = readLock | bladeLockType.lockvmDeployState;
            readLock = readLock | bladeLockType.lockBIOS;

            lockableBladeSpec toRet = null;
            try
            {
                toRet = new lockableBladeSpec(IP, readLock, writeLock);

                string sqlCommand = "select * from bladeOwnership " +
                                    "join bladeConfiguration on ownershipKey = bladeConfiguration.ownershipID " +
                                    "where bladeIP = $bladeIP";
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    cmd.Parameters.AddWithValue("$bladeIP", IP);
                    using (SQLiteDataReader reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            bladeSpec newSpec = new bladeSpec(conn, reader, readLock, writeLock);
                            toRet.setSpec(newSpec);

                            if ((!permitAccessDuringDeployment) &&
                                newSpec.vmDeployState != VMDeployStatus.notBeingDeployed &&
                                newSpec.vmDeployState != VMDeployStatus.failed &&
                                newSpec.vmDeployState != VMDeployStatus.readyForDeployment)
                                throw new Exception("Attempt to access blade during VM deployment");
                            if ((!permitAccessDuringBIOS) && newSpec.currentlyHavingBIOSDeployed)
                                throw new Exception("Attempt to access blade during BIOS deployment");

                            if ((origReadLock & bladeLockType.lockvmDeployState) == 0 &&
                                (writeLock & bladeLockType.lockvmDeployState) == 0)
                                toRet.downgradeLocks(bladeLockType.lockvmDeployState, bladeLockType.lockNone);

                            if ((origReadLock & bladeLockType.lockBIOS) == 0 &&
                                (writeLock & bladeLockType.lockBIOS) == 0)
                                toRet.downgradeLocks(bladeLockType.lockBIOS, bladeLockType.lockNone);

                            leakCheckerInspector.monitorDisposable(toRet);
                            return toRet;
                        }
                        // No records returned.
                        throw new bladeNotFoundException();
                    }
                }
            }
            catch (Exception)
            {
                if (toRet != null)
                    toRet.Dispose();
                throw;
            }
        }

        // FIXME: code duplication ^^
        public bladeSpec getBladeByIP_withoutLocking(string IP)
        {
            string sqlCommand = "select * from bladeOwnership " +
                                "join bladeConfiguration on ownershipKey = bladeConfiguration.ownershipID " +
                                "where bladeIP = $bladeIP";
            using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
            {
                cmd.Parameters.AddWithValue("$bladeIP", IP);
                using (SQLiteDataReader reader = cmd.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        return new bladeSpec(conn, reader, bladeLockType.lockAll, bladeLockType.lockAll);
                    }
                    // No records returned.
                    throw new bladeNotFoundException();
                }
            }
        }

        public lockableVMSpec getVMByIP(string bladeName, bladeLockType readLock, bladeLockType writeLock)
        {
            // We need to lock IP addressess, since we're searching by them.
            readLock = readLock | bladeLockType.lockIPAddresses;

            lockableVMSpec toRet = new lockableVMSpec(bladeName, readLock, writeLock);

            try
            {
                string sqlCommand = "select * from bladeOwnership " +
                                    "join VMConfiguration on ownershipKey = VMConfiguration.ownershipID " +
                                    "where VMConfiguration.VMIP = $VMIP";
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    cmd.Parameters.AddWithValue("$VMIP", bladeName);
                    using (SQLiteDataReader reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            toRet.setSpec(new vmSpec(conn, reader, readLock, writeLock));
                            return toRet;
                        }

                        // No records returned.
                        toRet.Dispose();
                        return null;
                    }
                }

            }
            catch (Exception)
            {
                toRet.Dispose();

                throw;
            }
        }

        public disposingList<lockableVMSpec> getVMByVMServerIP(lockableBladeSpec blade, bladeLockType readLock,
            bladeLockType writeLock)
        {
            disposingList<lockableVMSpec> toRet = new disposingList<lockableVMSpec>();

            if ((blade.getCurrentLocks().read & bladeLockType.lockVMCreation) == 0)
                throw new Exception("lockVMCreation required on vmserver passed to getVMByVMServerIP");

            // We need to lock IP addressess on the VMs, since we lock by them.
            readLock = readLock | bladeLockType.lockIPAddresses;

            // Since we hold lockVMCreation, we can assume no new VMs will be added or removed to/from this blade. We assume that
            // VM IP addresses will never change, except during initialization, when they go from null - we just drop any with a
            // NULL IP address.

            Dictionary<string, lockableVMSpec> VMNames = new Dictionary<string, lockableVMSpec>();
            string sqlCommand = "select VMIP from vmConfiguration " +
                                "join bladeConfiguration on parentbladeID = bladeConfigKey " +
                                "where bladeIP = $vmServerIP";
            using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
            {
                cmd.Parameters.AddWithValue("$vmServerIP", blade.spec.bladeIP);
                using (SQLiteDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string VMName = reader[0].ToString();
                        if (!String.IsNullOrEmpty(VMName))
                            VMNames.Add(VMName, new lockableVMSpec(VMName, readLock, writeLock));
                    }
                }
            }

            try
            {
                // Now read each from the DB, now that we hold the lock for each.
                foreach (KeyValuePair<string, lockableVMSpec> kvp in VMNames)
                {
                    string vmName = kvp.Key;
                    lockableVMSpec vmSpec = kvp.Value;

                    string sql_getVM = "select bladeOwnership.*, vmConfiguration.* from vmConfiguration " +
                                       " join bladeOwnership on bladeOwnership.ownershipKey = vmConfiguration.ownershipID " +
                                       " join bladeConfiguration on parentbladeID = bladeConfigKey " +
                                       " where VMIP = $vmIP";

                    using (SQLiteCommand cmd = new SQLiteCommand(sql_getVM, conn))
                    {
                        cmd.Parameters.AddWithValue("$vmIP", vmName);

                        using (SQLiteDataReader reader = cmd.ExecuteReader())
                        {
                            if (!reader.Read())
                                throw new Exception("VM disappeared, even though we hold lockVMCreation on the parent!");

                            vmSpec.setSpec(new vmSpec(conn, reader, readLock, writeLock));
                            toRet.Add(vmSpec);
                        }
                    }

                }
            }
            catch (Exception)
            {
                foreach (KeyValuePair<string, lockableVMSpec> kvp in VMNames)
                {
                    kvp.Value.Dispose();
                }
                throw;
            }
            return toRet;
        }

        // Fixme: code duplication ^^
        public vmSpec getVMByIP_withoutLocking(string bladeName)
        {
            string sqlCommand = "select * from bladeOwnership " +
                                "join VMConfiguration on ownershipKey = VMConfiguration.ownershipID " +
                                "where VMConfiguration.VMIP = $VMIP";
            using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
            {
                cmd.Parameters.AddWithValue("$VMIP", bladeName);
                using (SQLiteDataReader reader = cmd.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        return new vmSpec(conn, reader, bladeLockType.lockAll, bladeLockType.lockAll);
                    }
                    // No records returned.
                    return null;
                }
            }
        }

        public currentOwnerStat[] getFairnessStats(disposingList<lockableBladeSpec> blades )
        {
            List<currentOwnerStat> bladeStats = getFairnessStatsForBlades(blades).ToList();
            if (bladeStats.Any(x => bladeStats.Count(y => x.ownerName == y.ownerName) == 0))
                throw new Exception("Not passed enough locks!");

            // Now add VM stats. 
            foreach (lockableBladeSpec blade in blades)
            {
                using (disposingList<lockableVMSpec> vms = getVMByVMServerIP(blade,
                    bladeLockType.lockOwnership | bladeLockType.lockVMCreation | bladeLockType.lockVirtualHW,
                    bladeLockType.lockNone))
                {
                    foreach (lockableVMSpec vm in vms)
                    {
                        string owner;
                        //if (vm.spec.state == bladeStatus.inUse || vm.spec.state == bladeStatus.inUseByDirector)
                        {
                            // During deployment, the VM is allocated to the VMServer, with the real requestor queued in the
                            // nextOwner. We count ownership quota against the nextOwner.
                            owner = vm.spec.currentOwner;
                            if (vm.spec.currentOwner == "vmserver")
                            {
                                owner = vm.spec.nextOwner;
                                if (string.IsNullOrEmpty(owner))
                                {
                                    // if this is empty, then this VM is not yet created and thus can't be owned.
                                    // It shouldn't be in the DB if it has no owner, unless the blade is not locked properly.
                                    throw new Exception("VM has 'vmserver' owner but no queued owner");
                                }
                            }
                            if (!vm.spec.ownershipRowID.HasValue)
                                throw new Exception("VM " + vm.spec.VMIP + " has no ownership row ID!?");
                            if (string.IsNullOrEmpty(owner))
                            {
                                // Likewise, this should be impossible, because we hold the VMCreation read lock.
                                throw new Exception("VM " + vm.spec.VMIP + " has no owner!?");
                            }
                        }

                        int cpuCount = vm.spec.cpuCount;
                        int memoryMB = vm.spec.memoryMB;

                        float pct = bladeSpec.asPercentageOfCapacity(cpuCount, memoryMB)/100f;

                        if (bladeStats.Count(x => x.ownerName == owner) == 0)
                            bladeStats.Add(new currentOwnerStat(owner, 0));
                        bladeStats.Single(x => x.ownerName == owner).allocatedBlades += (pct * 100 / 1);
                    }
                }
            }

            bladeStats.RemoveAll(x => x.ownerName == "vmserver");
            return bladeStats.ToArray();
        }

        public currentOwnerStat[] getFairnessStatsForBlades(disposingList<lockableBladeSpec> blades)
        {
            Dictionary<string, currentOwnerStat> ownershipByOwnerIP = new Dictionary<string, currentOwnerStat>();

            // TODO: check .state and omit release-requested blades
            foreach (lockableBladeSpec blade in blades)
            {
                if (!string.IsNullOrEmpty(blade.spec.currentOwner) && !ownershipByOwnerIP.ContainsKey(blade.spec.currentOwner))
                    ownershipByOwnerIP.Add(blade.spec.currentOwner, new currentOwnerStat(blade.spec.currentOwner, 0));

                if (!string.IsNullOrEmpty(blade.spec.nextOwner) && !ownershipByOwnerIP.ContainsKey(blade.spec.nextOwner))
                    ownershipByOwnerIP.Add(blade.spec.nextOwner, new currentOwnerStat(blade.spec.nextOwner, 0));

                // We don't count any blades which are in 'release requested' as owned by the current owner - we count them as owned
                // by the queued owner.
                if (blade.spec.state == bladeStatus.releaseRequested)
                {
                    if (string.IsNullOrEmpty(blade.spec.nextOwner))
                        throw new Exception("Blade has no .nextOwner but is in releaseRequested state");
                    ownershipByOwnerIP[blade.spec.nextOwner].allocatedBlades++;
                }
                else if (blade.spec.state == bladeStatus.inUse || blade.spec.state == bladeStatus.inUseByDirector)
                {
                    ownershipByOwnerIP[blade.spec.currentOwner].allocatedBlades++;
                }
            }

            return ownershipByOwnerIP.Values.ToArray();
        }

        public vmSpec getVMByDBID_nolocking(long VMID)
        {
            string sqlCommand = "select * from bladeOwnership " +
                                "join VMConfiguration on ownershipKey = VMConfiguration.ownershipID " +
                                "where vmConfigKey = $VMID";
            using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
            {
                cmd.Parameters.AddWithValue("$VMID", VMID);
                using (SQLiteDataReader reader = cmd.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        return new vmSpec(conn, reader, bladeLockType.lockAll, bladeLockType.lockAll);
                    }
                    // No records returned.
                    return null;
                }
            }
        }

        public List<vmSpec> getVMByVMServerIP_nolocking(string vmServerIP)
        {
            List<long> VMIDs = new List<long>();
            string sqlCommand = "select vmConfigKey from vmConfiguration " +
                                "join bladeConfiguration on parentbladeID = bladeConfigKey " +
                                "where bladeIP = $vmServerIP";
            using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
            {
                cmd.Parameters.AddWithValue("$vmServerIP", vmServerIP);
                using (SQLiteDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                        VMIDs.Add((long)reader["vmConfigKey"]);
                }
            }

            List<vmSpec> toRet = new List<vmSpec>();
            foreach (int vmID in VMIDs)
                toRet.Add(getVMByDBID_nolocking(vmID));

            return toRet;
        }

        public string[] getBladesByAllocatedServer(string NodeIP)
        {
            string sqlCommand = "select bladeIP from bladeOwnership " +
                                "join bladeConfiguration on ownershipKey = bladeConfiguration.ownershipID " +
                                "where bladeOwnership.currentOwner = $bladeOwner";
            using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
            {
                cmd.Parameters.AddWithValue("$bladeOwner", NodeIP);
                using (SQLiteDataReader reader = cmd.ExecuteReader())
                {
                    List<string> toRet = new List<string>(10);
                    while (reader.Read())
                        toRet.Add((string)reader["bladeIP"]);
                    return toRet.ToArray();
                }
            }
        }

        public disposingListOfBladesAndVMs getBladesAndVMs(Func<bladeSpec, bool> BladeFilter, Func<vmSpec, bool> VMFilter, bladeLockType lockTypeRead, bladeLockType lockTypeWrite, bool permitAccessDuringBIOS = false, bool permitAccessDuringDeployment = false)
        {
            disposingListOfBladesAndVMs toRet = new disposingListOfBladesAndVMs();
            toRet.blades = getAllBladeInfo(BladeFilter, lockTypeRead, lockTypeWrite, permitAccessDuringBIOS, permitAccessDuringDeployment);
            toRet.VMs = getAllVMInfo(VMFilter, lockTypeRead, lockTypeWrite);

            return toRet;
        }

        public GetBladeStatusResult getBladeStatus(string nodeIp, string requestorIp)
        {
            using (lockableBladeSpec blade = getBladeByIP(nodeIp, bladeLockType.lockOwnership, bladeLockType.lockNone,
                permitAccessDuringBIOS: true, permitAccessDuringDeployment: true))
            {
                switch (blade.spec.state)
                {
                    case bladeStatus.unused:
                        return GetBladeStatusResult.unused;
                    case bladeStatus.releaseRequested:
                        return GetBladeStatusResult.releasePending;
                    case bladeStatus.inUse:
                        if (blade.spec.currentOwner == requestorIp)
                            return GetBladeStatusResult.yours;
                        return GetBladeStatusResult.notYours;
                    case bladeStatus.inUseByDirector:
                        return GetBladeStatusResult.notYours;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
        }

        // TODO: reduce duplication with above
        public GetBladeStatusResult getVMStatus(string nodeIp, string requestorIp)
        {
            using (lockableVMSpec blade = getVMByIP(nodeIp, bladeLockType.lockOwnership, bladeLockType.lockNone))
            {
                switch (blade.spec.state)
                {
                    case bladeStatus.unused:
                        return GetBladeStatusResult.unused;
                    case bladeStatus.releaseRequested:
                        return GetBladeStatusResult.releasePending;
                    case bladeStatus.inUse:
                        if (blade.spec.currentOwner == requestorIp)
                            return GetBladeStatusResult.yours;
                        return GetBladeStatusResult.notYours;
                    case bladeStatus.inUseByDirector:
                        return GetBladeStatusResult.notYours;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
        }

        public void initWithBlades(bladeSpec[] bladeSpecs)
        {
            dropDB();
            createTables();

            // Since we disposed and recreated the DBConnection, we'll need to update each bladeSpec with the new one, and
            // blow away any DB IDs.
            foreach (bladeSpec spec in bladeSpecs)
            {
                spec.conn = conn;
                spec.ownershipRowID = null;
                spec.bladeID = null;
            }

            foreach (bladeSpec spec in bladeSpecs)
                addNode(spec);
        }            

        public void addNode(bladeOwnership spec)
        {
            spec.createOrUpdateInDB();
        }

        public void makeIntoAVMServer(lockableBladeSpec toConvert)
        {
            // Delete any VM configurations that have been left lying around.
            string sql = "select bladeConfigKey from VMConfiguration " +
                            " join BladeConfiguration on  BladeConfigKey = ownershipKey " +
                            "join bladeOwnership on VMConfiguration.parentBladeID = ownershipKey " +
                            " where bladeConfigKey = $bladeIP";
            List<long> toDel = new List<long>();
            using (SQLiteCommand cmd = new SQLiteCommand(sql, conn))
            {
                cmd.Parameters.AddWithValue("$bladeIP", toConvert.spec.bladeIP);
                using (SQLiteDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        toDel.Add((long) reader[0]);
                    }
                }
            }

            string deleteSQL = "delete from VMConfiguration where id in (" + String.Join(",", toDel) + ")";
            using (SQLiteCommand cmd = new SQLiteCommand(deleteSQL, conn))
            {
                cmd.ExecuteNonQuery();
            }

            // And then mark this blade as being a VM server.
            toConvert.spec.currentlyBeingAVMServer = true;
            toConvert.spec.state = bladeStatus.inUseByDirector;
            // Since we don't know if the blade has been left in a good state (or even if it was a VM server previously) we 
            // force a power cycle before we use it.
            toConvert.spec.vmDeployState = VMDeployStatus.needsPowerCycle;
        }

        public void refreshKeepAliveForRequestor(string requestorIP)
        {
            string sqlCommand = "update bladeOwnership set lastKeepAlive=$NOW " +
                                "where currentOwner = $ownerIP";
            using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
            {
                cmd.Parameters.AddWithValue("$ownerIP", requestorIP);
                cmd.Parameters.AddWithValue("$NOW", DateTime.Now.ToString());
                cmd.ExecuteNonQuery();
            }
        }

        public vmserverTotals getVMServerTotals(bladeSpec blade)
        {
            // You should hold a lock on the VM server before calling this, to ensure the result doesn't change before you get
            // a chance to use it.
            if ((blade.permittedAccessRead & bladeLockType.lockVMCreation) == bladeLockType.lockNone)
                throw new Exception("lockVMCreation is needed when calling .getVMServerTotals");

            string sqlCommand = "select sum(cpucount) as cpus, sum(memoryMB) as ram, count(*) as VMs " +
                                " from vmConfiguration " +
                                "join bladeConfiguration on parentbladeID = bladeConfigKey " +
                                "where bladeIP = $vmServerIP" + 
                                " and isWaitingForResources = 0 ";
            using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
            {
                cmd.Parameters.AddWithValue("$vmServerIP", blade.bladeIP);
                using (SQLiteDataReader reader = cmd.ExecuteReader())
                {
                    if (!reader.Read())
                        throw new Exception();
                    return new vmserverTotals(reader);
                }                    
            }
        }

        public void Dispose()
        {
            conn.Dispose();
        }
    }

    public class currentOwnerStat
    {
        public string ownerName;
        public float allocatedBlades;

        public currentOwnerStat(string newOwnerName, int newAllocatedBlades )
        {
            ownerName = newOwnerName;
            allocatedBlades = newAllocatedBlades;
        }
    }

    public class vmserverTotals
    {
        public readonly int cpus;
        public readonly int ram;
        public readonly int VMs;

        public vmserverTotals(int newCpus, int newRAM, int newVMs)
        {
            cpus = newCpus;
            ram = newRAM;
            VMs = newVMs;
        }

        public vmserverTotals(SQLiteDataReader row)
        {
            cpus = ram = VMs = 0;
            if (!(row["cpus"] is DBNull))
                cpus = Convert.ToInt32((long)row["cpus"]);
            if (!(row["ram"] is DBNull))
                ram = Convert.ToInt32((long)row["ram"]);
            if (!(row["VMs"] is DBNull))
                VMs = Convert.ToInt32( (long)row["VMs"]);
        }
    }
}