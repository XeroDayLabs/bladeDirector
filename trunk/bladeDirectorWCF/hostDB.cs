using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Threading;
using bladeDirectorWCF.Properties;

namespace bladeDirectorWCF
{
    public class hostDB : IDisposable
    {
        private Object connLock = new object();
        public SQLiteConnection conn;
        private string dbFilename;

        public hostDB(string basePath)
        {
            // Juuuust to make sure
            string sqliteOpts = SQLiteConnection.SQLiteCompileOptions;
            if (!sqliteOpts.Contains("THREADSAFE=1"))
                throw new Exception("This build of SQLite is not threadsafe");

            //lock (connLock)
            {
                dbFilename = Path.Combine(basePath, "hoststate.sqlite");

                // If we're making a new file, remember that, since we'll have to create a new schema.
                bool needToCreateSchema = !File.Exists(dbFilename);
                conn = new SQLiteConnection("Data Source=" + dbFilename);
                conn.Open();

                if (needToCreateSchema)
                    createTables();
            }
        }

        public hostDB()
        {
            //lock (connLock)
            {
                dbFilename = ":memory:";

                conn = new SQLiteConnection("Data Source=" + dbFilename);
                conn.Open();

                createTables();
            }
        }

        private void createTables()
        {
            //lock (connLock)
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
        }

        private void dropDB()
        {
            //lock (connLock)
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
            //lock (connLock)
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

                    // Otherwise, okay.
                    toRet.Add(blade);
                }
                return toRet;
            }
        }

        public disposingList<lockableVMSpec> getAllVMInfo(Func<vmSpec, bool> filter, bladeLockType lockTypeRead, bladeLockType lockTypeWrite)
        {
            //lock (connLock)
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

            lockableBladeSpec toRet = new lockableBladeSpec(conn, IP, readLock, writeLock);

            try
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

                            return toRet;
                        }
                        // No records returned.
                        throw new bladeNotFoundException();
                    }
                }
            }
            catch (Exception)
            {
                toRet.Dispose();
                throw;
            }
        }

        // FIXME: code duplication ^^
        public bladeSpec getBladeByIP_withoutLocking(string IP)
        {
            //lock (connLock)
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
        }

        public lockableVMSpec getVMByIP(string bladeName, bladeLockType readLock, bladeLockType writeLock)
        {
            //lock (connLock)
            {
                // We need to lock IP addressess, since we're searching by them.
                readLock = readLock | bladeLockType.lockIPAddresses;

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
                            return new lockableVMSpec(conn, reader, readLock, writeLock);
                        }
                        // No records returned.
                        return null;
                    }
                }
            }
        }
        // Fixme: code duplication ^^
        public vmSpec getVMByIP_withoutLocking(string bladeName)
        {
            //lock (connLock)
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
        }

        public lockableVMSpec getVMByDBID(long VMID)
        {
            //lock (connLock)
            {
                return new lockableVMSpec(conn, getVMByDBID_nolocking(VMID));
            }
        }

        public vmSpec getVMByDBID_nolocking(long VMID)
        {
            //lock (connLock)
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
        }

        public disposingList<lockableVMSpec> getVMByVMServerIP(string vmServerIP)
        {
            //lock (connLock)
            {
                List<vmSpec> VMs = getVMByVMServerIP_nolocking(vmServerIP);
                disposingList<lockableVMSpec> toRet = new disposingList<lockableVMSpec>();
                foreach (vmSpec vmSpec in VMs)
                    toRet.Add(new lockableVMSpec(conn, vmSpec));
                return toRet;
            }
        }

        public List<vmSpec> getVMByVMServerIP_nolocking(string vmServerIP)
        {
            //lock (connLock)
            {
                List<long> VMIDs = new List<long>();
                string sqlCommand = "select vmConfigKey from vmConfiguration " +
                                    "join bladeConfiguration on parentbladeID = bladeConfigKey " +
//                                    "join bladeownership on bladeownership.ownershipKey = vmConfiguration.ownershipID " +
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
        }

        public string[] getBladesByAllocatedServer(string NodeIP)
        {
            //lock (connLock)
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
        }

        public disposingListOfBladesAndVMs getBladesAndVMs(Func<bladeSpec, bool> BladeFilter, Func<vmSpec, bool> VMFilter, bladeLockType lockTypeRead, bladeLockType lockTypeWrite, bool permitAccessDuringBIOS = false, bool permitAccessDuringDeployment = false)
        {
            disposingListOfBladesAndVMs toRet = new disposingListOfBladesAndVMs();
            //lock (connLock)
            {
                toRet.blades = getAllBladeInfo(BladeFilter, lockTypeRead, lockTypeWrite, permitAccessDuringBIOS, permitAccessDuringDeployment);
                toRet.VMs = getAllVMInfo(VMFilter, lockTypeRead, lockTypeWrite);
            }
            return toRet;
        }

        public GetBladeStatusResult getBladeStatus(string nodeIp, string requestorIp)
        {
            //lock (connLock)
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
        }

        public void initWithBlades(bladeSpec[] bladeSpecs)
        {
            //lock (connLock)
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
        }

        public void addNode(bladeOwnership spec)
        {
            spec.createOrUpdateInDB();
        }

        public void makeIntoAVMServer(lockableBladeSpec toConvert)
        {
            //lock (connLock)
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

        public vmserverTotals getVMServerTotalsByVMServerIP(string vmServerIP)
        {
            // You should hold a lock on the VM server before calling this, to ensure the result doesn't change before you get
            // a chance to use it.

            //lock (connLock)
            {
                string sqlCommand = "select sum(cpucount) as cpus, sum(memoryMB) as ram, count(*) as VMs " +
                                    " from vmConfiguration " +
                                    "join bladeConfiguration on parentbladeID = bladeConfigKey " +
                                    "where bladeIP = $vmServerIP";
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    cmd.Parameters.AddWithValue("$vmServerIP", vmServerIP);
                    using (SQLiteDataReader reader = cmd.ExecuteReader())
                    {
                        if (!reader.Read())
                            throw new Exception();
                        return new vmserverTotals(reader);
                    }                    
                }
            }
        }

        public void Dispose()
        {
            conn.Dispose();
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