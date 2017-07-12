using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Threading;
using bladeDirector.Properties;

namespace bladeDirector
{
    public class hostDB
    {
        private Object connLock = new object();
        public SQLiteConnection conn;
        private string dbFilename;

        public hostDB(string basePath)
        {
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

        public disposingList<lockableBladeSpec> getAllBladeInfo(Func<bladeSpec, bool> filter, bladeLockType lockType, int max = Int32.MaxValue)
        {
            //lock (connLock)
            {
                disposingList<lockableBladeSpec> toRet = new disposingList<lockableBladeSpec>();
                foreach (string bladeIP in getAllBladeIP())
                {
                    lockableBladeSpec blade = getBladeByIP(bladeIP, lockType);
                    if (filter(blade.spec))
                        toRet.Add(blade);
                    else
                        blade.Dispose();
                }
                return toRet;
            }
        }

        public List<bladeSpec> getAllBladeInfo_nolocking(Func<bladeSpec, bool> filter, int max = Int32.MaxValue)
        {
            //lock (connLock)
            {
                string sqlCommand = "select * from bladeOwnership " +
                                    "join bladeConfiguration on ownershipKey = bladeConfiguration.ownershipID ";
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    using (SQLiteDataReader reader = cmd.ExecuteReader())
                    {
                        List<bladeSpec> toRet = new List<bladeSpec>();

                        while (reader.Read())
                        {
                            bladeSpec newSpec = new bladeSpec(reader, bladeLockType.lockAll);
                            if (filter(newSpec))
                                toRet.Add(newSpec);
                            if (toRet.Count > max)
                                break;
                        }

                        return toRet;
                    }
                }
            }
        }

        public disposingList<lockableVMSpec> getAllVMInfo(Func<vmSpec, bool> filter)
        {
            //lock (connLock)
            {
                string sqlCommand = "select * from bladeOwnership " +
                                    "join VMConfiguration on ownershipKey = VMConfiguration.ownershipID ";
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    using (SQLiteDataReader reader = cmd.ExecuteReader())
                    {
                        disposingList<lockableVMSpec> toRet = new disposingList<lockableVMSpec>();

                        while (reader.Read())
                        {
                            // Only lock those that the filter wants.
                            vmSpec newVM = new vmSpec(reader);
                            if (filter(newVM))
                                toRet.Add(new lockableVMSpec(conn, reader));
                        }

                        return toRet;
                    }
                }
            }
        }

        // FIXME: code duplication
        public lockableBladeSpec getBladeByIP(string IP, bladeLockType lockType)
        {
            //lock (connLock)
            {
                lockableBladeSpec toRet = new lockableBladeSpec(conn, IP, lockType);

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
                            toRet.setSpec(new bladeSpec(reader, lockType));
                            return toRet;
                        }
                        // No records returned.
                        toRet.Dispose();
                        throw new bladeNotFoundException();
                    }
                }
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
                            return new bladeSpec(reader, bladeLockType.lockAll);
                        }
                        // No records returned.
                        throw new bladeNotFoundException();
                    }
                }
            }
        }

        public lockableVMSpec getVMByIP(string bladeName)
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
                            return new lockableVMSpec(conn, reader);
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
                            return new vmSpec(reader);
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
                            return new vmSpec(reader);
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

        public disposingListOfBladesAndVMs getBladesAndVMs(Func<bladeSpec, bool> BladeFilter, Func<vmSpec, bool> VMFilter, bladeLockType lockType)
        {
            disposingListOfBladesAndVMs toRet = new disposingListOfBladesAndVMs();
            //lock (connLock)
            {
                toRet.blades = getAllBladeInfo(BladeFilter, lockType);
                toRet.VMs = getAllVMInfo(VMFilter);
            }
            return toRet;
        }

        public GetBladeStatusResult getBladeStatus(string nodeIp, string requestorIp)
        {
            //lock (connLock)
            {
                using (lockableBladeSpec blade = getBladeByIP(nodeIp, bladeLockType.lockOwnership))
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

                foreach (bladeSpec spec in bladeSpecs)
                    addNode(spec);
            }            
        }

        public void addNode(bladeSpec spec)
        {
            spec.createInDB(conn);
        }

        private Dictionary<long, int> VmCountPerBlade = new Dictionary<long, int>();
        public lockableVMSpec createChildVM(vmSpec newVM, bladeSpec parent)
        {
            // We must carefully populate .indexOnServer atomically.
            lock (VmCountPerBlade)
            {
                if (VmCountPerBlade.ContainsKey(parent.bladeID))
                {
                    newVM.indexOnServer = ++VmCountPerBlade[parent.bladeID];
                    newVM.createInDB(conn);
                }
                else
                {
                    VmCountPerBlade.Add(parent.bladeID, 0);
                    newVM.indexOnServer = 0;
                    newVM.createInDB(conn);
                }

                return getVMByDBID(newVM.vmConfigKey.Value);
            }
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
                toConvert.spec.VMDeployState = VMDeployStatus.needsPowerCycle;
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