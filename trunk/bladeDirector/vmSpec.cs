using System;
using System.Data.SQLite;
using System.Xml.Serialization;
using bladeDirector.Properties;
using createDisks;

namespace bladeDirector
{
    [XmlInclude(typeof (bladeOwnership))]
    public class vmSpec : bladeOwnership
    {
        public long parentBladeID;
        public string parentBladeIP;
        public string iscsiIP;
        public string VMIP;
        public long? vmConfigKey;
        public string eth0MAC;
        public string eth1MAC;
        public string displayName;
        public int indexOnServer;

        public ushort kernelDebugPort;
        public string kernelDebugKey;

        // These two properties are accessed by the client via the XML response. They must be public and non-read-only.
        // ReSharper disable MemberCanBePrivate.Global
        // ReSharper disable NotAccessedField.Global
        public string username;
        public string password;
        // ReSharper restore NotAccessedField.Global
        // ReSharper restore MemberCanBePrivate.Global

        public VMHardwareSpec hwSpec;

        public vmSpec(VMDeployStatus deployState)
            : base(deployState,bladeLockType.lockAll)
        {
            vmConfigKey = null;
        }

        public vmSpec(SQLiteDataReader reader)
            : base(reader, bladeLockType.lockAll)
        {
            parentBladeID = (long)reader["parentBladeID"];
            if (!(reader["parentBladeIP"] is DBNull))
                parentBladeIP = (string)reader["parentBladeIP"];
            vmConfigKey = (long)reader["vmConfigKey"];
            if (!(reader["iscsiIP"] is DBNull))
                iscsiIP = (string)reader["iscsiIP"];
            if (!(reader["VMIP"] is DBNull))
                VMIP = (string)reader["VMIP"];
            if (!(reader["eth0MAC"] is DBNull))
                eth0MAC = (string)reader["eth0MAC"];
            if (!(reader["eth1MAC"] is DBNull))
                eth1MAC = (string)reader["eth1MAC"];
            if (!(reader["displayName"] is DBNull))
                displayName = (string)reader["displayName"];
            if (!(reader["kernelDebugPort"] is DBNull))
                kernelDebugPort = Convert.ToUInt16(reader["kernelDebugPort"]);
            if (!(reader["kernelDebugKey"] is DBNull))
                kernelDebugKey = (string)reader["kernelDebugKey"];
            if (!(reader["indexOnServer"] is DBNull))
                indexOnServer = Convert.ToInt32(reader["indexOnServer"]);
            

            username = Settings.Default.vmUsername;
            password = Settings.Default.vmPassword;

            hwSpec = new VMHardwareSpec(reader);
        }

        public override void createInDB(SQLiteConnection conn)
        {
            base.createInDB(conn);

            const string cmd_bladeConfig = "insert into VMConfiguration" +
                                           "(ownershipID, parentBladeID, parentBladeIP, memoryMB, cpuCount, vmConfigKey, VMIP, iscsiip, eth0mac, eth1mac, displayname, kernelDebugPort, kernelDebugKey, indexOnServer )" +
                                           " VALUES " +
                                           "($ownershipID, $parentBladeID, $parentBladeIP, $memoryMB, $cpuCount, $vmConfigKey, $VMIP, $iscsiip, $eth0mac, $eth1mac, $displayname, $kernelDebugPort, $kernelDebugKey, $indexOnServer )";
            using (SQLiteCommand cmd = new SQLiteCommand(cmd_bladeConfig, conn))
            {
                cmd.Parameters.AddWithValue("$ownershipID", ownershipRowID);
                cmd.Parameters.AddWithValue("$parentBladeID", parentBladeID);
                cmd.Parameters.AddWithValue("$parentBladeIP", parentBladeIP);
                cmd.Parameters.AddWithValue("$memoryMB", hwSpec.memoryMB);
                cmd.Parameters.AddWithValue("$cpuCount", hwSpec.cpuCount);
                cmd.Parameters.AddWithValue("$vmConfigKey", vmConfigKey);
                cmd.Parameters.AddWithValue("$vmIP", VMIP);
                cmd.Parameters.AddWithValue("$iscsiIP", iscsiIP);
                cmd.Parameters.AddWithValue("$eth0MAC", eth0MAC);
                cmd.Parameters.AddWithValue("$eth1MAC", eth1MAC);
                cmd.Parameters.AddWithValue("$displayName", displayName);
                cmd.Parameters.AddWithValue("$kernelDebugPort", kernelDebugPort);
                cmd.Parameters.AddWithValue("$kernelDebugKey", kernelDebugKey);
                cmd.Parameters.AddWithValue("$indexOnServer", indexOnServer);

                cmd.ExecuteNonQuery();
                vmConfigKey = (int)conn.LastInsertRowId;
            }
        }

        public override void deleteInDB(SQLiteConnection conn)
        {
            base.deleteInDB(conn);

            const string cmd_bladeConfig = "delete from VMConfiguration where vmConfigKey = $vmConfigKey";
            using (SQLiteCommand cmd = new SQLiteCommand(cmd_bladeConfig, conn))
            {
                cmd.Parameters.AddWithValue("$vmConfigKey", vmConfigKey);
                cmd.ExecuteNonQuery();
            }
        }

        public override void updateInDB(SQLiteConnection conn)
        {
            base.updateInDB(conn);

            if (vmConfigKey == null)
            {
                createInDB(conn);
                return;
            }

            const string cmd_bladeConfig = "update VMConfiguration set " +
                                           " ownershipID=$ownershipID, " +
                                           " parentBladeID=$parentBladeID, " +
                                           " parentBladeIP=$parentBladeIP, " +
                                           " memoryMB=$memoryMB, " +
                                           " cpuCount=$cpuCount, " +
                                           " VMIP=$VMIP, " +
                                           " iscsiIP=$iscsiIP, " +
                                           " eth0MAC=$eth0MAC, " +
                                           " eth1MAC=$eth1MAC, " +
                                           " displayName=$displayName, " +
                                           " kernelDebugPort=$kernelDebugPort, " +
                                           " kernelDebugKey=$kernelDebugKey, " +
                                           " indexOnServer=$indexOnServer  " +
                                           " where vmConfigKey = $vmConfigKey";
            using (SQLiteCommand cmd = new SQLiteCommand(cmd_bladeConfig, conn))
            {
                cmd.Parameters.AddWithValue("$ownershipID", ownershipRowID);
                cmd.Parameters.AddWithValue("$parentBladeID", parentBladeID);
                cmd.Parameters.AddWithValue("$parentBladeIP", parentBladeIP);
                cmd.Parameters.AddWithValue("$memoryMB", hwSpec.memoryMB);
                cmd.Parameters.AddWithValue("$cpuCount", hwSpec.cpuCount);
                cmd.Parameters.AddWithValue("$vmConfigKey", vmConfigKey);
                cmd.Parameters.AddWithValue("$vmIP", VMIP);
                cmd.Parameters.AddWithValue("$iscsiIP", iscsiIP);
                cmd.Parameters.AddWithValue("$eth0MAC", eth0MAC);
                cmd.Parameters.AddWithValue("$eth1MAC", eth1MAC);
                cmd.Parameters.AddWithValue("$displayName", displayName);
                cmd.Parameters.AddWithValue("$kernelDebugPort", kernelDebugPort);
                cmd.Parameters.AddWithValue("$kernelDebugKey", kernelDebugKey);
                cmd.Parameters.AddWithValue("$indexOnServer", indexOnServer);
                
                cmd.ExecuteNonQuery();
                vmConfigKey = (int)conn.LastInsertRowId;
            }

            return;
        }
 
        public override itemToAdd toItemToAdd(bool useNextOwner = false)
        {
            itemToAdd toRet = new itemToAdd();

            // If this is set, we should ignore the 'currentOwner' and use the 'nextOwner' instead. This is used when we are 
            // preparing a VM for another blade, and have ownership temporarily assigned to the blade director itself.
            string owner;
            if (useNextOwner) 
                owner = nextOwner;
            else 
                owner = currentOwner;

            toRet.cloneName = VMIP + "-" + owner + "-" + currentSnapshot;
            toRet.serverIP = owner;
            toRet.snapshotName = currentSnapshot;
            toRet.bladeIP = VMIP;
            toRet.computerName = displayName;
            toRet.kernelDebugPort = kernelDebugPort;
            toRet.kernelDebugKey = kernelDebugKey;
            toRet.isVirtualMachine = true;

            return toRet;
        }
    }
}