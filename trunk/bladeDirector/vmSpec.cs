using System;
using System.Data.SQLite;
using System.Xml.Serialization;

namespace bladeDirector
{
    [XmlInclude(typeof (bladeOwnership))]
    public class vmSpec : bladeOwnership
    {
        public long parentBladeID;
        public string iscsiIP;
        public string VMIP;
        public int vmSpecID;
        public string eth0MAC;
        public string eth1MAC;
        public string displayName;
        public int indexOnServer;

        public ushort kernelDebugPort;
        public string kernelDebugKey;

        public string username;
        public string password;

        public VMHardwareSpec hwSpec;

        public vmSpec()
        {
            // For XML serialisation
        }

        public vmSpec(SQLiteDataReader reader)
            : base(reader)
        {
            parentBladeID = (long)reader["parentBladeID"];
            if (!(reader["iscsiIP"] is System.DBNull))
                iscsiIP = (string)reader["iscsiIP"];
            if (!(reader["VMIP"] is System.DBNull))
                VMIP = (string)reader["VMIP"];
            if (!(reader["eth0MAC"] is System.DBNull))
                eth0MAC = (string)reader["eth0MAC"];
            if (!(reader["eth1MAC"] is System.DBNull))
                eth1MAC = (string)reader["eth1MAC"];
            if (!(reader["displayName"] is System.DBNull))
                displayName = (string)reader["displayName"];
            if (!(reader["kernelDebugPort"] is System.DBNull))
                kernelDebugPort = Convert.ToUInt16(reader["kernelDebugPort"]);
            if (!(reader["kernelDebugKey"] is System.DBNull))
                kernelDebugKey = (string)reader["kernelDebugKey"];
            if (!(reader["indexOnServer"] is System.DBNull))
                indexOnServer = Convert.ToInt32(reader["indexOnServer"]);
            
            username = Properties.Settings.Default.vmUsername;
            password = Properties.Settings.Default.vmPassword;

            hwSpec = new VMHardwareSpec(reader);
        }

        public override void createInDB(SQLiteConnection conn)
        {
            base.createInDB(conn);

            string cmd_bladeConfig = "insert into VMConfiguration" +
                                     "(ownershipID, parentBladeID, memoryMB, cpuCount, VMIP, iscsiip, eth0mac, eth1mac, displayname, kernelDebugPort, kernelDebugKey, indexOnServer )" +
                                     " VALUES " +
                                     "($ownershipID, $parentBladeID, $memoryMB, $cpuCount, $VMIP, $iscsiip, $eth0mac, $eth1mac, $displayname, $kernelDebugPort, $kernelDebugKey, $indexOnServer )";
            using (SQLiteCommand cmd = new SQLiteCommand(cmd_bladeConfig, conn))
            {
                cmd.Parameters.AddWithValue("$ownershipID", base.ownershipRowID);
                cmd.Parameters.AddWithValue("$parentBladeID", parentBladeID);
                cmd.Parameters.AddWithValue("$memoryMB", hwSpec.memoryMB);
                cmd.Parameters.AddWithValue("$cpuCount", hwSpec.cpuCount);
                cmd.Parameters.AddWithValue("$vmSpecID", vmSpecID);
                cmd.Parameters.AddWithValue("$vmIP", VMIP);
                cmd.Parameters.AddWithValue("$iscsiIP", iscsiIP);
                cmd.Parameters.AddWithValue("$eth0MAC", eth0MAC);
                cmd.Parameters.AddWithValue("$eth1MAC", eth1MAC);
                cmd.Parameters.AddWithValue("$displayName", displayName);
                cmd.Parameters.AddWithValue("$kernelDebugPort", kernelDebugPort);
                cmd.Parameters.AddWithValue("$kernelDebugKey", kernelDebugKey);
                cmd.Parameters.AddWithValue("$indexOnServer", indexOnServer);

                cmd.ExecuteNonQuery();
                vmSpecID = (int)conn.LastInsertRowId;
            }
        }

        public override void deleteInDB(SQLiteConnection conn)
        {
            base.deleteInDB(conn);

            string cmd_bladeConfig = "delete from VMConfiguration where id = $vmSpecID";
            using (SQLiteCommand cmd = new SQLiteCommand(cmd_bladeConfig, conn))
            {
                cmd.Parameters.AddWithValue("$vmSpecID", vmSpecID);
                cmd.ExecuteNonQuery();
            }
        }

        public override resultCode updateInDB(SQLiteConnection conn)
        {
            base.updateInDB(conn);

            string cmd_bladeConfig = "update VMConfiguration set " +
                                     " ownershipID=$ownershipID, " +
                                     " parentBladeID=$parentBladeID, " +
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
                                     " where id = $vmSpecID";
            using (SQLiteCommand cmd = new SQLiteCommand(cmd_bladeConfig, conn))
            {
                cmd.Parameters.AddWithValue("$ownershipID", base.ownershipRowID);
                cmd.Parameters.AddWithValue("$parentBladeID", parentBladeID);
                cmd.Parameters.AddWithValue("$memoryMB", hwSpec.memoryMB);
                cmd.Parameters.AddWithValue("$cpuCount", hwSpec.cpuCount);
                cmd.Parameters.AddWithValue("$vmSpecID", vmSpecID);
                cmd.Parameters.AddWithValue("$vmIP", VMIP);
                cmd.Parameters.AddWithValue("$iscsiIP", iscsiIP);
                cmd.Parameters.AddWithValue("$eth0MAC", eth0MAC);
                cmd.Parameters.AddWithValue("$eth1MAC", eth1MAC);
                cmd.Parameters.AddWithValue("$displayName", displayName);
                cmd.Parameters.AddWithValue("$kernelDebugPort", kernelDebugPort);
                cmd.Parameters.AddWithValue("$kernelDebugKey", kernelDebugKey);
                cmd.Parameters.AddWithValue("$indexOnServer", indexOnServer);
                
                cmd.ExecuteNonQuery();
                vmSpecID = (int)conn.LastInsertRowId;
            }

            return resultCode.success;
        }
 
    }
}