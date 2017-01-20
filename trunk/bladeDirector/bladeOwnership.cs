using System;
using System.Data.SQLite;
using System.Xml.Serialization;

namespace bladeDirector
{
    [XmlInclude(typeof (bladeSpec))]
    public class bladeOwnership : bladeSpec
    {
        public long bladeID;
        public bladeStatus state = bladeStatus.unused;
        public string currentOwner = null;
        public string nextOwner = null;
        public DateTime lastKeepAlive;
        public string lastDeployedBIOS = null;

        public bladeOwnership()
        {
            // For xml ser
        }

        public bladeOwnership(bladeSpec spec)
            : base(spec.bladeIP, spec.iscsiIP, spec.iLOIP, spec.iLOPort, spec.currentSnapshot, false)
        {
            
        }

        public bladeOwnership(string newIPAddress, string newICSIIP, string newILOIP, ushort newILOPort, string newCurrentSnapshot)
            : base(newIPAddress, newICSIIP, newILOIP, newILOPort, newCurrentSnapshot, false)
        {
        }

        public bladeOwnership(SQLiteDataReader reader)
        {
            iscsiIP = (string)reader["iscsiIP"];
            bladeID = (long)reader["id"];
            bladeIP = (string)reader["bladeIP"];
            iLOPort = ushort.Parse(reader["iLOPort"].ToString());
            iLOIP = (string)reader["iLOIP"];

            long enumIdx = (long)reader["state"];
            state = (bladeStatus) ((int)enumIdx);
            if (reader["currentOwner"] is System.DBNull)
                currentOwner = null;
            else
                currentOwner = (string)reader["currentOwner"];
            if (reader["nextOwner"] is System.DBNull)
                nextOwner = null;
            else
                nextOwner = (string)reader["nextOwner"];

            if (reader["currentSnapshot"] is System.DBNull)
                currentSnapshot = "clean";
            else
                currentSnapshot = (string) reader["currentSnapshot"];

            lastKeepAlive = DateTime.Parse((string)reader["lastKeepAlive"]);
            currentlyHavingBIOSDeployed = (long)reader["currentlyHavingBIOSDeployed"] != 0;
        }

        public void createInDB(SQLiteConnection conn)
        {

            string cmd_bladeConfig = "insert into bladeConfiguration" +
                                     "(iscsiIP, bladeIP, iLoIP, iLOPort, currentSnapshot, currentlyHavingBIOSDeployed)" +
                                     " VALUES " +
                                     "($iscsiIP, $bladeIP, $iLoIP, $iLOPort, $currentSnapshot, $currentlyHavingBIOSDeployed)";
            using (SQLiteCommand cmd = new SQLiteCommand(cmd_bladeConfig, conn))
            {
                cmd.Parameters.AddWithValue("$iscsiIP", iscsiIP);
                cmd.Parameters.AddWithValue("$bladeIP", bladeIP);
                cmd.Parameters.AddWithValue("$iLoIP", iLOIP);
                cmd.Parameters.AddWithValue("$iLOPort", iLOPort);
                cmd.Parameters.AddWithValue("$currentSnapshot", currentSnapshot);
                cmd.Parameters.AddWithValue("$currentlyHavingBIOSDeployed", currentlyHavingBIOSDeployed ? 1 : 0);
                cmd.ExecuteNonQuery();
                bladeID = (long)conn.LastInsertRowId;
            }
            
            string cmd_bladeOwnership = "insert into bladeOwnership " +
                                        "(bladeConfigID, state, currentOwner, lastKeepAlive)" +
                                        " VALUES " +
                                        "($bladeConfigID, $state, $currentOwner, $lastKeepAlive)";
            using (SQLiteCommand cmd = new SQLiteCommand(cmd_bladeOwnership, conn))
            {
                cmd.Parameters.AddWithValue("$bladeConfigID", bladeID);
                cmd.Parameters.AddWithValue("$state", state);
                cmd.Parameters.AddWithValue("$currentOwner", currentOwner);
                cmd.Parameters.AddWithValue("$lastKeepAlive", lastKeepAlive);
                cmd.ExecuteNonQuery();
            }
        }

        public new resultCode updateInDB(SQLiteConnection conn)
        {
            string sqlCommand = "update bladeOwnership set " +
                                "state = $state, currentOwner = $currentOwner, nextOwner = $nextOwner, lastKeepAlive = $lastKeepAlive " +
                                "where bladeConfigID = $bladeID";
            using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
            {
                cmd.Parameters.AddWithValue("$bladeID", bladeID);
                cmd.Parameters.AddWithValue("$state", state);
                cmd.Parameters.AddWithValue("$currentOwner", currentOwner);
                cmd.Parameters.AddWithValue("$nextOwner", nextOwner);
                cmd.Parameters.AddWithValue("$lastKeepAlive", lastKeepAlive);
                cmd.ExecuteNonQuery();
            }

            return base.updateInDB(conn);
        }
    }
}