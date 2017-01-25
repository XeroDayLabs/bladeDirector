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

        public bladeOwnership()
        {
            // For xml ser
        }

        public bladeOwnership(bladeSpec spec)
            : base(spec.bladeIP, spec.iscsiIP, spec.iLOIP, spec.iLOPort, spec.currentSnapshot, false, spec.lastDeployedBIOS)
        {
            
        }

        public bladeOwnership(string newIPAddress, string newICSIIP, string newILOIP, ushort newILOPort, string newCurrentSnapshot, string newBIOS)
            : base(newIPAddress, newICSIIP, newILOIP, newILOPort, newCurrentSnapshot, false, newBIOS)
        {
        }

        public bladeOwnership(SQLiteDataReader reader)
            : base(reader)
        {
            bladeID = (long)reader["id"];

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

            lastKeepAlive = DateTime.Parse((string)reader["lastKeepAlive"]);
        }

        public void createInDB(SQLiteConnection conn)
        {
            bladeID = base.createInDB(conn);

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
                                "state = $state, currentOwner = $currentOwner, nextOwner = $nextOwner, " +
                                "lastKeepAlive = $lastKeepAlive " +
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