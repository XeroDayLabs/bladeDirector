using System;
using System.Data.SQLite;
using System.Diagnostics;
using System.Xml.Serialization;

namespace bladeDirector
{
    [XmlInclude(typeof (bladeSpec))]
    public class bladeOwnership
    {
        public bladeStatus state = bladeStatus.unused;
        public string currentOwner = null;
        public string nextOwner = null;
        public DateTime lastKeepAlive;
        public long? ownershipRowID;
        public string currentSnapshot;

        public bladeOwnership()
        {
            // For xml ser
        }

        protected bladeOwnership(SQLiteDataReader reader)
        {
            long enumIdx = (long)reader["state"];
            state = (bladeStatus)((int)enumIdx);
            ownershipRowID = (long?)reader["bladeOwnershipID"];

            if (!(reader["currentOwner"] is System.DBNull))
                currentOwner = (string)reader["currentOwner"];
            if (!(reader["nextOwner"] is System.DBNull))
                nextOwner = (string)reader["nextOwner"];

            if (reader["currentSnapshot"] is System.DBNull)
                currentSnapshot = "clean";
            else
                currentSnapshot = (string)reader["currentSnapshot"];

            lastKeepAlive = DateTime.Parse((string)reader["lastKeepAlive"]);
        }

        public virtual void createInDB(SQLiteConnection conn)
        {
            string cmd_bladeOwnership = "insert into bladeOwnership " +
                                        "(state, currentOwner, lastKeepAlive, currentSnapshot)" +
                                        " VALUES " +
                                        "($state, $currentOwner, $lastKeepAlive, $currentSnapshot)";
            using (SQLiteCommand cmd = new SQLiteCommand(cmd_bladeOwnership, conn))
            {
                cmd.Parameters.AddWithValue("$state", state);
                cmd.Parameters.AddWithValue("$currentOwner", currentOwner);
                cmd.Parameters.AddWithValue("$lastKeepAlive", lastKeepAlive);
                cmd.Parameters.AddWithValue("$currentSnapshot", currentSnapshot);
                cmd.ExecuteNonQuery();
                ownershipRowID = (int?)conn.LastInsertRowId;
            }
        }

        public virtual resultCode updateInDB(SQLiteConnection conn)
        {
            string sqlCommand;
            if (ownershipRowID.HasValue)
            {
                sqlCommand = "update bladeOwnership set " +
                             "state = $state, currentOwner = $currentOwner, nextOwner = $nextOwner, " +
                             "lastKeepAlive = $lastKeepAlive, currentSnapshot = $currentSnapshot " +
                             "where id = $ownershipID; ";
            }
            else
            {
                sqlCommand = "insert into bladeOwnership (state, currentOwner, nextOwner, lastKeepalive, currentSnapshot) values " +
                             " ( $state, $currentOwner, $nextOwner, $lastKeepAlive, $currentSnapshot) ";

            }
            using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
            {
                cmd.Parameters.AddWithValue("$state", state);
                cmd.Parameters.AddWithValue("$currentOwner", currentOwner);
                cmd.Parameters.AddWithValue("$nextOwner", nextOwner);
                cmd.Parameters.AddWithValue("$lastKeepAlive", lastKeepAlive);
                cmd.Parameters.AddWithValue("$currentSnapshot", currentSnapshot);
                if (ownershipRowID.HasValue)
                    cmd.Parameters.AddWithValue("$ownershipID", ownershipRowID);
                cmd.ExecuteNonQuery();
            }

            return resultCode.success;
        }

        public virtual void deleteInDB(SQLiteConnection conn)
        {
            string cmd_bladeConfig = "delete from bladeOwnership where ID = $id";
            using (SQLiteCommand cmd = new SQLiteCommand(cmd_bladeConfig, conn))
            {
                cmd.Parameters.AddWithValue("$id", ownershipRowID);
                cmd.ExecuteNonQuery();
            }
        }
    }
}