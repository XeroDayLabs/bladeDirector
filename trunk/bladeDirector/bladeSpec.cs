using System.Data.SQLite;
using System.Runtime.InteropServices;
using System.Xml.Serialization;

namespace bladeDirector
{
    [XmlInclude(typeof(bladeOwnership))]
    public class bladeSpec
    {
        // If you add fields, don't forget to add them to the Equals() override too.
        public string iscsiIP;
        public string bladeIP;
        public string iLOIP;
        public ushort iLOPort;
        public string currentSnapshot;
        public bool currentlyHavingBIOSDeployed = false;
        public string lastDeployedBIOS = null;

        public bladeSpec()
        {
            // For XML serialisation
        }

        public bladeSpec(string newBladeIP, string newISCSIIP, string newILOIP, ushort newILOPort, string newCurrentSnapshot, bool newCurrentlyHavingBIOSDeployed, string newCurrentBIOS)
        {
            iscsiIP = newISCSIIP;
            bladeIP = newBladeIP;
            iLOPort = newILOPort;
            iLOIP = newILOIP;
            currentSnapshot = newCurrentSnapshot;
            currentlyHavingBIOSDeployed = newCurrentlyHavingBIOSDeployed;
            lastDeployedBIOS = newCurrentBIOS;
        }

        protected bladeSpec(SQLiteDataReader reader)
        {
            iscsiIP = (string)reader["iscsiIP"];
            bladeIP = (string)reader["bladeIP"];
            iLOPort = ushort.Parse(reader["iLOPort"].ToString());
            iLOIP = (string)reader["iLOIP"];
            if (reader["currentSnapshot"] is System.DBNull)
                currentSnapshot = "clean";
            else
                currentSnapshot = (string)reader["currentSnapshot"];

            currentlyHavingBIOSDeployed = (long)reader["currentlyHavingBIOSDeployed"] != 0;

            if (reader["lastDeployedBIOS"] is System.DBNull)
                lastDeployedBIOS = null;
            else
                lastDeployedBIOS = (string)reader["lastDeployedBIOS"];
}

        public override bool Equals(object obj)
        {
            bladeSpec compareTo = obj as bladeSpec;
            if (compareTo == null)
                return false;

            if (iscsiIP != compareTo.iscsiIP)
                return false;
            if (bladeIP != compareTo.bladeIP)
                return false;
            if (iLOIP != compareTo.iLOIP)
                return false;
            if (iLOPort != compareTo.iLOPort)
                return false;
            if (currentSnapshot != compareTo.currentSnapshot)
                return false;
            if (currentlyHavingBIOSDeployed != compareTo.currentlyHavingBIOSDeployed)
                return false;

            return true;
        }

        protected resultCode updateInDB(SQLiteConnection conn)
        {
            string cmdConfiguration = "update bladeConfiguration set " +
                " currentSnapshot=$currentSnapshot, " +
                " currentlyHavingBIOSDeployed=$currentlyHavingBIOSDeployed, " +
                " lastDeployedBIOS=$lastDeployedBIOS " +
                " where bladeIP = $bladeIP";
            using (SQLiteCommand cmd = new SQLiteCommand(cmdConfiguration, conn))
            {
                cmd.Parameters.AddWithValue("currentSnapshot", currentSnapshot);
                cmd.Parameters.AddWithValue("currentlyHavingBIOSDeployed", currentlyHavingBIOSDeployed);
                cmd.Parameters.AddWithValue("bladeIP", bladeIP);
                cmd.Parameters.AddWithValue("lastDeployedBIOS", lastDeployedBIOS);
                cmd.ExecuteNonQuery();
            }
            return resultCode.success;
        }

        protected long createInDB(SQLiteConnection conn)
        {
            string cmd_bladeConfig = "insert into bladeConfiguration" +
                                     "(iscsiIP, bladeIP, iLoIP, iLOPort, currentSnapshot, currentlyHavingBIOSDeployed, lastDeployedBIOS)" +
                                     " VALUES " +
                                     "($iscsiIP, $bladeIP, $iLoIP, $iLOPort, $currentSnapshot, $currentlyHavingBIOSDeployed, $lastDeployedBIOS)";
            using (SQLiteCommand cmd = new SQLiteCommand(cmd_bladeConfig, conn))
            {
                cmd.Parameters.AddWithValue("$iscsiIP", iscsiIP);
                cmd.Parameters.AddWithValue("$bladeIP", bladeIP);
                cmd.Parameters.AddWithValue("$iLoIP", iLOIP);
                cmd.Parameters.AddWithValue("$iLOPort", iLOPort);
                cmd.Parameters.AddWithValue("$currentSnapshot", currentSnapshot);
                cmd.Parameters.AddWithValue("$currentlyHavingBIOSDeployed", currentlyHavingBIOSDeployed ? 1 : 0);
                cmd.Parameters.AddWithValue("$lastDeployedBIOS", lastDeployedBIOS);
                cmd.ExecuteNonQuery();
                return (long)conn.LastInsertRowId;
            }            
        }
    }
}