using System.Data.SQLite;
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

        public bladeSpec()
        {
            // For XML serialisation
        }

        public bladeSpec(string newBladeIP, string newISCSIIP, string newILOIP, ushort newILOPort, string newCurrentSnapshot, bool newCurrentlyHavingBIOSDeployed)
        {
            iscsiIP = newISCSIIP;
            bladeIP = newBladeIP;
            iLOPort = newILOPort;
            iLOIP = newILOIP;
            currentSnapshot = newCurrentSnapshot;
            currentlyHavingBIOSDeployed = newCurrentlyHavingBIOSDeployed;
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
                " currentlyHavingBIOSDeployed=$currentlyHavingBIOSDeployed " +
                " where bladeIP = bladeIP";
            using (SQLiteCommand cmd = new SQLiteCommand(cmdConfiguration, conn))
            {
                cmd.Parameters.AddWithValue("$currentSnapshot", currentSnapshot);
                cmd.Parameters.AddWithValue("currentlyHavingBIOSDeployed", currentlyHavingBIOSDeployed);
                cmd.Parameters.AddWithValue("bladeIP", bladeIP);
                if (cmd.ExecuteNonQuery() == 1)
                    return resultCode.success;
            }
            return resultCode.genericFail;
        }
    }
}