using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SQLite;
using System.Diagnostics;
using System.Linq;
using System.Web.UI.WebControls;
using System.Xml.Serialization;
using bladeDirectorWCF.Properties;

namespace bladeDirectorWCF
{
    [XmlInclude(typeof (bladeSpec))]
    public abstract class bladeOwnership : IDebuggableThing
    {
        [XmlIgnore]
        public SQLiteConnection conn;

        public bladeLockType permittedAccessRead;
        public bladeLockType permittedAccessWrite;

        [XmlIgnore]
        public bladeStatus state
        {
            get { checkPermsR("state"); return _state; } 
            set { checkPermsW("state"); _state = value; }
        }
        private bladeStatus _state;

        [XmlIgnore]
        public string currentOwner
        {
            get { checkPermsR("currentOwner"); return _currentOwner; }
            set { checkPermsW("currentOwner"); _currentOwner = value; }
        }
        private string _currentOwner;

        [XmlIgnore]
        public string nextOwner
        {
            get { checkPermsR("nextOwner"); return _nextOwner; }
            set { checkPermsW("nextOwner"); _nextOwner = value; }
        }
        private string _nextOwner;

        [XmlIgnore]
        public DateTime lastKeepAlive
        {
            get { checkPermsR("lastKeepAlive"); return _lastKeepAlive; } 
            set { checkPermsW("lastKeepAlive"); _lastKeepAlive = value; }
        }
        private DateTime _lastKeepAlive;

        [XmlIgnore]
        public string currentSnapshot
        {
            get { checkPermsR("currentSnapshot"); return _currentSnapshot; } 
            set { checkPermsW("currentSnapshot"); _currentSnapshot = value; }
        }
        private string _currentSnapshot;

        [XmlIgnore]
        public string friendlyName
        {
            get { checkPermsR("friendlyName"); return _friendlyName; }
            set { checkPermsW("friendlyName"); _friendlyName = value; }
        }
        private string _friendlyName;

        [XmlIgnore]
        public ushort kernelDebugPort
        {
            get { checkPermsR("kernelDebugPort"); return _kernelDebugPort; }
            set { checkPermsW("kernelDebugPort"); _kernelDebugPort = value; }
        }
        public ushort _kernelDebugPort;

        [XmlIgnore]
        public string kernelDebugKey
        {
            get { checkPermsR("kernelDebugKey"); return _kernelDebugKey; }
            set { checkPermsW("kernelDebugKey"); _kernelDebugKey = value; }
        }
        private string _kernelDebugKey;

        [XmlIgnore]
        public string availableUsersCSV
        {
            get { checkPermsR("availableUsersCSV"); return _availableUsersCSV; }
            set { checkPermsW("availableUsersCSV"); _availableUsersCSV = value; }
        }
        private string _availableUsersCSV;

        public long? ownershipRowID;

        // These must be public so that the WCF client can access them.
        // ReSharper disable MemberCanBeProtected.Global
        public abstract string kernelDebugAddress { get; }

        [XmlIgnore]
        public userDesc[] credentials
        {
            get { return unpackUsersCSV(); }
        }
        // ReSharper restore MemberCanBeProtected.Global

        protected bladeOwnership()
        {
            // Used for XML de/ser
            permittedAccessRead = bladeLockType.lockAll;
            permittedAccessWrite = bladeLockType.lockAll;
        }

        protected bladeOwnership(SQLiteConnection conn, string friendlyName, VMSoftwareSpec spec, bladeLockType permittedAccessRead, bladeLockType permittedAccessWrite)
            : this(conn, friendlyName, spec.debuggerPort, spec.debuggerKey, permittedAccessRead, permittedAccessWrite)
        {
        }

        protected bladeOwnership(SQLiteConnection conn, string newFriendlyName, ushort newDebugPort, string newDebugKey, bladeLockType permittedAccessRead, bladeLockType permittedAccessWrite)
        {
            this.conn = conn;
            _state = bladeStatus.unused;
            _friendlyName = newFriendlyName;
            _kernelDebugPort = newDebugPort;
            _kernelDebugKey = newDebugKey;
            this.permittedAccessRead = permittedAccessRead;
            this.permittedAccessWrite = permittedAccessWrite;

            if (permittedAccessRead.HasFlag(bladeLockType.lockVirtualHW))
                _availableUsersCSV = makeUsersCSV(new[] { new userDesc(Settings.Default.vmUsername, Settings.Default.vmPassword) });
        }

        protected bladeOwnership(SQLiteConnection conn, SQLiteDataReader reader, bladeLockType permittedAccessRead, bladeLockType permittedAccessWrite)
        {
            this.conn = conn;
            this.permittedAccessRead = permittedAccessRead ;
            this.permittedAccessWrite = permittedAccessWrite;

            if (permittedAccessRead.HasFlag(bladeLockType.lockVirtualHW))
                _availableUsersCSV = makeUsersCSV(new[] { new userDesc(Settings.Default.vmUsername, Settings.Default.vmPassword) });

            parseFromDBRow(reader);
        }

        protected string makeUsersCSV(userDesc[] newUsers)
        {
            string toRet = string.Join(",", newUsers.Select(x =>
                Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(x.password))
                + "," +
                Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(x.password))
                ).ToArray());

            return toRet;
        }

        protected userDesc[] unpackUsersCSV()
        {
//            if (string.IsNullOrEmpty(availableUsersCSV))
//                return new userDesc[0];

            List<userDesc> toRet = new List<userDesc>();

            string[] segments = availableUsersCSV.Split(',');

            for (int i = 0; i < segments.Length; i+=2)
            {
                string usernameb64 = segments[i];
                string passwordb64 = segments[i];

                toRet.Add(new userDesc(
                    System.Text.Encoding.UTF8.GetString(Convert.FromBase64String(usernameb64)),
                    System.Text.Encoding.UTF8.GetString(Convert.FromBase64String(passwordb64))
                ));
            }

            return toRet.ToArray();
        }

        public abstract string getCloneName();

        protected void parseFromDBRow(SQLiteDataReader reader)
        {
            string[] fieldList = new string[reader.FieldCount];
            for (int i = 0; i < reader.FieldCount; i++)
                fieldList[i] = reader.GetName(i);

            if (fieldList.Contains("kernelDebugKey") && !(reader["kernelDebugKey"] is DBNull))
                _kernelDebugKey = (string)reader["kernelDebugKey"];

            if (fieldList.Contains("availableUsersCSV") && !(reader["availableUsersCSV"] is DBNull))
                _availableUsersCSV = (string)reader["availableUsersCSV"];

            if (fieldList.Contains("friendlyName") && !(reader["friendlyName"] is DBNull))
                _friendlyName = (string)reader["friendlyName"];

            if (fieldList.Contains("state") && !(reader["state"] is DBNull))
            {
                long enumIdx = (long) reader["state"];
                _state = (bladeStatus) ((int) enumIdx);
            }

            if (fieldList.Contains("ownershipKey"))
                ownershipRowID = (long?) reader["ownershipKey"];

            if (fieldList.Contains("currentOwner") && !(reader["currentOwner"] is DBNull))
                _currentOwner = (string) reader["currentOwner"];

            if (fieldList.Contains("nextOwner") && !(reader["nextOwner"] is DBNull))
            {
                _nextOwner = (string) reader["nextOwner"];
            }

            if (fieldList.Contains("currentSnapshot"))
            {
                if (reader["currentSnapshot"] is DBNull)
                    _currentSnapshot = "clean";
                else
                    _currentSnapshot = (string) reader["currentSnapshot"];
            }

            if (fieldList.Contains("lastKeepAlive"))
            {
                if (reader["lastKeepAlive"] is DBNull)
                {
                    lastKeepAlive = DateTime.MinValue;
                }
                else
                {
                    lastKeepAlive = DateTime.Parse((string) reader["lastKeepAlive"]);
                }
            }
        }

        #region locking stuff
        protected virtual List<string> getPermittedFieldsInclInheritorsR()
        {
            return getPermittedFieldsR();
        }

        private List<string> getPermittedFieldsR()
        {
            return getPermittedFields(permittedAccessRead);
        }

        protected virtual List<string> getPermittedFieldsInclInheritorsW()
        {
            return getPermittedFieldsW();
        }

        private List<string> getPermittedFieldsW()
        {
            return getPermittedFields(permittedAccessWrite);
        }

        public abstract List<string> getPermittedFieldsIncludingInheritors(bladeLockType lockType);

        public List<string> getPermittedFields(bladeLockType lockType)
        {
            List<string> toRet = new List<string>();
            if (lockType.HasFlag(bladeLockType.lockIPAddresses))
            {
                toRet.Add("kernelDebugPort");
            }
            if (lockType.HasFlag(bladeLockType.lockVirtualHW))
            {
                toRet.Add("kernelDebugKey");
                toRet.Add("friendlyName");
                toRet.Add("availableUsersCSV");
            }
            if (lockType.HasFlag(bladeLockType.lockOwnership))
            {
                toRet.Add("state");
                toRet.Add("currentOwner");
                toRet.Add("nextOwner");
            }
            if (lockType.HasFlag(bladeLockType.lockSnapshot))
            {
                toRet.Add("currentSnapshot");
            }
            toRet.Add("lastKeepAlive");

            return toRet.Distinct().ToList();
        }

        protected void checkPermsR(string propertyName)
        {
            if (!getPermittedFieldsInclInheritorsR().Contains(propertyName))
                throw new bladeLockExeception("Lock violation: Read access to field "  + propertyName + " denied");
        }

        protected void checkPermsW(string propertyName)
        {
            if (!getPermittedFieldsInclInheritorsW().Contains(propertyName))
                throw new bladeLockExeception("Lock violation: Write access to field " + propertyName + " denied");
        }

        public void notifyOfUpgradedLocks(bladeLockType readToAdd, bladeLockType writeToAdd)
        {
            foreach (string lockName in bladeLockCollection.getLockNames())
            {
                bladeLockType lockType = (bladeLockType)Enum.Parse(typeof (bladeLockType), lockName);
                bool previouslyHeldRead = ((int)permittedAccessRead & (int)lockType) != 0;
                bool addingRead = ((int)readToAdd & (int)lockType) != 0;
                bool addingWrite = ((int)writeToAdd & (int)lockType) != 0;

                if (previouslyHeldRead)
                    addingRead = false;

                // If we are adding a read lock, we must get clean data from the DB. We don't need to do this for a write lock if
                // we already held the read lock, since no-one could've changed it.
                if (addingRead | (!previouslyHeldRead && addingWrite))
                {
                    updateFieldFromDB(getPermittedFieldsIncludingInheritors(lockType));
                }
            }
            // Writer locks imply reader locks
            permittedAccessRead |= readToAdd | writeToAdd;
            permittedAccessWrite |= writeToAdd;
        }

        public void notifyOfDowngradedLocks(bladeLockType readToAdd, bladeLockType writeToAdd)
        {
            permittedAccessRead = bladeLockCollection.clearField(permittedAccessRead, readToAdd);
            permittedAccessWrite = bladeLockCollection.clearField(permittedAccessWrite, writeToAdd);

            foreach (string lockName in bladeLockCollection.getLockNames())
            {
                bladeLockType lockType = (bladeLockType) Enum.Parse(typeof (bladeLockType), lockName);

                // We are downgrading if we previously held a writer lock, and now hold a reader lock.
                bool releasingWrite = ((int)writeToAdd & (int)lockType) != 0;
                bool releasingRead = ((int)readToAdd & (int)lockType) != 0;
                bool downgradingWriteToRead = releasingWrite && !releasingRead;

                if (releasingWrite)
                {
                    // Flush anything we are no longer holding a write lock on to the DB.
                    createOrUpdateInDB(getPermittedFieldsIncludingInheritors(lockType));
                }

                if (downgradingWriteToRead)
                {
                    // We should make sure we set the read lock if we're downgrading
                    permittedAccessRead |= lockType;
                }
            }
        }
#endregion

        public virtual void createOrUpdateInDB()
        {
            createOrUpdateOwnershipInDB(getPermittedFieldsW());
        }

        public abstract void updateFieldFromDB(List<string> toWrite);

        protected void updateOwnershipFieldFromDB(List<string> toWrite)
        {
            // Remove anything that this class can't write (ie, fields handled by the inheritor)
            List<string> handledFields = getPermittedFields(bladeLockType.lockAll);
            List<string> fieldsToWrite = toWrite.Where(x => handledFields.Exists(y => x == y)).ToList();

            // We will permit writes of no fields if we don't yet have a row ID.
            if (fieldsToWrite.Count == 0)
                return;

            if (!ownershipRowID.HasValue)
                throw new Exception("Cannot reload from DB because we have no row ID");

            string sqlCommand =   "select " + string.Join(",", fieldsToWrite) + " from bladeOwnership "
                                + " where ownershipKey = $ownershipKey; ";

            using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
            {
                cmd.Parameters.AddWithValue("$ownershipKey", ownershipRowID);
                using (SQLiteDataReader reader = cmd.ExecuteReader())
                {
                    if (!reader.Read())
                        throw new Exception("DB row not found");
                    parseFromDBRow(reader);
                    if (reader.Read())
                        throw new Exception("More than one DB row returned");
                }
            }
        }

        public abstract void createOrUpdateInDB(List<string> toWrite);
        
        protected void createOrUpdateOwnershipInDB(List<string> toWrite)
        {
            // Remove anything that this class can't write (ie, fields handled by the inheritor)
            List<string> handledFields = getPermittedFields(bladeLockType.lockAll);
            string[] fieldsToWrite = toWrite.Where(x => handledFields.Exists(y => x == y)).ToArray();

            // We will permit writes of no fields if we don't yet have a row ID.
            if (fieldsToWrite.Length == 0 && ownershipRowID.HasValue)
            {
                Debug.WriteLine("Nothing to write");
                return;
            }

            string sqlCommand;
            if (ownershipRowID.HasValue)
            {
                sqlCommand = "update bladeOwnership set ";
                sqlCommand += string.Join(",", fieldsToWrite.Select(x => x + "=$" + x));
                sqlCommand += " where ownershipKey = $ownershipKey; ";
            }
            else
            {
                sqlCommand = "insert into bladeOwnership ";
                if (fieldsToWrite.Length == 0)
                {
                    sqlCommand += " DEFAULT VALUES ";
                }
                else
                {
                    sqlCommand += "(" + string.Join(",", fieldsToWrite);
                    sqlCommand += ") values (";
                    sqlCommand += string.Join(",", fieldsToWrite.Select(x => "$" + x));
                    sqlCommand += ")";
                }
            } 
            
            Debug.WriteLine("SQL " + sqlCommand);

            using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
            {
                cmd.Parameters.AddWithValue("$state", _state);
                cmd.Parameters.AddWithValue("$currentOwner", _currentOwner);
                cmd.Parameters.AddWithValue("$nextOwner", _nextOwner);
                cmd.Parameters.AddWithValue("$lastKeepAlive", _lastKeepAlive);
                cmd.Parameters.AddWithValue("$currentSnapshot", _currentSnapshot);
                cmd.Parameters.AddWithValue("$kernelDebugPort", _kernelDebugPort);
                cmd.Parameters.AddWithValue("$kernelDebugKey", _kernelDebugKey);
                cmd.Parameters.AddWithValue("$friendlyName", _friendlyName);
                cmd.Parameters.AddWithValue("$availableUsersCSV", _availableUsersCSV);
                if (ownershipRowID.HasValue)
                    cmd.Parameters.AddWithValue("$ownershipKey", ownershipRowID);
                cmd.ExecuteNonQuery();
                if (!ownershipRowID.HasValue)
                {
                    ownershipRowID = (int?)conn.LastInsertRowId;
                    Debug.WriteLine("New row ID is " + ownershipRowID);
                }
                else
                {
                    Debug.WriteLine("row ID already generated");
                }
            }
        }

        public virtual void deleteInDB()
        {
            const string cmd_bladeConfig = "delete from bladeOwnership where ownershipKey = $ownershipKey";

            using (SQLiteCommand cmd = new SQLiteCommand(cmd_bladeConfig, conn))
            {
                cmd.Parameters.AddWithValue("$ownershipKey", ownershipRowID);
                cmd.ExecuteNonQuery();
            }
        }

        protected string generateIPXEScript(string script)
        {
            if (state == bladeStatus.unused)
            {
                return  "#!ipxe\r\n" +
                        "prompt Blade does not have any owner! Hit [enter] to reboot\r\n" +
                        "reboot\r\n";
            }
            
            // If we're owner by "vmserver", this means that we are currently preparing the iSCSI image for the client. Since we
            // are booting the client, it must get the snapshot which the client has requested.
            // This usually happens when we are using createDisks to prepare a VM.
            string ownershipToPresent = currentOwner;
            if (ownershipToPresent == "vmserver")
                ownershipToPresent = nextOwner;
            script = script.Replace("{BLADE_OWNER}", ownershipToPresent);

            script = script.Replace("{BLADE_NETMASK_ISCSI}", "255.255.0.0");
            script = script.Replace("{BLADE_SNAPSHOT}", currentSnapshot);

            return script;
        }
    }

    public class bladeLockExeception : Exception
    {
        public bladeLockExeception(string msg) : base(msg) { }
    }
}