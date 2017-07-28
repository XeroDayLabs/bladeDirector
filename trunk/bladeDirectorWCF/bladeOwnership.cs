using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SQLite;
using System.Diagnostics;
using System.Linq;
using System.Web.UI.WebControls;
using System.Xml.Serialization;
using createDisks;

namespace bladeDirectorWCF
{
    [XmlInclude(typeof (bladeSpec))]
    public abstract class bladeOwnership
    {
        [XmlIgnore]
        public SQLiteConnection conn;

        public bladeLockType permittedAccessRead;
        public bladeLockType permittedAccessWrite;

        public bladeStatus state { get { checkPermsR("state"); return _state; } set {  checkPermsW("state"); _state = value; } }
        private bladeStatus _state;

        public string currentOwner { get { checkPermsR("currentOwner"); return _currentOwner; } set { checkPermsW("currentOwner"); _currentOwner = value; } }
        private string _currentOwner;

        public string nextOwner
        {
            get { checkPermsR("nextOwner"); return _nextOwner; }
            set { checkPermsW("nextOwner"); _nextOwner = value; }
        }

        private string _nextOwner;

        public DateTime lastKeepAlive { get { checkPermsR("lastKeepAlive"); return _lastKeepAlive; } set { checkPermsW("lastKeepAlive"); _lastKeepAlive = value; } }
        private DateTime _lastKeepAlive;

        public string currentSnapshot { get { checkPermsR("currentSnapshot"); return _currentSnapshot; } set { checkPermsW("currentSnapshot"); _currentSnapshot = value; } }
        private string _currentSnapshot;

        public long? ownershipRowID;

        protected bladeOwnership()
        {
            // Used for XML de/ser
            permittedAccessRead = bladeLockType.lockAll;
            permittedAccessWrite = bladeLockType.lockAll;
        }

        protected bladeOwnership(SQLiteConnection conn, bladeLockType permittedAccessRead, bladeLockType permittedAccessWrite)
        {
            this.conn = conn;
            _state = bladeStatus.unused;
            this.permittedAccessRead = permittedAccessRead;
            this.permittedAccessWrite = permittedAccessWrite;

            if (conn != null)
            {
                // Do an empty insert, so we get a DB ID for this row
                createOrUpdateOwnershipInDB(new List<string>());
            }
        }

        protected bladeOwnership(SQLiteConnection conn, SQLiteDataReader reader, bladeLockType permittedAccessRead, bladeLockType permittedAccessWrite)
        {
            this.conn = conn;
            this.permittedAccessRead = permittedAccessRead ;
            this.permittedAccessWrite = permittedAccessWrite;

            parseFromDBRow(reader);
        }

        protected void parseFromDBRow(SQLiteDataReader reader)
        {
            string[] fieldList = new string[reader.FieldCount];
            for (int i = 0; i < reader.FieldCount; i++)
                fieldList[i] = reader.GetName(i);

            if (fieldList.Contains("state"))
            {
                if (reader["state"] is DBNull)
                {
                    _state = bladeStatus.unused;
                }
                else
                {
                    long enumIdx = (long) reader["state"];
                    _state = (bladeStatus) ((int) enumIdx);
                }
            }

            if (fieldList.Contains("ownershipKey"))
                ownershipRowID = (long?) reader["ownershipKey"];

            if (fieldList.Contains("currentOwner"))
            {
                if ((reader["currentOwner"] is DBNull))
                {
                    _currentOwner = null;
                }
                else
                {
                    _currentOwner = (string) reader["currentOwner"];
                }
            }

            if (fieldList.Contains("nextOwner"))
            {
                if (reader["nextOwner"] is DBNull)
                {
                    _nextOwner = null;
                }
                else
                {
                    _nextOwner = (string) reader["nextOwner"];
                }
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
            if ((lockType & bladeLockType.lockOwnership) != bladeLockType.lockNone)
            {
                toRet.Add("state");
                toRet.Add("currentOwner");
                toRet.Add("nextOwner");
            }
            if ((lockType & bladeLockType.lockSnapshot) != bladeLockType.lockNone)
            {
                toRet.Add("currentSnapshot");
            }
            toRet.Add("lastKeepAlive");

            return toRet.Distinct().ToList();
        }

        protected void checkPermsR(string propertyName)
        {
            if (!getPermittedFieldsInclInheritorsR().Contains(propertyName))
                throw new Exception("Lock violation: Read access to field "  + propertyName + " denied");
        }

        protected void checkPermsW(string propertyName)
        {
            if (!getPermittedFieldsInclInheritorsW().Contains(propertyName))
                throw new Exception("Lock violation: Write access to field " + propertyName + " denied");
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

        public virtual itemToAdd toItemToAdd(bool useNextOwner = false)
        {
            throw new NotImplementedException();
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

            script = script.Replace("{BLADE_NETMASK_ISCSI}", "255.255.192.0");
            script = script.Replace("{BLADE_SNAPSHOT}", currentSnapshot);

            return script;
        }
    }

}