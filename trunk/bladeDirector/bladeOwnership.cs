using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Linq;
using System.Xml.Serialization;
using createDisks;

namespace bladeDirector
{
    [XmlInclude(typeof (bladeSpec))]
    public abstract class bladeOwnership
    {
        public readonly bladeLockType permittedAccess;

        public bladeStatus state { get { return _state; } set {  checkPerms("state"); _state = value; } }
        private bladeStatus _state;

        // FIXME: this shuld be a blade property, not an ownership property
        public VMDeployStatus VMDeployState { get { return _VMDeployState; } set { checkPerms("VMDeployState"); _VMDeployState = value; } }
        private VMDeployStatus _VMDeployState;

        public string currentOwner { get { return _currentOwner; } set { checkPerms("currentOwner"); _currentOwner = value; } }
        private string _currentOwner;

        public string nextOwner { get { return _nextOwner; } set { checkPerms("nextOwner"); _nextOwner = value; } }
        private string _nextOwner;

        public DateTime lastKeepAlive { get { return _lastKeepAlive; } set { checkPerms("lastKeepAlive"); _lastKeepAlive = value; } }
        private DateTime _lastKeepAlive;

        public string currentSnapshot { get { return _currentSnapshot; } set { checkPerms("currentSnapshot"); _currentSnapshot = value; } }
        private string _currentSnapshot;

        public long? ownershipRowID;

        protected bladeOwnership(VMDeployStatus newVMDeployState, bladeLockType permittedAccess)
        {
            _state = bladeStatus.unused;
            this.permittedAccess = permittedAccess;
            _VMDeployState = newVMDeployState;
        }

        protected bladeOwnership(IDataRecord reader, bladeLockType permittedAccess)
        {
            this.permittedAccess = permittedAccess;

            long enumIdx = (long)reader["state"];
            _state = (bladeStatus)((int)enumIdx);
            _VMDeployState = (VMDeployStatus)Convert.ToInt32(reader["VMDeployState"]);

            ownershipRowID = (long?)reader["ownershipKey"];

            if (!(reader["currentOwner"] is DBNull))
                _currentOwner = (string)reader["currentOwner"];
            if (!(reader["nextOwner"] is DBNull))
                _nextOwner = (string)reader["nextOwner"];

            if (reader["currentSnapshot"] is DBNull)
                _currentSnapshot = "clean";
            else
                _currentSnapshot = (string)reader["currentSnapshot"];

            lastKeepAlive = DateTime.Parse((string)reader["lastKeepAlive"]);
        }

        protected virtual List<string> getPermittedFieldsInclInheritors()
        {
            return getPermittedFields();
        }

        private List<string> getPermittedFields()
        {
            List<string> toRet = new List<string>();
            if ((permittedAccess & bladeLockType.lockOwnership) != bladeLockType.lockNone)
            {
                toRet.Add("state");
                toRet.Add("currentOwner");
                toRet.Add("nextOwner");
            }
            if ((permittedAccess & bladeLockType.lockVMDeployState) != bladeLockType.lockNone)
            {
                toRet.Add("VMDeployState");
            }
            if ((permittedAccess & bladeLockType.lockSnapshot) != bladeLockType.lockNone)
            {
                toRet.Add("currentSnapshot");
            }
            toRet.Add("lastKeepAlive");

            return toRet.Distinct().ToList();
        }

        protected void checkPerms(string propertyName)
        {
            if (!getPermittedFieldsInclInheritors().Contains(propertyName))
                throw new Exception("Lock violation: Access to field "  + propertyName + " denied");
        }

        public virtual void createInDB(SQLiteConnection conn)
        {
            string[] fieldsToWrite = getPermittedFields().ToArray();
            if (fieldsToWrite.Length== 0)
                return;

            string sqlCommand = "insert into bladeOwnership (";
            sqlCommand += string.Join(",", fieldsToWrite);
            sqlCommand += ") values (";
            sqlCommand += string.Join(",", fieldsToWrite.Select(x => "$" + x));
            sqlCommand += ")";

            using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
            {
                cmd.Parameters.AddWithValue("$state", _state);
                cmd.Parameters.AddWithValue("$currentOwner", _currentOwner);
                cmd.Parameters.AddWithValue("$nextOwner", _nextOwner);
                cmd.Parameters.AddWithValue("$lastKeepAlive", _lastKeepAlive);
                cmd.Parameters.AddWithValue("$currentSnapshot", _currentSnapshot);
                cmd.Parameters.AddWithValue("$VMDeployState", _VMDeployState);
                cmd.ExecuteNonQuery();
                ownershipRowID = (int?)conn.LastInsertRowId;
            }
        }

        public virtual void updateInDB(SQLiteConnection conn)
        {
            string[] fieldsToWrite = getPermittedFields().ToArray();
            if (fieldsToWrite.Length == 0)
                return;

            string sqlCommand;
            if (ownershipRowID.HasValue)
            {
                sqlCommand = "update bladeOwnership set ";
                sqlCommand += string.Join(",", fieldsToWrite.Select(x => x + "=$" + x));
                sqlCommand += " where ownershipKey = $ownershipKey; ";
            }
            else
            {
                sqlCommand = "insert into bladeOwnership (";
                sqlCommand += string.Join(",", fieldsToWrite);
                sqlCommand += ") values (";
                sqlCommand += string.Join(",", fieldsToWrite.Select(x => "$" + x));
                sqlCommand += ")";
            } 
            
            using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
            {
                cmd.Parameters.AddWithValue("$state", _state);
                cmd.Parameters.AddWithValue("$currentOwner", _currentOwner);
                cmd.Parameters.AddWithValue("$nextOwner", _nextOwner);
                cmd.Parameters.AddWithValue("$lastKeepAlive", _lastKeepAlive);
                cmd.Parameters.AddWithValue("$VMDeployState", _VMDeployState);
                cmd.Parameters.AddWithValue("$currentSnapshot", _currentSnapshot);
                if (ownershipRowID.HasValue)
                    cmd.Parameters.AddWithValue("$ownershipKey", ownershipRowID);
                cmd.ExecuteNonQuery();
            }
        }

        public virtual void deleteInDB(SQLiteConnection conn)
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
    }

    public enum VMDeployStatus
    {
        needsPowerCycle = 0,
        readyForDeployment = 1
    }
}