using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Linq;
using System.Xml.Serialization;
using bladeDirectorWCF.Properties;
using createDisks;

namespace bladeDirectorWCF
{
    [XmlInclude(typeof (bladeOwnership))]
    public class vmSpec : bladeOwnership
    {
        public long parentBladeID
        {
            get { checkPermsR("parentBladeID"); return _parentBladeID; }
            set { checkPermsW("parentBladeID"); _parentBladeID = value; }
        }
        private long _parentBladeID;

        public string parentBladeIP
        {
            get { checkPermsR("parentBladeIP"); return _parentBladeIP; }
            set { checkPermsW("parentBladeIP"); _parentBladeIP = value; }
        }
        private string _parentBladeIP;
        
        public string iscsiIP
        {
            get { checkPermsR("iscsiIP"); return _iscsiIP; }
            set { checkPermsW("iscsiIP"); _iscsiIP = value; }
        }
        private string _iscsiIP;

        public string VMIP
        {
            get { checkPermsR("VMIP"); return _VMIP; }
            set { checkPermsW("VMIP"); _VMIP = value; }
        }
        private string _VMIP;

        public long? vmConfigKey;
        
        public string eth0MAC
        {
            get { checkPermsR("eth0MAC"); return _eth0MAC; }
            set { checkPermsW("eth0MAC"); _eth0MAC = value; }
        }
        private string _eth0MAC;
        
        public string eth1MAC
        {
            get { checkPermsR("eth1MAC"); return _eth1MAC; }
            set { checkPermsW("eth1MAC"); _eth1MAC = value; }
        }
        private string _eth1MAC;
        
        public string displayName
        {
            get { checkPermsR("displayName"); return _displayName; }
            set { checkPermsW("displayName"); _displayName = value; }
        }
        private string _displayName;
        
        public int indexOnServer
        {
            get { checkPermsR("indexOnServer"); return _indexOnServer; }
            set { checkPermsW("indexOnServer"); _indexOnServer = value; }
        }
        private int _indexOnServer;

        public ushort kernelDebugPort
        {
            get { checkPermsR("kernelDebugPort"); return _kernelDebugPort; }
            set { checkPermsW("kernelDebugPort"); _kernelDebugPort = value; }
        }
        private ushort _kernelDebugPort;

        public string kernelDebugKey
        {
            get { checkPermsR("kernelDebugKey"); return _kernelDebugKey; }
            set { checkPermsW("kernelDebugKey"); _kernelDebugKey = value; }
        }
        private string _kernelDebugKey;

        // These two properties are accessed by the client via the XML response. They must be public and non-read-only.
        // ReSharper disable MemberCanBePrivate.Global
        // ReSharper disable NotAccessedField.Global
        public string username;
        public string password;
        // ReSharper restore NotAccessedField.Global
        // ReSharper restore MemberCanBePrivate.Global

        public int cpuCount
        {
            get { checkPermsR("cpuCount"); return _cpuCount; }
            set { checkPermsW("cpuCount"); _cpuCount = value; }
        }
        private int _cpuCount;

        public int memoryMB
        {
            get { checkPermsR("memoryMB"); return _memoryMB; }
            set { checkPermsW("memoryMB"); _memoryMB = value; }
        }
        private int _memoryMB;

        public bool isWaitingForResources
        {
            get { checkPermsR("isWaitingForResources"); return _isWaitingForResources; }
            set { checkPermsW("isWaitingForResources"); _isWaitingForResources = value; }
        }
        private bool _isWaitingForResources;
        

        public vmSpec()
            : base()
        {
            // Used for XML de/ser
        }

        public vmSpec(SQLiteConnection conn, bladeLockType readLock, bladeLockType writeLock)
            : base(conn, readLock, writeLock)
        {
            vmConfigKey = null;
        }

        public vmSpec(SQLiteConnection conn, string IP , bladeLockType readLock, bladeLockType writeLock, bool writeToDBImmediately = true)
            : base(conn, readLock, writeLock)
        {
            vmConfigKey = null;
            VMIP = IP;

            if (writeToDBImmediately)
                createOrUpdateInDB(new List<string>() { "vmConfigKey", "VMIP" });
        }

        public vmSpec(SQLiteConnection conn, SQLiteDataReader reader, bladeLockType readLock, bladeLockType writeLock)
            : base(conn, reader, readLock, writeLock)
        {
            parseFromDBRow(reader);

            username = Settings.Default.vmUsername;
            password = Settings.Default.vmPassword;
        }

        public void parseFromDBRow(SQLiteDataReader reader)
        {
            string[] fieldList = new string[reader.FieldCount];
            for (int i = 0; i < reader.FieldCount; i++)
                fieldList[i] = reader.GetName(i);

            if (fieldList.Contains("parentBladeID"))
                _parentBladeID = (long)reader["parentBladeID"];

            if (fieldList.Contains("vmConfigKey"))
                vmConfigKey = (long?)reader["vmConfigKey"];

            if (fieldList.Contains("parentBladeIP") && !(reader["parentBladeIP"] is DBNull))
                    _parentBladeIP = (string) reader["parentBladeIP"];
            if (fieldList.Contains("iscsiIP") && !(reader["iscsiIP"] is DBNull))
                _iscsiIP = (string)reader["iscsiIP"];
            if (fieldList.Contains("VMIP") && !(reader["VMIP"] is DBNull))
                _VMIP = (string)reader["VMIP"];
            if (fieldList.Contains("eth0MAC") && !(reader["eth0MAC"] is DBNull))
                _eth0MAC = (string)reader["eth0MAC"];
            if (fieldList.Contains("eth1MAC") && !(reader["eth1MAC"] is DBNull))
                _eth1MAC = (string)reader["eth1MAC"];
            if (fieldList.Contains("displayname") && !(reader["displayname"] is DBNull))
                _displayName = (string)reader["displayname"];
            if (fieldList.Contains("kernelDebugPort") && !(reader["kernelDebugPort"] is DBNull))
                _kernelDebugPort = Convert.ToUInt16(reader["kernelDebugPort"]);
            if (fieldList.Contains("kernelDebugKey") && !(reader["kernelDebugKey"] is DBNull))
                _kernelDebugKey = (string)reader["kernelDebugKey"];
            if (fieldList.Contains("indexOnServer") && !(reader["indexOnServer"] is DBNull))
                _indexOnServer = Convert.ToInt32(reader["indexOnServer"]);
            if (fieldList.Contains("memoryMB") && !(reader["memoryMB"] is DBNull))
                _memoryMB = Convert.ToInt32(reader["memoryMB"]);
            if (fieldList.Contains("cpuCount") && !(reader["cpuCount"] is DBNull))
                _cpuCount = Convert.ToInt32(reader["cpuCount"]);
            if (fieldList.Contains("isWaitingForResources"))
                _isWaitingForResources = (long)reader["isWaitingForResources"] != 0;
        }

#region locking stuff
        protected override List<string> getPermittedFieldsInclInheritorsR()
        {
            List<string> ourFields = getPermittedFields(permittedAccessRead);
            ourFields.AddRange(base.getPermittedFieldsInclInheritorsR());
            return ourFields;
        }

        private List<string> getPermittedFieldsR()
        {
            return getPermittedFields(permittedAccessRead);
        }

        protected override List<string> getPermittedFieldsInclInheritorsW()
        {
            List<string> ourFields = getPermittedFields(permittedAccessWrite);
            ourFields.AddRange(base.getPermittedFieldsInclInheritorsW());
            return ourFields;
        }

        private List<string> getPermittedFieldsW()
        {
            return getPermittedFields(permittedAccessWrite);
        }

        public override List<string> getPermittedFieldsIncludingInheritors(bladeLockType lockType)
        {
            List<string> toRet = getPermittedFields(lockType);
            toRet.AddRange(base.getPermittedFields(lockType));
            return toRet;
        }

        private new static List<string> getPermittedFields(bladeLockType lockType)
        {
            List<string> toRet = new List<string>();
            if ((lockType & bladeLockType.lockIPAddresses) != bladeLockType.lockNone)
            {
                toRet.Add("iscsiIP");
                toRet.Add("VMIP");
            }
            if ((lockType & bladeLockType.lockVirtualHW) != bladeLockType.lockNone)
            {
                toRet.Add("vmConfigKey");
                toRet.Add("eth0MAC");
                toRet.Add("eth1MAC");
                toRet.Add("displayName");
                toRet.Add("kernelDebugPort");
                toRet.Add("kernelDebugKey");
                toRet.Add("cpuCount");
                toRet.Add("memoryMB");
            }
            if ((lockType & bladeLockType.lockOwnership) != bladeLockType.lockNone)
            {
                toRet.Add("parentBladeID");
                toRet.Add("parentBladeIP");
                toRet.Add("indexOnServer");
                toRet.Add("isWaitingForResources");
            }

            return toRet.Distinct().ToList();
        }
#endregion

        public override void deleteInDB()
        {
            base.deleteInDB();

            const string cmd_VMConfig = "delete from VMConfiguration where vmConfigKey = $vmConfigKey";
            using (SQLiteCommand cmd = new SQLiteCommand(cmd_VMConfig, conn))
            {
                cmd.Parameters.AddWithValue("$vmConfigKey", vmConfigKey);
                cmd.ExecuteNonQuery();
            }
        }

        public override void createOrUpdateInDB()
        {
            createOrUpdateInDB(getPermittedFieldsInclInheritorsW());
        }

        public override void updateFieldFromDB(List<string> toWrite)
        {
            base.updateOwnershipFieldFromDB(toWrite);

            // Mask out the base classes fields
            List<string> handledFields = getPermittedFields(bladeLockType.lockAll);
            toWrite = toWrite.Where(x => handledFields.Exists(y => x == y)).ToList();

            if (toWrite.Count == 0)
                return;

            if (!vmConfigKey.HasValue)
                throw new Exception("Cannot reload from DB because we have no row ID");

            string sqlCommand;
            sqlCommand = "select " + string.Join(",", toWrite) + " from VMConfiguration "
                         + " where vmConfigKey = $vmConfigKey; ";

            using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
            {
                cmd.Parameters.AddWithValue("$vmConfigKey", vmConfigKey);
                using (SQLiteDataReader reader = cmd.ExecuteReader())
                {
                    if (!reader.Read())
                        throw new Exception("DB row not found");
                    parseFromDBRow(reader);
                    base.parseFromDBRow(reader);
                    if (reader.Read())
                        throw new Exception("More than one DB row returned");
                }
            }
        }

        public override void createOrUpdateInDB(List<string> fieldsToWrite)
        {
            base.createOrUpdateOwnershipInDB(fieldsToWrite);

            // Mask out the base classes fields
            List<string> handledFields = getPermittedFields(bladeLockType.lockAll);
            fieldsToWrite = fieldsToWrite.Where(x => handledFields.Contains(x)).ToList();

            fieldsToWrite.Add("vmConfigKey");
            fieldsToWrite.Add("ownershipID");

            string cmd_VMConfig;
            if (vmConfigKey == null)
            {
                // No row inserted yet, so insert.
                cmd_VMConfig = "insert into vmConfiguration (";
                cmd_VMConfig += string.Join(",", fieldsToWrite);
                cmd_VMConfig += ") values (";
                cmd_VMConfig += string.Join(",", fieldsToWrite.Select(x => "$" + x));
                cmd_VMConfig += ")";
            }
            else
            {
                // No row inserted yet, so insert.
                cmd_VMConfig = "update VMConfiguration set ";
                cmd_VMConfig += string.Join(",", fieldsToWrite.Select(x => x + "=$" + x));
                cmd_VMConfig += " where vmConfigKey = $vmConfigKey; ";
            }

            using (SQLiteCommand cmd = new SQLiteCommand(cmd_VMConfig, conn))
            {
                cmd.Parameters.AddWithValue("$ownershipID", ownershipRowID);
                cmd.Parameters.AddWithValue("$parentBladeID", _parentBladeID);
                cmd.Parameters.AddWithValue("$parentBladeIP", _parentBladeIP);
                cmd.Parameters.AddWithValue("$memoryMB", _memoryMB);
                cmd.Parameters.AddWithValue("$cpuCount", _cpuCount);
                cmd.Parameters.AddWithValue("$vmConfigKey", vmConfigKey);
                cmd.Parameters.AddWithValue("$vmIP", _VMIP);
                cmd.Parameters.AddWithValue("$iscsiIP", _iscsiIP);
                cmd.Parameters.AddWithValue("$eth0MAC", _eth0MAC);
                cmd.Parameters.AddWithValue("$eth1MAC", _eth1MAC);
                cmd.Parameters.AddWithValue("$displayName", _displayName);
                cmd.Parameters.AddWithValue("$kernelDebugPort", _kernelDebugPort);
                cmd.Parameters.AddWithValue("$kernelDebugKey", _kernelDebugKey);
                cmd.Parameters.AddWithValue("$indexOnServer", _indexOnServer);
                cmd.Parameters.AddWithValue("$isWaitingForResources", _isWaitingForResources);

                if (cmd.ExecuteNonQuery() != 1)
                {
                    throw new Exception("SQL statement did not return 1: '" + cmd_VMConfig + "'");
                }
                if (!vmConfigKey.HasValue)
                    vmConfigKey = (int)conn.LastInsertRowId;
            }
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

        public string generateIPXEScript()
        {
            string script = Resources.ipxeTemplate;

            script = script.Replace("{BLADE_IP_ISCSI}", iscsiIP);
            script = script.Replace("{BLADE_IP_MAIN}", VMIP);

            return base.generateIPXEScript(script);
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}