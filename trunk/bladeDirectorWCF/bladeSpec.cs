using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Xml.Serialization;
using bladeDirectorWCF.Properties;
using createDisks;

namespace bladeDirectorWCF
{
    [XmlInclude(typeof(bladeOwnership))]
    public class bladeSpec : bladeOwnership
    {
        // ReSharper disable UnusedMember.Global

        // If you add fields, don't forget to add them to the Equals() override too.
        public string iscsiIP 
        {
            get { checkPermsR("iscsiIP"); return _iscsiIP; }
            set { checkPermsW("iscsiIP"); _iscsiIP = value;  } 
        }
        private string _iscsiIP;

        public string bladeIP
        {
            get { checkPermsR("bladeIP"); return _bladeIP; }
            set { checkPermsW("bladeIP"); _bladeIP = value; }
        }
        private string _bladeIP;

        public string iLOIP
        {
            get { checkPermsR("iLOIP"); return _iLOIP; }
            set { checkPermsW("iLOIP"); _iLOIP = value; }
        }
        private string _iLOIP;

        public ushort iLOPort
        {
            get { checkPermsR("iLOPort"); return _iLOPort; }
            set { checkPermsW("iLOPort"); _iLOPort = value; }
        }
        private ushort _iLOPort;

        public VMDeployStatus vmDeployState
        {
            get { checkPermsR("vmDeployState"); return _vmDeployState; }
            set { checkPermsW("vmDeployState"); _vmDeployState = value; }
        }
        private VMDeployStatus _vmDeployState;

        public bool currentlyHavingBIOSDeployed
        {
            get { checkPermsR("currentlyHavingBIOSDeployed"); return _currentlyHavingBIOSDeployed; }
            set { checkPermsW("currentlyHavingBIOSDeployed"); _currentlyHavingBIOSDeployed = value; }
        }
        private bool _currentlyHavingBIOSDeployed;

        public bool currentlyBeingAVMServer
        {
            get { checkPermsR("currentlyBeingAVMServer"); return _currentlyBeingAVMServer; }
            set { checkPermsW("currentlyBeingAVMServer"); _currentlyBeingAVMServer = value; }
        }
        private bool _currentlyBeingAVMServer;

        public string lastDeployedBIOS
        {
            get { checkPermsR("lastDeployedBIOS"); return _lastDeployedBIOS; }
            set { checkPermsW("lastDeployedBIOS"); _lastDeployedBIOS = value; }
        }
        private string _lastDeployedBIOS;
        // ReSharper restore UnusedMember.Global

        [XmlIgnore]
        public long? bladeID { get; set; }

        // ReSharper disable NotAccessedField.Global
        // ReSharper disable MemberCanBePrivate.Global
        /// <summary>
        /// Exposed via WCF to the service
        /// </summary>
        public string iLoUsername;
        
        /// <summary>
        /// Exposed via WCF to the service
        /// </summary>
        public string iLoPassword;

        /// <summary>
        /// Exposed via WCF to the service
        /// </summary>
        public string ESXiUsername;

        /// <summary>
        /// Exposed via WCF to the service
        /// </summary>
        public string ESXiPassword;
        // ReSharper restore NotAccessedField.Global
        // ReSharper restore MemberCanBePrivate.Global

        private VMCapacity _VMCapacity = new VMCapacity();

        public bladeSpec()
            : base()
        {
            // For XML de/ser
        }

        public bladeSpec(SQLiteConnection conn,
            string newBladeIP, string newISCSIIP,
            string newILOIP, ushort newILOPort)
            : base(conn, bladeLockType.lockAll, bladeLockType.lockAll)
        {
            _iscsiIP = newISCSIIP;
            _bladeIP = newBladeIP;
            _iLOPort = newILOPort;
            _iLOIP = newILOIP;

            _currentlyHavingBIOSDeployed = false;
            _lastDeployedBIOS = null;
            _vmDeployState = VMDeployStatus.notBeingDeployed;

            ESXiUsername = Settings.Default.esxiUsername;
            ESXiPassword = Settings.Default.esxiPassword;
            iLoUsername = Settings.Default.iloUsername;
            iLoPassword = Settings.Default.iloPassword;

            if (conn != null)
            {
                // Do an empty insert, so we get a DB ID for this row
                createOrUpdateInDB(new List<string>());
            }
        }

        public bladeSpec(SQLiteConnection conn,
            string newBladeIP, string newISCSIIP, 
            string newILOIP, ushort newILOPort, 
            bool newCurrentlyHavingBIOSDeployed, VMDeployStatus newvmDeployState, string newCurrentBIOS, bladeLockType permittedAccessRead, bladeLockType permittedAccessWrite)
            : base(conn, permittedAccessRead, permittedAccessWrite)
        {
            _iscsiIP = newISCSIIP;
            _bladeIP = newBladeIP;
            _iLOPort = newILOPort;
            _iLOIP = newILOIP;

            _currentlyHavingBIOSDeployed = newCurrentlyHavingBIOSDeployed;
            _lastDeployedBIOS = newCurrentBIOS;
            _vmDeployState = newvmDeployState;

            ESXiUsername = Settings.Default.esxiUsername;
            ESXiPassword = Settings.Default.esxiPassword;
            iLoUsername = Settings.Default.iloUsername;
            iLoPassword = Settings.Default.iloPassword;

            if (conn != null)
            {
                // Do an empty insert, so we get a DB ID for this row
                createOrUpdateInDB(new List<string>());
            }
        }

        public bladeSpec(SQLiteConnection conn, SQLiteDataReader reader, bladeLockType permittedAccessRead, bladeLockType permittedAccessWrite)
            : base(conn, reader, permittedAccessRead, permittedAccessWrite)
        {
            parseFromDBRow(reader);

            ESXiUsername = Settings.Default.esxiUsername;
            ESXiPassword = Settings.Default.esxiPassword;
            iLoUsername = Settings.Default.iloUsername;
            iLoPassword = Settings.Default.iloPassword;
        }

        private void parseFromDBRow(SQLiteDataReader reader)
        {
            string[] fieldList = new string[reader.FieldCount];
            for (int i = 0; i < reader.FieldCount; i++)
                fieldList[i] = reader.GetName(i);

            if (fieldList.Contains("iscsiIP"))
                _iscsiIP = (string)reader["iscsiIP"];

            if (fieldList.Contains("bladeIP"))
                _bladeIP = (string)reader["bladeIP"];

            if (fieldList.Contains("iLOPort"))
                _iLOPort = ushort.Parse(reader["iLOPort"].ToString());

            if (fieldList.Contains("iLOIP"))
                _iLOIP = (string)reader["iLOIP"];

            if (fieldList.Contains("vmDeployState"))
            {
                Debug.WriteLine(reader["vmDeployState"].GetType());
                if (reader["vmDeployState"] is DBNull)
                    _vmDeployState = VMDeployStatus.notBeingDeployed;
                else
                    _vmDeployState = (VMDeployStatus) ((long) reader["vmDeployState"]);
            }

            if (fieldList.Contains("bladeConfigKey"))
            {
                if (reader["bladeConfigKey"] is DBNull)
                    bladeID = null;
                else
                    bladeID = (long) reader["bladeConfigKey"];
            }

            if (fieldList.Contains("currentlyHavingBIOSDeployed"))
                _currentlyHavingBIOSDeployed = (long)reader["currentlyHavingBIOSDeployed"] != 0;

            if (fieldList.Contains("currentlyBeingAVMServer"))
                _currentlyBeingAVMServer = (long)reader["currentlyBeingAVMServer"] != 0;

            if (fieldList.Contains("lastDeployedBIOS"))
            {
                if (reader["lastDeployedBIOS"] is DBNull)
                    _lastDeployedBIOS = null;
                else
                    _lastDeployedBIOS = (string) reader["lastDeployedBIOS"];
            }
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
            if (currentlyHavingBIOSDeployed != compareTo.currentlyHavingBIOSDeployed)
                return false;
            if (currentlyBeingAVMServer != compareTo.currentlyBeingAVMServer)
                return false;
            if (vmDeployState != compareTo.vmDeployState)
                return false;
            if (bladeID != compareTo.bladeID)
                return false;

            return true;
        }

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
            return toRet.Distinct().ToList();
        }

        private new static List<string> getPermittedFields(bladeLockType lockType)
        {
            List<string> toRet = new List<string>();
            if ((lockType & bladeLockType.lockIPAddresses) != bladeLockType.lockNone)
            {
                toRet.Add("iscsiIP");
                toRet.Add("bladeIP");
                toRet.Add("iLOIP");
                toRet.Add("iLOPort");
            }
            if ((lockType & bladeLockType.lockvmDeployState) != bladeLockType.lockNone)
            {
                toRet.Add("vmDeployState");
            }
            if ((lockType & bladeLockType.lockBIOS) != bladeLockType.lockNone)
            {
                toRet.Add("currentlyHavingBIOSDeployed");
                toRet.Add("lastDeployedBIOS");
            }
            if ((lockType & bladeLockType.lockOwnership) != bladeLockType.lockNone)
            {
                toRet.Add("currentlyBeingAVMServer");
            }

            return toRet.Distinct().ToList();
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

            if (!bladeID.HasValue)
                throw new Exception("Cannot reload from DB because we have no row ID");

            string sqlCommand;
            sqlCommand = "select " + string.Join(",", toWrite) + " from bladeConfiguration "
                         + " where bladeConfigKey = $bladeID; ";

            using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
            {
                cmd.Parameters.AddWithValue("$bladeID", bladeID);
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
            fieldsToWrite = fieldsToWrite.Where(x => handledFields.Exists(y => x == y)).ToList();

            if (fieldsToWrite.Count == 0 && bladeID.HasValue)
                return;

            if (!ownershipRowID.HasValue)
                throw new ArgumentException("Base ownership not propogated to DB correctly");
            fieldsToWrite.Add("ownershipID");

            string sqlCommand;
            if (bladeID.HasValue)
            {
                sqlCommand = "update bladeConfiguration set " + string.Join(",", fieldsToWrite.Select(x => x + "=$" + x));
                sqlCommand += " where bladeConfigKey = $bladeConfigKey; ";

                fieldsToWrite.Add("bladeConfigKey");
            }
            else
            {
                sqlCommand = "insert into bladeConfiguration ";
                if (fieldsToWrite.Count == 0)
                {
                    sqlCommand += " DEFAULT VALUES";
                }
                else
                {
                    sqlCommand += "(" + string.Join(",", fieldsToWrite);
                    sqlCommand += ") values (";
                    sqlCommand += string.Join(",", fieldsToWrite.Select(x => "$" + x));
                    sqlCommand += ")";
                }
            }

            using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
            {
                cmd.Parameters.AddWithValue("iscsiIP", _iscsiIP);
                cmd.Parameters.AddWithValue("bladeIP", _bladeIP);
                cmd.Parameters.AddWithValue("iLOIP", _iLOIP);
                cmd.Parameters.AddWithValue("iLOPort", _iLOPort);
                cmd.Parameters.AddWithValue("currentlyHavingBIOSDeployed", _currentlyHavingBIOSDeployed ? 1 : 0);
                cmd.Parameters.AddWithValue("vmDeployState", _vmDeployState);
                cmd.Parameters.AddWithValue("currentlyBeingAVMServer", _currentlyBeingAVMServer ? 1 : 0);
                cmd.Parameters.AddWithValue("lastDeployedBIOS", _lastDeployedBIOS);

                cmd.Parameters.AddWithValue("ownershipID", ownershipRowID.Value);
                if (bladeID.HasValue)
                    cmd.Parameters.AddWithValue("bladeConfigKey", bladeID.Value);

                cmd.ExecuteNonQuery();
                if (!bladeID.HasValue)
                    bladeID = (int)conn.LastInsertRowId;
            }
        }

        public bool canAccommodate(hostDB db, VMHardwareSpec req)
        {
            if ((permittedAccessRead & bladeLockType.lockVMCreation) == bladeLockType.lockNone)
                throw  new Exception("lockVMCreation is needed when calling .canAccomodate");

            vmserverTotals totals = db.getVMServerTotalsByVMServerIP(bladeIP);

            if (totals.VMs + 1 > _VMCapacity.maxVMs)
                return false;
            if (totals.ram + req.memoryMB > _VMCapacity.maxVMMemoryMB)
                return false;
            if (totals.cpus + req.cpuCount > _VMCapacity.maxCPUCount)
                return false;

            return true;
        }

        public lockableVMSpec createChildVM(SQLiteConnection conn, hostDB db, VMHardwareSpec reqhw, VMSoftwareSpec reqsw, string newOwner)
        {
            if ((permittedAccessRead & bladeLockType.lockVMCreation) == bladeLockType.lockNone)
                throw new Exception("lockVMCreation is needed when calling .createChildVM");

            vmSpec newVM = new vmSpec(conn, bladeLockType.lockAll, bladeLockType.lockAll);
            newVM.parentBladeIP = bladeIP;
            newVM.currentOwner = "vmserver"; // We own the blade until we are done setting it up
            newVM.state = bladeStatus.inUseByDirector;
            newVM.nextOwner = newOwner;
            newVM.parentBladeID = bladeID.Value;
            newVM.memoryMB = reqhw.memoryMB;
            newVM.cpuCount = reqhw.cpuCount;
            vmserverTotals totals = db.getVMServerTotalsByVMServerIP(bladeIP);
            newVM.indexOnServer = totals.VMs + 1;

            byte[] VMServerIPBytes = IPAddress.Parse(bladeIP).GetAddressBytes();

            // VM IPs are in 172.17.(128+bladeIndex).vmIndex
            newVM.VMIP = "172.17." + (28 + VMServerIPBytes[3]) + "." + newVM.indexOnServer;
            newVM.iscsiIP = "192.168." + (28 + VMServerIPBytes[3]) + "." + newVM.indexOnServer;
            newVM.eth0MAC = "00:50:56:00:" + (VMServerIPBytes[3] - 100).ToString("D2") + ":" + newVM.indexOnServer.ToString("D2");
            newVM.eth1MAC = "00:50:56:01:" + (VMServerIPBytes[3] - 100).ToString("D2") + ":" + newVM.indexOnServer.ToString("D2");
            newVM.displayName = "VM_" + (VMServerIPBytes[3] - 100).ToString("D2") + "_" + newVM.indexOnServer.ToString("D2");

            if (reqsw.debuggerPort == 0)
                reqsw.debuggerPort = (ushort) (50000 + ((VMServerIPBytes[3] - 100)*100) + newVM.indexOnServer);
            newVM.kernelDebugPort = reqsw.debuggerPort;
            newVM.kernelDebugKey = reqsw.debuggerKey;
            // VMs always have this implicit snapshot.
            newVM.currentSnapshot = "vm";

            return new lockableVMSpec(db.conn, newVM);
        }

        public override itemToAdd toItemToAdd(bool useNextOwner)
        {
            itemToAdd toRet = new itemToAdd();

            if (useNextOwner)
                toRet.cloneName = bladeIP + "-" + nextOwner + "-" + currentSnapshot;
            else
                toRet.cloneName = bladeIP + "-" + currentOwner + "-" + currentSnapshot;
            toRet.serverIP = currentOwner;
            toRet.snapshotName = currentSnapshot;
            toRet.bladeIP = bladeIP;
            toRet.computerName = bladeIP;
            toRet.isVirtualMachine = false;

            return toRet;
        }

        public string generateIPXEScript()
        {
            // Physical machines can be doing a variety of things, such as read/writing BIOS config, booting ESXi, or booting
            // normally via iSCSI. Each has its own template config, definined in the project resources. Select the appropriate
            // one here.
            string script;

            if (currentlyHavingBIOSDeployed)
                script = Resources.ipxeTemplateForBIOS;
            else if (currentlyBeingAVMServer)
                script = Resources.ipxeTemplateForESXi;
            else
                script = Resources.ipxeTemplate;

            script = script.Replace("{BLADE_IP_ISCSI}", iscsiIP);
            script = script.Replace("{BLADE_IP_MAIN}", bladeIP);

            return base.generateIPXEScript(script);
        }
    }

    public class VMCapacity
    {
        public int maxVMs = 20;
        public int maxVMMemoryMB = 1024 * 20;
        public int maxCPUCount = 12;
    }

    public enum VMDeployStatus
    {
        notBeingDeployed = 0,
        needsPowerCycle = 1,
        waitingForPowerUp = 2,
        readyForDeployment = 3,
        failed = 4
    }
}