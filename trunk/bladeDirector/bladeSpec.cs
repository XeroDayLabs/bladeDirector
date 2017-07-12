using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Linq;
using System.Net;
using System.Xml.Serialization;
using bladeDirector.Properties;
using createDisks;

namespace bladeDirector
{
    [XmlInclude(typeof(bladeOwnership))]
    public class bladeSpec : bladeOwnership
    {
        // ReSharper disable UnusedMember.Global

        // If you add fields, don't forget to add them to the Equals() override too.
        public string iscsiIP 
        {
            get { return _iscsiIP; }
            set { checkPerms("iscsiIP"); _iscsiIP = value;  } 
        }
        private string _iscsiIP;

        public string bladeIP
        {
            get { return _bladeIP; }
            set { checkPerms("bladeIP"); _bladeIP = value; }
        }
        private string _bladeIP;

        public string iLOIP
        {
            get { return _iLOIP; }
            set { checkPerms("iLOIP"); _iLOIP = value; }
        }
        private string _iLOIP;

        public ushort iLOPort
        {
            get { return _iLOPort; }
            set { checkPerms("iLOPort"); _iLOPort = value; }
        }
        private ushort _iLOPort;

        public bool currentlyHavingBIOSDeployed
        {
            get { return _currentlyHavingBIOSDeployed; }
            set { checkPerms("currentlyHavingBIOSDeployed"); _currentlyHavingBIOSDeployed = value; }
        }
        private bool _currentlyHavingBIOSDeployed;

        public bool currentlyBeingAVMServer
        {
            get { return _currentlyBeingAVMServer; }
            set { checkPerms("currentlyBeingAVMServer"); _currentlyBeingAVMServer = value; }
        }
        private bool _currentlyBeingAVMServer;

        public string lastDeployedBIOS
        {
            get { return _lastDeployedBIOS; }
            set { checkPerms("lastDeployedBIOS"); _lastDeployedBIOS = value; }
        }
        private string _lastDeployedBIOS;
        // ReSharper restore UnusedMember.Global

        public long bladeID { get; private set; }

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

        public bladeSpec(
            string newBladeIP, string newISCSIIP, 
            string newILOIP, ushort newILOPort, 
            bool newCurrentlyHavingBIOSDeployed, VMDeployStatus newVMDeployState, string newCurrentBIOS, bladeLockType permittedAccess)
            : base(newVMDeployState, permittedAccess)
        {
            _iscsiIP = newISCSIIP;
            _bladeIP = newBladeIP;
            _iLOPort = newILOPort;
            _iLOIP = newILOIP;

            _currentlyHavingBIOSDeployed = newCurrentlyHavingBIOSDeployed;
            _lastDeployedBIOS = newCurrentBIOS;

            ESXiUsername = Settings.Default.esxiUsername;
            ESXiPassword = Settings.Default.esxiPassword;
            iLoUsername = Settings.Default.iloUsername;
            iLoPassword = Settings.Default.iloPassword;
        }

        public bladeSpec(SQLiteDataReader reader, bladeLockType permittedAccess)
            : base(reader, permittedAccess)
        {
            _iscsiIP = (string)reader["iscsiIP"];
            _bladeIP = (string)reader["bladeIP"];
            _iLOPort = ushort.Parse(reader["iLOPort"].ToString());
            _iLOIP = (string)reader["iLOIP"];

            bladeID = (long)reader["bladeConfigKey"];

            _currentlyHavingBIOSDeployed = (long)reader["currentlyHavingBIOSDeployed"] != 0;
            _currentlyBeingAVMServer = (long)reader["currentlyBeingAVMServer"] != 0;

            if (reader["lastDeployedBIOS"] is DBNull)
                _lastDeployedBIOS = null;
            else
                _lastDeployedBIOS = (string)reader["lastDeployedBIOS"];

            ESXiUsername = Settings.Default.esxiUsername;
            ESXiPassword = Settings.Default.esxiPassword;
            iLoUsername = Settings.Default.iloUsername;
            iLoPassword = Settings.Default.iloPassword;
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
            if (bladeID != compareTo.bladeID)
                return false;
            if (VMDeployState != compareTo.VMDeployState)
                return false;

            return true;
        }

        protected override List<string> getPermittedFieldsInclInheritors()
        {
            List<string> ourFields = getPermittedFields();
            ourFields.AddRange(base.getPermittedFieldsInclInheritors());
            return ourFields;
        }

        private List<string> getPermittedFields()
        {
            List<string> toRet = new List<string>();
            if ((permittedAccess & bladeLockType.lockAll) != bladeLockType.lockNone)
            {
                toRet.Add("iscsiIP");
                toRet.Add("bladeIP");
                toRet.Add("iLOIP");
                toRet.Add("iLOPort");
            }
            if ((permittedAccess & bladeLockType.lockBIOS) != bladeLockType.lockNone)
            {
                toRet.Add("currentlyHavingBIOSDeployed");
                toRet.Add("lastDeployedBIOS");
            }
            if ((permittedAccess & bladeLockType.lockOwnership) != bladeLockType.lockNone)
            {
                toRet.Add("currentlyBeingAVMServer");
            }

            return toRet.Distinct().ToList();
        }

        public override void updateInDB(SQLiteConnection conn)
        {
            base.updateInDB(conn);

            if (!ownershipRowID.HasValue)
                throw new ArgumentException("ownershipID");

            List<string> fieldsToWrite = getPermittedFields();
            if (fieldsToWrite.Count == 0)
                return;

            fieldsToWrite.Add("ownershipID");

            string sqlCommand = "update bladeConfiguration set ";
            sqlCommand += string.Join(",", fieldsToWrite.Select(x => x + "=$" + x));
            sqlCommand += " where bladeIP = $bladeIP; ";

            using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
            {

                cmd.Parameters.AddWithValue("iscsiIP", _iscsiIP);
                cmd.Parameters.AddWithValue("bladeIP", _bladeIP);
                cmd.Parameters.AddWithValue("iLOIP", _iLOIP);
                cmd.Parameters.AddWithValue("iLOPort", _iLOPort);
                cmd.Parameters.AddWithValue("currentlyHavingBIOSDeployed", _currentlyHavingBIOSDeployed ? 1 : 0);
                cmd.Parameters.AddWithValue("currentlyBeingAVMServer", _currentlyBeingAVMServer ? 1 : 0);
                cmd.Parameters.AddWithValue("lastDeployedBIOS", _lastDeployedBIOS);
                cmd.Parameters.AddWithValue("ownershipID", ownershipRowID.Value);
                cmd.ExecuteNonQuery();
            }
            return;
        }

        public override void createInDB(SQLiteConnection conn)
        {
            base.createInDB(conn);

            if (!ownershipRowID.HasValue)
                throw new ArgumentException("ownershipID");

            List<string> fieldsToWrite = getPermittedFields();
            if (fieldsToWrite.Count == 0)
                return;

            fieldsToWrite.Add("ownershipID");

            string sqlCommand = "insert into bladeConfiguration (";
            sqlCommand += string.Join(",", fieldsToWrite);
            sqlCommand += ") values (";
            sqlCommand += string.Join(",", fieldsToWrite.Select(x => "$" + x));
            sqlCommand += ")";

            using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
            {
                cmd.Parameters.AddWithValue("$iscsiIP", iscsiIP);
                cmd.Parameters.AddWithValue("$bladeIP", bladeIP);
                cmd.Parameters.AddWithValue("$iLoIP", iLOIP);
                cmd.Parameters.AddWithValue("$iLOPort", iLOPort);
                cmd.Parameters.AddWithValue("$currentlyHavingBIOSDeployed", currentlyHavingBIOSDeployed ? 1 : 0);
                cmd.Parameters.AddWithValue("$currentlyBeingAVMServer", currentlyBeingAVMServer ? 1 : 0);
                cmd.Parameters.AddWithValue("$lastDeployedBIOS", lastDeployedBIOS);
                cmd.Parameters.AddWithValue("$ownershipID", ownershipRowID.Value);
                cmd.ExecuteNonQuery();
                bladeID = (int)conn.LastInsertRowId;
            }            
        }

        public bool canAccommodate(hostDB db, VMHardwareSpec req)
        {
            if ((permittedAccess & bladeLockType.lockVMCreation) == bladeLockType.lockNone)
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

        public lockableVMSpec createChildVM(hostDB db, VMHardwareSpec reqhw, VMSoftwareSpec reqsw, string newOwner)
        {
            if ((permittedAccess & bladeLockType.lockVMCreation) == bladeLockType.lockNone)
                throw new Exception("lockVMCreation is needed when calling .createChildVM");

            vmSpec newVM = new vmSpec(VMDeployStatus.needsPowerCycle);
            newVM.parentBladeIP = bladeIP;
            newVM.currentOwner = "vmserver"; // We own the blade until we are done setting it up
            newVM.state = bladeStatus.inUseByDirector;
            newVM.nextOwner = newOwner;
            newVM.parentBladeID = bladeID;
            newVM.hwSpec = reqhw;
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
    }

    public class VMCapacity
    {
        public int maxVMs = 20;
        public int maxVMMemoryMB = 1024 * 20;
        public int maxCPUCount = 12;
    }
}