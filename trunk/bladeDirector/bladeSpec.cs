using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
using System.Xml.Serialization;
using createDisks;

namespace bladeDirector
{
    [XmlInclude(typeof(bladeOwnership))]
    public class bladeSpec : bladeOwnership
    {
        // If you add fields, don't forget to add them to the Equals() override too.
        public string iscsiIP;
        public string bladeIP;
        public string iLOIP;
        public ushort iLOPort;
        
        /// <summary>
        /// Exposed via WCF to the service
        /// </summary>
        // ReSharper disable once MemberCanBePrivate.Global
        public string iLoUsername;
        
        /// <summary>
        /// Exposed via WCF to the service
        /// </summary>
        // ReSharper disable once MemberCanBePrivate.Global
        public string iLoPassword;

        public bool currentlyHavingBIOSDeployed = false;
        public bool currentlyBeingAVMServer = false;
        public string lastDeployedBIOS = null;
        public long bladeID;

        /// <summary>
        /// Exposed via WCF to the service
        /// </summary>
        // ReSharper disable once MemberCanBePrivate.Global
        public string ESXiUsername;

        /// <summary>
        /// Exposed via WCF to the service
        /// </summary>
        // ReSharper disable once MemberCanBePrivate.Global
        public string ESXiPassword;

        private int maxVMs = 20;
        private int maxVMMemoryMB = 1024 * 20;
        private int maxCPUCount = 12;

        public bladeSpec()
        {
            // For XML serialisation
        }

        public bladeSpec(string newBladeIP, string newISCSIIP, string newILOIP, ushort newILOPort, bool newCurrentlyHavingBIOSDeployed, string newCurrentBIOS)
        {
            iscsiIP = newISCSIIP;
            bladeIP = newBladeIP;
            iLOPort = newILOPort;
            iLOIP = newILOIP;

            currentlyHavingBIOSDeployed = newCurrentlyHavingBIOSDeployed;
            lastDeployedBIOS = newCurrentBIOS;

            ESXiUsername = Properties.Settings.Default.esxiUsername;
            ESXiPassword = Properties.Settings.Default.esxiPassword;
            iLoUsername = Properties.Settings.Default.iloUsername;
            iLoPassword = Properties.Settings.Default.iloPassword;
        }

        public bladeSpec(SQLiteDataReader reader)
            : base(reader)
        {
            iscsiIP = (string)reader["iscsiIP"];
            bladeIP = (string)reader["bladeIP"];
            iLOPort = ushort.Parse(reader["iLOPort"].ToString());
            iLOIP = (string)reader["iLOIP"];

            bladeID = (long)reader["bladeConfigurationid"];

            currentlyHavingBIOSDeployed = (long)reader["currentlyHavingBIOSDeployed"] != 0;
            currentlyBeingAVMServer = (long)reader["currentlyBeingAVMServer"] != 0;

            if (reader["lastDeployedBIOS"] is System.DBNull)
                lastDeployedBIOS = null;
            else
                lastDeployedBIOS = (string)reader["lastDeployedBIOS"];

            ESXiUsername = Properties.Settings.Default.esxiUsername;
            ESXiPassword = Properties.Settings.Default.esxiPassword;
            iLoUsername = Properties.Settings.Default.iloUsername;
            iLoPassword = Properties.Settings.Default.iloPassword;
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

            return true;
        }

        public override resultCode updateInDB(SQLiteConnection conn)
        {
            base.updateInDB(conn);

            if (!base.ownershipRowID.HasValue)
                throw new ArgumentException("ownershipID");

            string cmdConfiguration = "update bladeConfiguration set " +
                " currentlyHavingBIOSDeployed=$currentlyHavingBIOSDeployed, " +
                " currentlyBeingAVMServer=$currentlyBeingAVMServer, " +
                " lastDeployedBIOS=$lastDeployedBIOS, " +
                " ownershipID=$ownershipID " +
                " where bladeIP = $bladeIP";
            using (SQLiteCommand cmd = new SQLiteCommand(cmdConfiguration, conn))
            {
                cmd.Parameters.AddWithValue("currentlyHavingBIOSDeployed", currentlyHavingBIOSDeployed ? 1 : 0);
                cmd.Parameters.AddWithValue("currentlyBeingAVMServer", currentlyBeingAVMServer ? 1 : 0);
                cmd.Parameters.AddWithValue("bladeIP", bladeIP);
                cmd.Parameters.AddWithValue("lastDeployedBIOS", lastDeployedBIOS);
                cmd.Parameters.AddWithValue("ownershipID", base.ownershipRowID.Value);
                cmd.ExecuteNonQuery();
            }
            return resultCode.success;
        }

        public override void createInDB(SQLiteConnection conn)
        {
            base.createInDB(conn);

            if (!base.ownershipRowID.HasValue)
                throw new ArgumentException("ownershipID");

            string cmd_bladeConfig = "insert into bladeConfiguration" +
                                     "(iscsiIP, bladeIP, iLoIP, iLOPort, currentlyHavingBIOSDeployed, currentlyBeingAVMServer, lastDeployedBIOS, ownershipID)" +
                                     " VALUES " +
                                     "($iscsiIP, $bladeIP, $iLoIP, $iLOPort, $currentlyHavingBIOSDeployed, $currentlyBeingAVMServer, $lastDeployedBIOS, $ownershipID)";
            using (SQLiteCommand cmd = new SQLiteCommand(cmd_bladeConfig, conn))
            {
                cmd.Parameters.AddWithValue("$iscsiIP", iscsiIP);
                cmd.Parameters.AddWithValue("$bladeIP", bladeIP);
                cmd.Parameters.AddWithValue("$iLoIP", iLOIP);
                cmd.Parameters.AddWithValue("$iLOPort", iLOPort);
                cmd.Parameters.AddWithValue("$currentlyHavingBIOSDeployed", currentlyHavingBIOSDeployed ? 1 : 0);
                cmd.Parameters.AddWithValue("$currentlyBeingAVMServer", currentlyBeingAVMServer ? 1 : 0);
                cmd.Parameters.AddWithValue("$lastDeployedBIOS", lastDeployedBIOS);
                cmd.Parameters.AddWithValue("$ownershipID", base.ownershipRowID.Value);
                cmd.ExecuteNonQuery();
                bladeID = (int)conn.LastInsertRowId;
            }            
        }

        public bool canAccommodate(SQLiteConnection conn, VMHardwareSpec req)
        {
            List<vmSpec> vms = getChildVMs(conn);

            if (vms.Count + 1>= this.maxVMs)
                return false;
            if (vms.Sum(x => x.hwSpec.memoryMB) + req.memoryMB >= this.maxVMMemoryMB)
                return false;
            if (vms.Sum(x => x.hwSpec.cpuCount) + req.cpuCount >= this.maxCPUCount)
                return false;

            return true;
        }

        private List<vmSpec> getChildVMs(SQLiteConnection conn)
        {
            string cmd_bladeConfig = "select *, bladeOwnership.id as bladeOwnershipID from VMConfiguration " + 
                " join bladeOwnership on bladeOwnership.id == vmconfiguration.ownershipid " + 
                " where parentBladeID = $VMServerID";
            List<vmSpec> vms = new List<vmSpec>();
            using (SQLiteCommand cmd = new SQLiteCommand(cmd_bladeConfig, conn))
            {
                cmd.Parameters.AddWithValue("$VMServerID", bladeID);
                using (SQLiteDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        vmSpec newVM = new vmSpec(reader);
                        vms.Add(newVM);
                    }
                }
            }
            return vms;
        }

        public void becomeAVMServer(SQLiteConnection conn)
        {
            // Delete any VM configurations that have been left lying around.

            string sql = "select VMConfiguration.id from VMConfiguration " +
                        " join BladeConfiguration on  BladeConfiguration.id = bladeOwnership.id " +
                        "join bladeOwnership on VMConfiguration.parentBladeID = bladeOwnership.id " +
                        " where BladeConfiguration.bladeIP = $bladeIP";
            List<long> toDel = new List<long>();
            using (SQLiteCommand cmd = new SQLiteCommand(sql, conn))
            {
                cmd.Parameters.AddWithValue("$bladeIP",  bladeIP);
                using (SQLiteDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        toDel.Add((long) reader[0]);
                    }
                }
            }

            string deleteSQL = "delete from VMConfiguration where id in (" + String.Join(",", toDel) + ")";
            using (SQLiteCommand cmd = new SQLiteCommand(deleteSQL, conn))
            {
                cmd.ExecuteNonQuery();
            }

            currentlyBeingAVMServer = true;
            state = bladeStatus.inUseByDirector;
        }

        public vmSpec createChildVM(SQLiteConnection conn, VMHardwareSpec reqhw, VMSoftwareSpec reqsw, string newOwner)
        {
            vmSpec newVM = new vmSpec();
            newVM.currentOwner = "vmserver";    // We own the blade until we are done setting it up
            newVM.state = bladeStatus.inUseByDirector;
            newVM.nextOwner = newOwner;
            newVM.parentBladeID = this.bladeID;
            newVM.hwSpec = reqhw;
            newVM.indexOnServer = getChildVMs(conn).Count + 1;

            byte[] VMServerIPBytes = IPAddress.Parse(this.bladeIP).GetAddressBytes();

            // VM IPs are in 172.17.(128+bladeIndex).vmIndex
            newVM.VMIP = "172.17." + (28 + VMServerIPBytes[3]) + "." + newVM.indexOnServer;
            newVM.iscsiIP = "192.168." + (28 + VMServerIPBytes[3]) + "." + newVM.indexOnServer;
            newVM.eth0MAC = "00:50:56:00:" + (VMServerIPBytes[3] - 100).ToString("D2") + ":" + newVM.indexOnServer.ToString("D2");
            newVM.eth1MAC = "00:50:56:01:" + (VMServerIPBytes[3] - 100).ToString("D2") + ":" + newVM.indexOnServer.ToString("D2");
            newVM.displayName = "VM_" + (VMServerIPBytes[3] - 100).ToString("D2") + "_" + newVM.indexOnServer.ToString("D2");

            if (reqsw.debuggerPort == 0)
                reqsw.debuggerPort = (ushort) (50000 + ((VMServerIPBytes[3] - 100) * 100) + newVM.indexOnServer);
            newVM.kernelDebugPort = reqsw.debuggerPort;
            newVM.kernelDebugKey = reqsw.debuggerKey;
            // VMs always have this implicit snapshot.
            newVM.currentSnapshot = "vm";

            return newVM;
        }

        public override itemToAdd toItemToAdd()
        {
            itemToAdd toRet = new itemToAdd();

            toRet.cloneName = this.bladeIP + "-" + this.currentOwner + "-" + this.currentSnapshot;
            toRet.serverIP = this.currentOwner;
            toRet.snapshotName = this.currentSnapshot;
            toRet.bladeIP = this.bladeIP;
            toRet.computerName = this.bladeIP;

            return toRet;
        }
    }
}