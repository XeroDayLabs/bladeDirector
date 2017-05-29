using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.ServiceModel;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Services.Description;
using System.Web.UI.WebControls;
using bladeDirector.bootMenuWCF;
using createDisks;
using hypervisors;
using Renci.SshNet;

namespace bladeDirector
{
    public static class hostStateDB
    {
        private static List<string> logEvents = new List<string>();
        public static TimeSpan keepAliveTimeout = TimeSpan.FromMinutes(1);

        private static Object connLock = new object();
        private static SQLiteConnection conn = null;
        public static string dbFilename;

        private static ConcurrentDictionary<string, biosThreadState> biosUpdateState = new ConcurrentDictionary<string, biosThreadState>();
        private static Dictionary<string, VMThreadState> VMDeployState = new Dictionary<string, VMThreadState>();

        private static Dictionary<string, resultCode> currentSnapshotSelections = new Dictionary<string, resultCode>();

        public static void init(string basePath)
        {
            lock (connLock)
            {
                dbFilename = Path.Combine(basePath, "hoststate.sqlite");

                // If we're making a new file, remember that, since we'll have to create a new schema.
                bool needToCreateSchema = !File.Exists(dbFilename);

                conn = new SQLiteConnection("Data Source=" + dbFilename);
                conn.Open();

                if (needToCreateSchema)
                    createTables();
            }
        }

        public static void resetAll()
        {
            lock (connLock)
            {
                dropDB();
                createTables();
            }
        }

        private static void dropDB()
        {
            lock (connLock)
            {
                if (conn != null)
                {
                    conn.Close();
                    conn.Dispose();
                }

                if (dbFilename != ":memory:")
                {
                    DateTime deadline = DateTime.Now + TimeSpan.FromMinutes(1);
                    while (true)
                    {
                        try
                        {
                            File.Delete(dbFilename);
                            break;
                        }
                        catch (UnauthorizedAccessException)
                        {
                            if (deadline < DateTime.Now)
                                throw;
                            Thread.Sleep(TimeSpan.FromSeconds(2));
                        }
                    }
                }

                conn = new SQLiteConnection("Data Source=" + dbFilename);
                conn.Open();
            }
        }

        private static void createTables()
        {
            lock (connLock)
            {
                string[] sqlCommands = Properties.Resources.DBCreation.Split(';');

                foreach (string sqlCommand in sqlCommands)
                {
                    using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }

        public static void setKeepAliveTimeout(TimeSpan newTimeout)
        {
            keepAliveTimeout = newTimeout;
        }

        public static void addLogEvent(string newEntry)
        {
            lock (logEvents)
            {
                logEvents.Add(DateTime.Now + " : " + newEntry);
            }
        }

        public static string[] getAllBladeIP()
        {
            lock (connLock)
            {
                string sqlCommand = "select bladeIP from bladeConfiguration";
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    using (SQLiteDataReader reader = cmd.ExecuteReader())
                    {
                        List<string> toRet = new List<string>();
                        while (reader.Read())
                            toRet.Add((string) reader[0]);
                        return toRet.ToArray();
                    }
                }
            }
        }

        public static List<bladeSpec> getAllBladeInfo()
        {
            lock (connLock)
            {
                string sqlCommand = "select *, bladeOwnership.id as bladeOwnershipID, bladeConfiguration.id as bladeConfigurationID from bladeOwnership " +
                                    "join bladeConfiguration on bladeOwnership.id = bladeConfiguration.ownershipID ";
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    using (SQLiteDataReader reader = cmd.ExecuteReader())
                    {
                        List<bladeSpec> toRet = new List<bladeSpec>();

                        while (reader.Read())
                            toRet.Add(new bladeSpec(reader));

                        return toRet.ToList();
                    }
                }
            }
        }

        public static bladeOwnership getBladeOrVMOwnershipByIP(string IP)
        {
            lock (connLock)
            {
                bladeOwnership toRet = getBladeByIP(IP);
                if (toRet != null)
                    return toRet;

                toRet = getVMByIP(IP);
                if (toRet != null)
                    return toRet;

                return null;
            }
        }

        public static bladeSpec getBladeByIP(string IP)
        {
            lock (connLock)
            {
                string sqlCommand = "select *, bladeOwnership.id as bladeOwnershipID, bladeConfiguration.id as bladeConfigurationID from bladeOwnership " +
                                    "join bladeConfiguration on bladeOwnership.id = bladeConfiguration.ownershipID " +
                                    "where bladeConfiguration.bladeIP = $bladeIP";
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    cmd.Parameters.AddWithValue("$bladeIP", IP);
                    using (SQLiteDataReader reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            return new bladeSpec(reader);
                        }
                        else
                        {
                            // No records returned.
                            return null;
                        }
                    }
                }
            }
        }

        public static vmSpec getVMByIP(string bladeName)
        {
            lock (connLock)
            {
                string sqlCommand = "select *, bladeOwnership.id as bladeOwnershipID, VMConfiguration.id as vmConfigurationID from bladeOwnership " +
                                    "join VMConfiguration on bladeOwnership.id = VMConfiguration.ownershipID " +
                                    "where VMConfiguration.VMIP = $VMIP";
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    cmd.Parameters.AddWithValue("$VMIP", bladeName);
                    using (SQLiteDataReader reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            return new vmSpec(reader);
                        }
                        else
                        {
                            // No records returned.
                            return null;
                        }
                    }
                }
            }
        }

        public static vmSpec[] getVMByVMServerIP(string vmServerIP)
        {
            List<vmSpec> toRet = new List<vmSpec>();

            lock (connLock)
            {
                string sqlCommand = "select *, VMConfiguration.id as vmConfigurationID, " +
                                    "bladeownership.id as bladeOwnershipID, " +
                                    "bladeConfiguration.id as bladeConfigurationID from bladeConfiguration " +
                                    "join vmConfiguration on vmConfiguration.parentbladeID = bladeConfiguration.ID " +
                                    "join bladeownership on bladeownership.id = vmConfiguration.ownershipID " +
                                    "where bladeConfiguration.bladeIP = $vmServerIP";
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    cmd.Parameters.AddWithValue("$vmServerIP", vmServerIP);
                    using (SQLiteDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                            toRet.Add(new vmSpec(reader));
                    }
                }
            }

            return toRet.ToArray();
        }

        public static resultCode tryRequestNode(string bladeIP, string requestorID)
        {
            lock (connLock)
            {
                bladeSpec reqBlade = getBladeByIP(bladeIP);
                if (reqBlade == null)
                {
                    addLogEvent("Blade " + requestorID + " requested blade " + bladeIP + "(not found)");
                    return resultCode.bladeNotFound;
                }

                reqBlade = checkKeepAlives(reqBlade);

                // If the blade is currently unused, we can just take it.
                if (reqBlade.state == bladeStatus.unused)
                {
                    reqBlade.currentOwner = requestorID;
                    reqBlade.state = bladeStatus.inUse;
                    reqBlade.lastKeepAlive = DateTime.Now;
                    reqBlade.updateInDB(conn);

                    addLogEvent("Blade " + requestorID + " requested blade " + bladeIP + "(success, blade was idle)");
                    notifyBootDirectorOfNode(reqBlade);
                    return resultCode.success;
                }

                // Otherwise, we need to request that the blade is released, and return 'pending'. 
                // Note that we don't permit a requestor to both own the blade, and be in the queue - this is because the
                // requestor would be unable to determine when its blade is allocated. We just return queuefull in that
                // situation.
                if (reqBlade.currentOwner == requestorID)
                {
                    addLogEvent("Blade " + requestorID + " requested blade " + bladeIP + "(failure, blade is already owned by this blade)");
                    return resultCode.bladeQueueFull;
                }

                // If the blade is already queued as requested, just report OK and leave it there,
                if (reqBlade.nextOwner == requestorID)
                {
                    addLogEvent("Blade " + requestorID + " requested blade " + bladeIP + "(success, requestor was already in queue)");
                    notifyBootDirectorOfNode(reqBlade);
                    return resultCode.success;
                }

                // See if the blade queue is actually full
                if (reqBlade.nextOwner != null)
                {
                    addLogEvent("Blade " + requestorID + " requested blade " + bladeIP + "(failure, blade queue is full)");
                    return resultCode.bladeQueueFull;
                }

                // It's all okay, so request the release.
                reqBlade.state = bladeStatus.releaseRequested;
                reqBlade.nextOwner = requestorID;
                reqBlade.updateInDB(conn);

                addLogEvent("Blade " + requestorID + " requested blade " + bladeIP + "(success, requestor added to blade queue)");
                return resultCode.pending;
            }
        }

        private static void checkKeepAlives(string reqBladeIP)
        {
            lock (connLock)
            {
                string sqlCommand = "select *, bladeOwnership.id as bladeOwnershipID, bladeConfiguration.id as bladeConfigurationID from bladeOwnership " +
                                    "join bladeConfiguration on bladeOwnership.id = bladeConfiguration.ownershipID ";
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    cmd.Parameters.AddWithValue("$bladeIP", reqBladeIP);
                    using (SQLiteDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            checkKeepAlives(new bladeSpec(reader));
                        }
                    }
                }
            }
        }

        private static void checkKeepAlives(vmSpec reqVM)
        {
            lock (connLock)
            {
                // VMs are slightly different to blades, since they are always implicitly owned and destroyed on release.
                if (reqVM.lastKeepAlive + keepAliveTimeout < DateTime.Now)
                {
                    if (reqVM.state != bladeStatus.inUseByDirector) // This will be true during deployment.
                    {
                        // Oh no, the blade owner failed to send a keepalive in time! Release it.
                        addLogEvent("Requestor " + reqVM.currentOwner + " failed to keepalive for VM " + reqVM.displayName + " (" + reqVM.VMIP + ") releasing VM");
                        releaseBladeOrVM(reqVM.VMIP, reqVM.currentOwner);
                    }
                }
            }
        }

        private static bladeSpec checkKeepAlives(bladeSpec reqBlade)
        {
            lock (connLock)
            {
                if (reqBlade.state == bladeStatus.unused)
                {
                    return reqBlade;
                }
                else if (reqBlade.state == bladeStatus.inUseByDirector)
                {
                    if (reqBlade.currentlyHavingBIOSDeployed)
                    {
                        // This situation can never timeout. Maybe it's a good idea if it does, but it can't for now.
                        return reqBlade;
                    }
                    else if (reqBlade.currentlyBeingAVMServer)
                    {
                        // If all VMs attached to this VM server are unused then we can destroy it.
                        vmSpec[] childVMs = getVMByVMServerIP(reqBlade.bladeIP);
                        foreach (vmSpec childVM in childVMs)
                            checkKeepAlives(childVM);
                        if (getVMByVMServerIP(reqBlade.bladeIP).Length == 0)
                        {
                            addLogEvent("VM server blade " + reqBlade.bladeIP + " has no running VMs, releasing");
                            releaseBladeOrVM(reqBlade.bladeIP, reqBlade.currentOwner);

                            return getBladeByIP(reqBlade.bladeIP);
                        }
                    }
                    else
                    {
                        // Eh, what is this doing then? :/
                        addLogEvent("Blade " + reqBlade.bladeIP + " is the inUseByDirector state, but isn't actually doing anything");
                        return getBladeByIP(reqBlade.bladeIP);
                    }
                }
                else
                {
                    if (reqBlade.lastKeepAlive + keepAliveTimeout < DateTime.Now)
                    {
                        // Oh no, the blade owner failed to send a keepalive in time! Release it.
                        addLogEvent("Requestor " + reqBlade.currentOwner + " failed to keepalive for " + reqBlade.bladeIP + ", releasing blade");
                        releaseBladeOrVM(reqBlade.bladeIP, reqBlade.currentOwner);

                        return getBladeByIP(reqBlade.bladeIP);
                    }
                }
            }
            return reqBlade;
        }

        public static string[] getBladesByAllocatedServer(string NodeIP)
        {
            lock (connLock)
            {
                checkKeepAlives(NodeIP);

                string sqlCommand = "select bladeIP from bladeOwnership " +
                                    "join bladeConfiguration on bladeOwnership.id = bladeConfiguration.ownershipID " +
                                    "where bladeOwnership.currentOwner = $bladeOwner";
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    cmd.Parameters.AddWithValue("$bladeOwner", NodeIP);
                    using (SQLiteDataReader reader = cmd.ExecuteReader())
                    {
                        List<string> toRet = new List<string>(10);
                        while (reader.Read())
                            toRet.Add((string)reader["bladeIP"]);
                        return toRet.ToArray();
                    }
                }
            }
        }

        public static void initWithBlades(string[] bladeIPs)
        {
            bladeSpec[] specs = new bladeSpec[bladeIPs.Length];
            int n = 0;
            foreach (string bladeIP in bladeIPs)
                specs[n++] = new bladeSpec(bladeIP, n.ToString(), n.ToString(), (ushort)n, false, null);

            initWithBlades(specs);
        }

        public static void initWithBlades(bladeSpec[] bladeSpecs)
        {
            lock (connLock)
            {
                dropDB();
                createTables();

                foreach (bladeSpec spec in bladeSpecs)
                    addNode(spec);
            }
        }

        public static void addNode(bladeSpec spec)
        {
            spec.createInDB(conn);
        }

        public static GetBladeStatusResult getBladeStatus(string nodeIp, string requestorIp)
        {
            lock (connLock)
            {
                checkKeepAlives(requestorIp);

                string sqlCommand = "select *, bladeOwnership.id as bladeOwnershipID, bladeConfiguration.id as bladeConfigurationID from bladeOwnership " +
                                    "join bladeConfiguration on bladeOwnership.id = bladeConfiguration.ownershipID " +
                                    "where bladeConfiguration.bladeIP = $bladeIP";
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    cmd.Parameters.AddWithValue("$bladeIP", nodeIp);
                    using (SQLiteDataReader reader = cmd.ExecuteReader())
                    {
                        if (!reader.Read())
                            return GetBladeStatusResult.bladeNotFound;

                        bladeSpec reqBlade = new bladeSpec(reader);

                        switch (reqBlade.state)
                        {
                            case bladeStatus.unused:
                                return GetBladeStatusResult.unused;
                            case bladeStatus.releaseRequested:
                                return GetBladeStatusResult.releasePending;
                            case bladeStatus.inUse:
                                if (reqBlade.currentOwner == requestorIp)
                                    return GetBladeStatusResult.yours;
                                else
                                    return GetBladeStatusResult.notYours;
                            case bladeStatus.inUseByDirector:
                                return GetBladeStatusResult.notYours;
                            default:
                                throw new ArgumentOutOfRangeException();
                        }
                    }
                }
            }
        }

        public static resultCode releaseBladeOrVM(string NodeIP, string RequestorIP, bool force = false)
        {
            lock (connLock)
            {
                string sqlCommand = "select *, bladeOwnership.id as bladeOwnershipID, bladeConfiguration.id as bladeConfigurationID from bladeOwnership " +
                                    "join bladeConfiguration on bladeOwnership.id = bladeConfiguration.ownershipID " +
                                    "where bladeConfiguration.bladeIP = $bladeIP";
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    cmd.Parameters.AddWithValue("$bladeIP", NodeIP);
                    using (SQLiteDataReader reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            bladeSpec reqBladeSpec = new bladeSpec(reader);
                            reqBladeSpec.currentlyBeingAVMServer = reqBladeSpec.currentlyHavingBIOSDeployed = false;
                            reqBladeSpec.updateInDB(conn);
                            releaseBlade(reqBladeSpec, RequestorIP, force);
                            return resultCode.success;
                        }
                    }
                }

                // Maybe its a VM
                sqlCommand = "select *, bladeOwnership.id as bladeOwnershipID, vmConfiguration.id as vmConfigurationID from bladeOwnership " +
                                "join vmConfiguration on bladeOwnership.id = vmConfiguration.ownershipID " +
                                "where vmConfiguration.VMIP = $VMIP";
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    cmd.Parameters.AddWithValue("$VMIP", NodeIP);
                    using (SQLiteDataReader reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            releaseVM(new vmSpec(reader));
                            return resultCode.success;
                        }
                    }
                }

                // Neither a blade nor a VM
                addLogEvent("Requestor " + RequestorIP + " attempted to release blade " + NodeIP + " (blade not found)");
                return resultCode.bladeNotFound;
            }
        }

        private static void releaseBlade(bladeSpec reqBlade, string RequestorIP, bool force)
        {
            if (!force)
            {
                if (reqBlade.currentOwner != RequestorIP)
                {
                    addLogEvent("Requestor " + RequestorIP + " attempted to release blade " + reqBlade.bladeIP + " (failure: blade is not owned by requestor)");
                    return;
                }
            }
            /*
            // Turn off the blade. We do this even if the blade will be used by someone else after this release.
            hypSpec_iLo hypSpec = new hypSpec_iLo(reqBlade.bladeIP, Properties.Settings.Default.esxiUsername, Properties.Settings.Default.ltspPassword,
                reqBlade.iLOIP, Properties.Settings.Default.iloUsername, Properties.Settings.Default.iloPassword, null, null, null, reqBlade.currentSnapshot, 0, null);
            using (hypervisor_iLo hyp = new hypervisor_iLo(hypSpec, clientExecutionMethod.smb))
            {
                try
                {
                    hyp.powerOff();
                }
                catch (Exception)
                {
                    // Nevermind.
                }
            }*/

            // Reset any VM-server the blade may be
            reqBlade.currentlyBeingAVMServer = reqBlade.currentlyHavingBIOSDeployed = false;
            foreach (vmSpec child in getVMByVMServerIP(reqBlade.bladeIP))
            {
                lock (VMDeployState)
                {
                    if (child.VMIP != null &&
                        VMDeployState.ContainsKey(child.VMIP.ToString()) &&
                        VMDeployState[child.VMIP].currentProgress.code != resultCode.pending  )
                    {
                        // Ahh, this VM is currently being deployed. We can't release it until the thread doing the deployment
                        VMDeployState[child.VMIP].deployDeadline = DateTime.MinValue;
                        while (VMDeployState[child.VMIP].currentProgress.code == resultCode.pending)
                        {
                            logEvents.Add("Waiting for VM deploy on " + child.VMIP + " to cancel");
                            Thread.Sleep(TimeSpan.FromSeconds(10));
                        }
                    }

                    lock (connLock)
                    {
                        child.deleteInDB(conn);
                    }
                }
            }

            // If there's someone waiting, allocate it to that blade.
            if (reqBlade.state == bladeStatus.releaseRequested)
            {
                addLogEvent("Requestor " + RequestorIP + " attempted to release blade " + reqBlade.bladeIP + " (success, blade is now owned by queue entry " + reqBlade.nextOwner + ")");

                reqBlade.state = bladeStatus.inUse;
                reqBlade.currentOwner = reqBlade.nextOwner;
                reqBlade.nextOwner = null;
                reqBlade.lastKeepAlive = DateTime.Now;
                reqBlade.updateInDB(conn);
            }
            else
            {
                // There's no-one waiting, so set it to idle.
                //if (reqBlade.state == bladeStatus.inUse || force)
                reqBlade.state = bladeStatus.unused;
                reqBlade.currentOwner = null;
                reqBlade.nextOwner = null;
                reqBlade.updateInDB(conn);
                addLogEvent("Requestor " + RequestorIP + " attempted to release blade " + reqBlade.bladeIP + " (success, blade is now idle)");
            }
        }

        private static void releaseVM(vmSpec toDel)
        {
            bladeSpec parentBladeSpec = getConfigurationOfBladeByID((int)toDel.parentBladeID);
            bool vmServerIsEmpty = getVMByVMServerIP(parentBladeSpec.bladeIP).Length == 1;

            // VMs always get destroyed on release.
            toDel.deleteInDB(conn);

            hypSpec_vmware spec = new hypSpec_vmware(toDel.displayName, parentBladeSpec.bladeIP, Properties.Settings.Default.esxiUsername, Properties.Settings.Default.esxiPassword,
               Properties.Settings.Default.vmUsername, Properties.Settings.Default.vmPassword,
                0, "", toDel.VMIP);

            try
            {
                using (hypervisor hyp = new hypervisor_vmware(spec, clientExecutionMethod.smb))
                {
                    hyp.powerOff();
                }
            }
            catch (SocketException) { }
            catch (WebException) { }

            // If the VM server is now empty, we can release it.
            if (vmServerIsEmpty)
                releaseBladeOrVM(parentBladeSpec.bladeIP, "vmserver");
        }

        public static resultCode forceBladeAllocation(string nodeIp, string newOwner)
        {
            lock (connLock)
            {
                bladeOwnership toAlloc = getBladeByIP(nodeIp);
                if (toAlloc == null)
                    toAlloc = getVMByIP(nodeIp);

                toAlloc.state = bladeStatus.inUse;
                toAlloc.currentOwner = newOwner;
                toAlloc.nextOwner = null;
                toAlloc.updateInDB(conn);

                return resultCode.success;
            }
        }

        public static bladeSpec getConfigurationOfBlade(string nodeIp)
        {
            lock (connLock)
            {
                string sqlCommand = "select *, bladeOwnership.id as bladeOwnershipID, bladeConfiguration.id as bladeConfigurationID from bladeOwnership " +
                                    "join bladeConfiguration on bladeOwnership.id = bladeConfiguration.ownershipID " +
                                    "where bladeConfiguration.bladeIP = $bladeIP";
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    cmd.Parameters.AddWithValue("$bladeIP", nodeIp);
                    SQLiteDataReader reader = cmd.ExecuteReader();

                    if (!reader.Read())
                    {
                        addLogEvent("getConfigurationOfBlade failed " + nodeIp + " (blade not found)");
                        return null;
                    }

                    return new bladeSpec(reader);
                }
            }
        }

        public static bladeSpec getConfigurationOfBladeByID(int nodeID)
        {
            lock (connLock)
            {
                string sqlCommand = "select *, bladeOwnership.id as bladeOwnershipID, bladeConfiguration.id as bladeConfigurationID from bladeOwnership " +
                                    "join bladeConfiguration on bladeOwnership.id = bladeConfiguration.ownershipID " +
                                    "where bladeConfiguration.id = $bladeID";
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    cmd.Parameters.AddWithValue("$bladeID", nodeID);
                    SQLiteDataReader reader = cmd.ExecuteReader();

                    if (!reader.Read())
                    {
                        addLogEvent("getConfigurationOfBlade failed ID " + nodeID + " (blade not found)");
                        return null;
                    }

                    return new bladeSpec(reader);
                }
            }
        }

        public static string getCurrentSnapshotForBladeOrVM(string nodeIp)
        {
            lock (connLock)
            {
                bladeSpec reqBlade = getBladeByIP(nodeIp);
                if (reqBlade != null)
                    return String.Format("{0}-{1}", reqBlade.bladeIP, reqBlade.currentSnapshot);

                vmSpec reqVM = getVMByIP(nodeIp);
                if (reqVM != null)
                    return String.Format("{0}-{1}", reqVM.VMIP, reqVM.currentSnapshot);
            }

            return null;
        }

        public static resultCodeAndBladeName RequestAnySingleVM(string requestorIP, VMHardwareSpec hwSpec, VMSoftwareSpec swReq )
        {
            if (hwSpec.memoryMB%4 != 0)
            {
                // Fun fact: ESXi VM memory size must be a multiple of 4mb.
                logEvents.Add("Failed VM alloc: memory size " + hwSpec.memoryMB + " is not a multiple of 4MB");
                return new resultCodeAndBladeName() {code = resultCode.genericFail};
            }

            lock (connLock)
            {
                lock (VMDeployState)
                {
                    getAllBladeInfo().ForEach(x => checkKeepAlives(x.bladeIP));

                    // First, we need to find a blade to use as a VM server. Do we have a free VM server? If so, just use that.
                    List<bladeSpec> allBladeInfo = getAllBladeInfo();
                    bladeSpec freeVMServer = allBladeInfo.SingleOrDefault((x) => x.currentlyBeingAVMServer && x.canAccommodate(conn, hwSpec));
                    if (freeVMServer == null)
                    {
                        // Nope, no free VM server. Maybe we can make a new one.
                        resultCodeAndBladeName resp = RequestAnySingleNode("vmserver");
                        if (resp.code != resultCode.success)
                            return resp;

                        freeVMServer = getConfigurationOfBlade(resp.bladeName);
                        freeVMServer.becomeAVMServer(conn);
                        freeVMServer.updateInDB(conn);
                    }

                    // Create rows for the child VM in the DB. Delete anything that was there previously.
                    vmSpec childVM = freeVMServer.createChildVMInDB(conn, hwSpec, swReq, requestorIP);
                    childVM.currentOwner = requestorIP;
                    childVM.state = bladeStatus.inUseByDirector;
                    childVM.updateInDB(conn);

                    string waitToken = childVM.VMIP.ToString();

                    // Is a deploy of this VM already in progress?
                    if (VMDeployState.ContainsKey(waitToken))
                    {
                        if (VMDeployState[waitToken].currentProgress.code != resultCode.pending)
                        {
                            // Yes, a deploy was in progress - but now it is finished.
                            VMDeployState.Remove(waitToken);
                        }
                    }

                    // Now start a new thread, which will ensure the VM server is powered up and then add the child VMs.
                    Thread worker = new Thread(VMServerBootThread);
                    worker.Name = "VMAllocationThread for VM " + childVM.VMIP + " on server " + freeVMServer.bladeIP;
                    VMThreadState deployState = new VMThreadState();
                    deployState.VMServer = freeVMServer;
                    deployState.childVM = childVM;
                    deployState.swSpec = swReq;
                    deployState.deployDeadline = DateTime.Now + TimeSpan.FromMinutes(25);
                    deployState.currentProgress = new resultCodeAndBladeName() { code = resultCode.pending };
                    VMDeployState.Add(waitToken, deployState);

                    worker.Start(waitToken);
                    return new resultCodeAndBladeName() { code = resultCode.pending, waitToken = waitToken };
                }
            }
        }

        private static void VMServerBootThread(object param)
        {
            string operationHandle = (string)param;
            VMThreadState threadState;
            lock (VMDeployState)
            {
                threadState = VMDeployState[operationHandle];
            }

            try
            {
                _VMServerBootThread(threadState);
            }
            catch (Exception e)
            {
                threadState.currentProgress.code = resultCode.genericFail;
                logEvents.Add("VMServer boot thread fatal exception: " + e.Message + " at "  + e.StackTrace);
            }
        }

        private static object VMServerBootThreadLock = new object();
        private static Dictionary<long, bladeState> physicalBladeLocks = new Dictionary<long, bladeState>();
        private static void _VMServerBootThread(VMThreadState threadState)
        {
            Debug.WriteLine(DateTime.Now + threadState.childVM.VMIP  + ": enter thread");
            // First, bring up the physical machine. It'll get the ESXi ISCSI config and boot up.
            hypSpec_iLo iloSpec = new hypSpec_iLo(threadState.VMServer.bladeIP, Properties.Settings.Default.esxiUsername, Properties.Settings.Default.esxiPassword, 
                threadState.VMServer.iLOIP, Properties.Settings.Default.iloUsername, Properties.Settings.Default.iloPassword, Properties.Settings.Default.iloUsername, 
                null, null, null, 0, null);
            using (hypervisor_iLo hyp = new hypervisor_iLo(iloSpec, clientExecutionMethod.SSHToBASH))
            {
                Debug.WriteLine(DateTime.Now + threadState.childVM.VMIP + ": connecting ilo");
                if (threadState.deployDeadline < DateTime.Now)
                    throw new TimeoutException();


                // Ensure only one thread tries to power on each VM server at once. Do this by having a collection of objects we lock
                // on, one for each physical server. Protect them with a global lock.
                Debug.WriteLine(DateTime.Now + threadState.childVM.VMIP + ": checking lock");

                lock (VMServerBootThreadLock)
                {
                    if (!physicalBladeLocks.ContainsKey(threadState.VMServer.bladeID))
                    {
                        Debug.WriteLine(DateTime.Now + threadState.childVM.VMIP + ": added lock");
                        physicalBladeLocks.Add(threadState.VMServer.bladeID, new bladeState() { isPoweredUp = false });
                    }
                }
                Debug.WriteLine(DateTime.Now + threadState.childVM.VMIP + ": checking power");
                if (!physicalBladeLocks[threadState.VMServer.bladeID].isPoweredUp)
                {
                    lock (physicalBladeLocks[threadState.VMServer.bladeID])
                    {
                        if (!physicalBladeLocks[threadState.VMServer.bladeID].isPoweredUp)
                        {
                            hyp.connect();
                            Debug.WriteLine(DateTime.Now + threadState.childVM.VMIP + ": connected ilo");
                            hyp.powerOff(threadState.deployDeadline);

                            Debug.WriteLine(DateTime.Now + threadState.childVM.VMIP + ": powering on");
                            if (getLastDeployedBIOSForBlade(threadState.VMServer.bladeIP) != Properties.Resources.VMServerBIOS)
                            {
                                Debug.WriteLine(DateTime.Now + threadState.childVM.VMIP + ": deploying BIOS to server " + threadState.VMServer.bladeIP);
                                rebootAndStartDeployingBIOSToBlade(threadState.VMServer.bladeIP, "vmserver", Properties.Resources.VMServerBIOS);

                                resultCode progress = resultCode.pending;
                                while (progress == resultCode.pending)
                                {
                                    progress = checkBIOSWriteProgress(threadState.VMServer.bladeIP);
                                    if (progress != resultCode.pending &&
                                        progress != resultCode.unknown &&
                                        progress != resultCode.success)
                                    {
                                        Debug.WriteLine(DateTime.Now + threadState.VMServer.bladeIP + ": BIOS deploy failed");
                                        throw new Exception("BIOS deploy failed, returning " + progress);
                                    }
                                }
                            }
                            hyp.powerOn(threadState.deployDeadline);

                            // Once it's powered up, we ensure the datastore is mounted okay. Sometimes I'm seeing ESXi hosts boot
                            // with an inaccessible NFS datastore, so remount if neccessary. Retry this since it doesn't seem to 
                            // always work first time.
                            string[] nfsMounts = hypervisor_iLo.doWithRetryOnSomeExceptions(() => hyp.startExecutable("esxcfg-nas", "-l")).stdout.Split(new[] { "\n", "\r" }, StringSplitOptions.RemoveEmptyEntries);
                            string foundMount = nfsMounts.SingleOrDefault(x => x.Contains("esxivms is /mnt/SSDs/esxivms from store.xd.lan mounted available"));
                            while (foundMount == null)
                            {
                                hypervisor_iLo.doWithRetryOnSomeExceptions(() => hyp.startExecutable("esxcfg-nas", "-d esxivms"));
                                hypervisor_iLo.doWithRetryOnSomeExceptions(() => hyp.startExecutable("esxcfg-nas", "-a --host store.xd.lan --share /mnt/SSDs/esxivms esxivms"));
                                hypervisor_iLo.doWithRetryOnSomeExceptions(() => hyp.startExecutable("esxcfg-rescan", "--all"));

                                nfsMounts = hypervisor_iLo.doWithRetryOnSomeExceptions(() => hyp.startExecutable("esxcfg-nas", "-l")).stdout.Split(new[] { "\n", "\r" }, StringSplitOptions.RemoveEmptyEntries);
                                foundMount = nfsMounts.SingleOrDefault(x => x.Contains("esxivms is /mnt/SSDs/esxivms from store.xd.lan mounted available"));
                            }

                            physicalBladeLocks[threadState.VMServer.bladeID].isPoweredUp = true;
                        }
                    }
                }
                while (!physicalBladeLocks[threadState.VMServer.bladeID].isPoweredUp)
                {
                    Debug.WriteLine(DateTime.Now + threadState.childVM.VMIP + ": Waiting for other thread to power on");
                    Thread.Sleep(TimeSpan.FromSeconds(5));
                }

                Debug.WriteLine(DateTime.Now + threadState.childVM.VMIP + ": powered on");

                // now SSH to the blade and actually create the VM.
                string destDir = "/vmfs/volumes/esxivms/" + threadState.VMServer.bladeID + "_" + threadState.childVM.vmSpecID;
                string destDirDatastoreType = "[esxivms] " + threadState.VMServer.bladeID + "_" + threadState.childVM.vmSpecID;
                string vmxPath = destDir + "/PXETemplate.vmx";

                if (threadState.deployDeadline < DateTime.Now)
                    throw new TimeoutException();

                // Remove the VM if it's already there. We don't mind if these commands fail - which they will, if the VM doesn't
                // exist.
                hypervisor_iLo.doWithRetryOnSomeExceptions(() => hyp.startExecutable("vim-cmd", "vmsvc/power.off `vim-cmd vmsvc/getallvms | grep \"" + destDirDatastoreType.Replace("[", "\\[").Replace("]", "\\]") + "\"`"));
                hypervisor_iLo.doWithRetryOnSomeExceptions(() => hyp.startExecutable("vim-cmd", "vmsvc/unregister `vim-cmd vmsvc/getallvms | grep \"" + destDirDatastoreType + "\"`"));

                // copy the template VM into a new directory
                doCmdAndCheckSuccess(hyp, "rm", " -rf " + destDir);
                doCmdAndCheckSuccess(hyp, "cp", " -R /vmfs/volumes/esxivms/PXETemplate " + destDir);
                // and then customise it.
                doCmdAndCheckSuccess(hyp, "sed", " -e 's/ethernet0.address[= ].*/ethernet0.address = \"" + threadState.childVM.eth0MAC + "\"/g' -i " + vmxPath);
                doCmdAndCheckSuccess(hyp, "sed", " -e 's/ethernet1.address[= ].*/ethernet1.address = \"" + threadState.childVM.eth1MAC + "\"/g' -i " + vmxPath);
                doCmdAndCheckSuccess(hyp, "sed", " -e 's/displayName[= ].*/displayName = \"" + threadState.childVM.displayName + "\"/g' -i " + vmxPath);
                doCmdAndCheckSuccess(hyp, "sed", " -e 's/memSize[= ].*/memSize = \"" + threadState.childVM.hwSpec.memoryMB + "\"/g' -i " + vmxPath);
                doCmdAndCheckSuccess(hyp, "sed", " -e 's/sched.mem.min[= ].*/sched.mem.min = \"" + threadState.childVM.hwSpec.memoryMB + "\"/g' -i " + vmxPath);
                doCmdAndCheckSuccess(hyp, "sed", " -e 's/sched.mem.minSize[= ].*/sched.mem.minSize = \"" + threadState.childVM.hwSpec.memoryMB + "\"/g' -i " + vmxPath);
                doCmdAndCheckSuccess(hyp, "sed", " -e 's/numvcpus[= ].*/numvcpus = \"" + threadState.childVM.hwSpec.cpuCount + "\"/g' -i " + vmxPath);
                doCmdAndCheckSuccess(hyp, "sed", " -e 's/uuid.bios[= ].*//g' -i " + vmxPath);
                doCmdAndCheckSuccess(hyp, "sed", " -e 's/uuid.location[= ].*//g' -i " + vmxPath);
                // doCmdAndCheckSuccess(hyp, "sed", " -e 's/serial0.fileName[= ].*/" + "serial0.fileName = \"telnet://:" + (1000 + threadState.childVM.vmSpecID) + "\"/g' -i " + vmxPath));

                // Now add that VM to ESXi, and the VM is ready to use.
                doCmdAndCheckSuccess(hyp, "vim-cmd", " solo/registervm " + vmxPath);

                Debug.WriteLine(DateTime.Now + threadState.childVM.VMIP + ": created");

                if (threadState.deployDeadline < DateTime.Now)
                    throw new TimeoutException();
            }

            // If the VM already has disks set up, delete them.
            string tagName = (threadState.swSpec.debuggerHost ?? "nodebug") + "-vm";

            itemToAdd itm = new itemToAdd();
            itm.bladeIP = threadState.childVM.VMIP;
            itm.cloneName = threadState.childVM.VMIP + "-" + tagName;
            itm.computerName = threadState.childVM.displayName;
            itm.snapshotName = threadState.childVM.VMIP + "-" + tagName;
            itm.kernelDebugPort = threadState.swSpec.debuggerPort;
            itm.serverIP = threadState.swSpec.debuggerHost;
            itm.kernelDebugKey = threadState.swSpec.debuggerKey;

            if (threadState.deployDeadline < DateTime.Now)
                throw new TimeoutException();

            Debug.WriteLine(DateTime.Now + threadState.childVM.VMIP + ": deleting");
            Program.deleteBlades(new[] { itm });

            if (threadState.deployDeadline < DateTime.Now)
                throw new TimeoutException();

            Debug.WriteLine(DateTime.Now + threadState.childVM.VMIP + ": adding");
            // Now create the disks, and customise the VM  by naming it appropriately.
            Program.addBlades(new[] {itm}, tagName, "localhost/bladeDirector", "bladebasestable-esxi", null, null, 
                (a,b) => new hypervisor_vmware(new hypSpec_vmware(a.computerName,
                threadState.VMServer.bladeIP, Properties.Settings.Default.esxiUsername, Properties.Settings.Default.esxiPassword,
                Properties.Settings.Default.vmUsername, Properties.Settings.Default.vmPassword,
                threadState.swSpec.debuggerPort, threadState.swSpec.debuggerKey, threadState.childVM.VMIP), clientExecutionMethod.smb), threadState.deployDeadline );

            Debug.WriteLine(DateTime.Now + threadState.childVM.VMIP + ": add complete");

            // TODO: Ability to deploy transportDriver

            if (threadState.deployDeadline < DateTime.Now)
                throw new TimeoutException();

            // Now we can select the new snapshot
            Debug.WriteLine(DateTime.Now + threadState.childVM.VMIP + ": selecting");
            resultCode snapshotRes = selectSnapshotForBladeOrVM(threadState.childVM.VMIP, tagName);
            while (snapshotRes == resultCode.pending)
            {
                snapshotRes = selectSnapshotForBladeOrVM_getProgress(threadState.childVM.VMIP);
                Thread.Sleep(TimeSpan.FromSeconds(4));
            }

            if (snapshotRes != resultCode.success)
            {
                Debug.WriteLine(DateTime.Now + threadState.childVM.VMIP + ": Failed to select snapshot,  " + snapshotRes);
                logEvents.Add(DateTime.Now + threadState.childVM.VMIP + ": Failed to select snapshot,  " + snapshotRes);
            }

            // All done. Re-read the VM state before we set state in case it's been changed while we ran (eg by selectSnapshotForBladeOrVM)
            lock (connLock)
            {
                bladeOwnership childVM = getBladeOrVMOwnershipByIP(threadState.childVM.VMIP);
                childVM.state = bladeStatus.inUse;
                childVM.updateInDB(conn);
            }
            threadState.currentProgress.bladeName = threadState.childVM.VMIP;
            threadState.currentProgress.code = resultCode.success;
        }

        private static void doCmdAndCheckSuccess(hypervisor_iLo hyp, string cmd, string args)
        {
            executionResult res = hypervisor_iLo.doWithRetryOnSomeExceptions<executionResult>(() => hyp.startExecutable(cmd, args));
            if (res.resultCode != 0)
            {
                logEvents.Add(string.Format("Command '{0}' with args '{1}' returned failure code {2}; stdout is '{3} and stderr is '{4}'", cmd, args, res.resultCode, res.stdout, res.stderr));
                throw new Exception("failed to execute ssh command");
            }
        }

        public static resultCodeAndBladeName RequestAnySingleVM_getProgress(string waitToken)
        {
            if (waitToken == null)
                return new resultCodeAndBladeName() { code = resultCode.bladeNotFound };

            if (!Monitor.TryEnter(VMDeployState, TimeSpan.FromSeconds(15)))
            {
                return new resultCodeAndBladeName() { code = resultCode.unknown };
            }
            else
            {
                try
                {
                    lock (VMDeployState)
                    {
                        if (!VMDeployState.ContainsKey(waitToken))
                            return new resultCodeAndBladeName() { code = resultCode.unknown };

                        return VMDeployState[waitToken].currentProgress;
                    }
                }
                finally 
                {
                    Monitor.Exit(VMDeployState);
                }
            }
        }
        
        public static resultCodeAndBladeName RequestAnySingleNode(string requestorIP)
        {
            lock (connLock)
            {
                List<bladeSpec> bladeStates = new List<bladeSpec>();

                // Make a list of blades
                string sqlCommand = "select *, bladeOwnership.id as bladeOwnershipID, bladeConfiguration.id as bladeConfigurationID from bladeOwnership " +
                                    "join bladeConfiguration on bladeOwnership.id = bladeConfiguration.ownershipID ";
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    using (SQLiteDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            bladeSpec newBlade = new bladeSpec(reader);
                            newBlade = checkKeepAlives(newBlade);
                            bladeStates.Add(newBlade);
                        }
                    }
                }

                // Put blades in an order of preference. First come unused blades, then used blades with an empty queue.
                IEnumerable<bladeSpec> unusedBlades = bladeStates.Where(x => x.currentOwner == null);
                IEnumerable<bladeSpec> emptyQueueBlades = bladeStates.Where(x => x.currentOwner != null && x.nextOwner == null);
                IEnumerable<bladeSpec> orderedBlades = unusedBlades.Concat(emptyQueueBlades);

                foreach (bladeSpec reqBlade in orderedBlades)
                {
                    resultCode res = tryRequestNode(reqBlade.bladeIP, requestorIP);
                    if (res == resultCode.success || res == resultCode.pending)
                        return new resultCodeAndBladeName() {bladeName = reqBlade.bladeIP, code = res};
                }
            }
            // Otherwise, all blades have full queues.
            return new resultCodeAndBladeName() { bladeName = null, code = resultCode.bladeQueueFull };
        }

        private static void notifyBootDirectorOfNode(bladeSpec blade)
        {
            Uri wcfURI = new Uri("http://localhost/bootMenuController");
            BasicHttpBinding myBind = new BasicHttpBinding();
            
            using (BootMenuWCFClient client = new BootMenuWCFClient(myBind, new EndpointAddress(wcfURI)))
            {
                try
                {
                    client.addMachine(blade.iLOIP);
                }
                catch (EndpointNotFoundException)
                {
                    logEvents.Add("Cannot find bootMenuController endpoint at " + wcfURI.ToString());
                }
            }
        }

        public static void keepAlive(string requestorIP)
        {
            lock (connLock)
            {
                string sqlCommand = "update bladeOwnership set lastKeepAlive=$NOW " +
                                    "where currentOwner = $ownerIP";
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    cmd.Parameters.AddWithValue("$ownerIP", requestorIP);
                    cmd.Parameters.AddWithValue("$NOW", DateTime.Now.ToString());
                    cmd.ExecuteNonQuery();
                }
            }
        }

        public static bool isBladeMine(string nodeIp, string requestorIp)
        {
            lock (connLock)
            {
                checkKeepAlives(requestorIp);

                string sqlCommand = "select count(*) from bladeOwnership " +
                                    "join bladeConfiguration on bladeOwnership.id = bladeConfiguration.ownershipID " +
                                    "where bladeOwnership.currentOwner = $requestorIp " + 
                                    "and   bladeConfiguration.bladeIP = $bladeIP ";
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    cmd.Parameters.AddWithValue("$bladeIP", nodeIp);
                    cmd.Parameters.AddWithValue("$requestorIp", requestorIp);
                    return (long)cmd.ExecuteScalar() > 0;
                }
            }
        }

        public static List<string> getLogEvents()
        {
            lock (logEvents)
            {
                List<string> toRet = new List<string>(logEvents);
                return toRet;
            }
        }

        public static resultCode selectSnapshotForBladeOrVM(string nodeIp, string newShot)
        {
            itemToAdd itm;
            lock (connLock)
            {
                bladeOwnership reqBlade = getBladeOrVMOwnershipByIP(nodeIp);
                if (reqBlade == null)
                    return resultCode.bladeNotFound;
                reqBlade.currentSnapshot = newShot;
                resultCode res = reqBlade.updateInDB(conn);
                if (res != resultCode.success)
                    return res;

                itm = reqBlade.toItemToAdd();
            }

            // re-create iscsi target/extent/etc if neccessary, but make sure we do this async, since it may take a few minutes to
            // get sense from FreeNAS.
            lock (currentSnapshotSelections)
            {
                if (currentSnapshotSelections.ContainsKey(nodeIp))
                    currentSnapshotSelections.Remove(nodeIp);
                currentSnapshotSelections.Add(nodeIp, resultCode.pending);
            }

            Task t = new Task(() =>
                    {
                        Program.repairBladeDeviceNodes(new itemToAdd[] {itm});
                        lock (currentSnapshotSelections)
                        {
                            currentSnapshotSelections[nodeIp] = resultCode.success;
                        }
                    }
                );
            t.Start();
            return resultCode.pending;
        }

        public static resultCode selectSnapshotForBladeOrVM_getProgress(string nodeIp)
        {
            lock (currentSnapshotSelections)
            {
                if (!currentSnapshotSelections.ContainsKey(nodeIp))
                    return resultCode.bladeNotFound;
                return currentSnapshotSelections[nodeIp];
            }
        }

        public static string getLastDeployedBIOSForBlade(string nodeIp)
        {
            lock (connLock)
            {
                bladeSpec reqBlade = getBladeByIP(nodeIp);
                if (reqBlade == null)
                    return null;

                return reqBlade.lastDeployedBIOS;
            }
        }

        public static resultCode rebootAndStartDeployingBIOSToBlade(string nodeIp, string requestorIP, string biosxml)
        {
            lock (connLock)
            {
                bladeSpec reqBlade = getBladeByIP(nodeIp);

                if (reqBlade.currentOwner != requestorIP)
                    return resultCode.bladeInUse;
                // First, check if this BIOS is already deployed.
                if (reqBlade.lastDeployedBIOS == biosxml)
                    return resultCode.noNeedLah;
                //  We need to:
                //  1) set this blade to boot into LTSP
                //  2) start the blade
                //  3) wait for it to boot
                //  4) SSH into it, and run conrep to configure the BIOS.
                
                // Mark the blade as BIOS-flashing. This will mean that, next time it boots, it will be served the LTSP image.
                reqBlade.currentlyHavingBIOSDeployed = true;
                reqBlade.updateInDB(conn);

                biosThreadState newState = new biosThreadState(nodeIp, reqBlade.iLOIP, biosxml);
                //lock (newState)
                {
                    if (biosUpdateState.TryAdd(nodeIp, newState) == false)
                    {
                        // the key was already present. Maybe it's finished a previous request.
                        if (checkBIOSWriteProgress(nodeIp) != resultCode.pending)
                        {
                            biosThreadState tmp;
                            biosUpdateState.TryRemove(nodeIp, out tmp);
                            if (biosUpdateState.TryAdd(nodeIp, newState) == false)
                                return resultCode.genericFail;
                        }
                        else
                        {
                            return resultCode.alreadyInProgress;
                        }
                    }

                    // otherwise, go ahead and spin up a new thread to handle this update.
                    newState.onBootFinish = SetBIOS;
                    newState.rebootThread = new Thread(ltspBootThread);
                    newState.rebootThread.Name = "Booting " + nodeIp + " to LTSP";
                    newState.rebootThread.Start(newState);
                }

                return resultCode.pending;
            }
        }

        public static resultCode rebootAndStartReadingBIOSConfiguration(string nodeIp, string requestorIP)
        {
            lock (connLock)
            {
                bladeSpec reqBlade = getBladeByIP(nodeIp);

                if (reqBlade.currentOwner != requestorIP)
                    return resultCode.bladeInUse;

                // Mark the blade as BIOS-flashing. This will mean that, next time it boots, it will be served the LTSP image.
                reqBlade.currentlyHavingBIOSDeployed = true;
                reqBlade.updateInDB(conn);

                biosThreadState newState = new biosThreadState(nodeIp, reqBlade.iLOIP, null);
                if (biosUpdateState.TryAdd(nodeIp, newState) == false)
                {
                    // the key was already present. Maybe it's finished a previous request.
                    if (checkBIOSWriteProgress(nodeIp) != resultCode.pending)
                    {
                        biosThreadState tmp;
                        biosUpdateState.TryRemove(nodeIp, out tmp);
                        if (biosUpdateState.TryAdd(nodeIp, newState) == false)
                            return resultCode.genericFail;
                    }
                    else
                    {
                        return resultCode.alreadyInProgress;
                    }
                }

                // otherwise, go ahead and spin up a new thread to handle this update.
                newState.onBootFinish = GetBIOS;
                newState.rebootThread = new Thread(ltspBootThread);
                newState.rebootThread.Name = "Booting " + nodeIp + " to LTSP";
                newState.rebootThread.Start(newState);

                return resultCode.pending;
            }
        }

        public static resultCode checkBIOSWriteProgress(string nodeIp)
        {
            lock (connLock)
            {
                biosThreadState newState;
                if (biosUpdateState.TryGetValue(nodeIp, out newState) == false)
                    return resultCode.bladeNotFound;

                if (!newState.isFinished)
                    return resultCode.pending;
                
                if (newState.result == resultCode.success)
                {
                    bladeOwnership bladeStatus = getBladeByIP(nodeIp);
                }
                return newState.result;
            }
        }

        public static resultCodeAndBIOSConfig checkBIOSReadProgress(string nodeIp)
        {
            lock (connLock)
            {
                biosThreadState newState;
                if (biosUpdateState.TryGetValue(nodeIp, out newState) == false)
                    return new resultCodeAndBIOSConfig( resultCode.bladeNotFound );

                if (!newState.isFinished)
                    return new resultCodeAndBIOSConfig(resultCode.pending );

                if (newState.result == resultCode.success)
                {
                    bladeOwnership bladeStatus = getBladeByIP(nodeIp);
                }
                return new resultCodeAndBIOSConfig(newState);
            }
        }

        private static void ltspBootThread(Object o)
        {
            biosThreadState param = (biosThreadState)o;
            param.result = resultCode.genericFail;
            _ltspBootThreadStart(param);
        }

        private static void _ltspBootThreadStart(biosThreadState param)
        {
            // Power cycle it
            using (hypervisor_iLo_HTTP hyp = new hypervisor_iLo_HTTP(param.iLoIP, Properties.Settings.Default.iloUsername, Properties.Settings.Default.iloPassword))
            {
                hyp.connect();
                while (true)
                {
                    hyp.powerOff();
                    if (hyp.getPowerStatus() == false)
                        break;
                    Thread.Sleep(TimeSpan.FromSeconds(3));
                }
                while (true)
                {
                    hyp.powerOn();
                    if (hyp.getPowerStatus() == true)
                        break;
                    Thread.Sleep(TimeSpan.FromSeconds(3));
                }
            }

            // Wait for it to boot.  Note that we don't ping the client repeatedly here - since the Ping class can cause 
            // a BSoD.. ;_; Instead, we wait for port 22 (SSH) to be open.
            param.connectDeadline = DateTime.Now + TimeSpan.FromMinutes(5);
            param.biosUpdateSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            param.biosUpdateSocket.BeginConnect(new IPEndPoint(IPAddress.Parse(param.nodeIp), 22), param.onBootFinish, param);
        }

        private static void copyDeploymentFilesToBlade(string nodeIP, string biosConfigFile)
        {
            using (var scp = new SftpClient(nodeIP, Properties.Settings.Default.ltspUsername, Properties.Settings.Default.ltspPassword))
            {
                scp.Connect();
                scp.WriteAllText("applyBIOS.sh", Properties.Resources.applyBIOS.Replace("\r\n", "\n"));
                scp.WriteAllText("getBIOS.sh", Properties.Resources.getBIOS.Replace("\r\n", "\n"));
                scp.WriteAllBytes("conrep", Properties.Resources.conrep);
                scp.WriteAllText("conrep.xml", Properties.Resources.conrep_xml.Replace("\r\n", "\n"));
                if (biosConfigFile != null)
                    scp.WriteAllText("newbios.xml", biosConfigFile.Replace("\r\n", "\n"));
            }
        }

        private static void GetBIOS(IAsyncResult ar)
        {
            biosThreadState state = (biosThreadState)ar.AsyncState;
            if (retryIfFailedConnect(ar, state))
                return;

            copyDeploymentFilesToBlade(state.nodeIp, null);

            using (SshClient exec = new SshClient(state.nodeIp, Properties.Settings.Default.ltspUsername, Properties.Settings.Default.ltspPassword))
            {
                exec.Connect();
                var commandResult = exec.RunCommand("bash ~/getBIOS.sh");


                if (commandResult.ExitStatus != 0)
                {
                    addLogEvent(string.Format("Reading BIOS on {0} resulted in error code {1}", state.nodeIp, commandResult.ExitStatus));
                    Debug.WriteLine(DateTime.Now + "Faied bios deploy, error code " + commandResult.ExitStatus);
                    Debug.WriteLine(DateTime.Now + "stdout " + commandResult.Result);
                    Debug.WriteLine(DateTime.Now + "stderr " + commandResult.Error);
                    state.result = resultCode.genericFail;
                }
                else
                {
                    addLogEvent(string.Format("Deployed BIOS successfully to {0}", state.nodeIp));
                    state.result = resultCode.success;
                }

                // Retrieve the output
                using (ScpClient scp = new ScpClient(state.nodeIp, Properties.Settings.Default.ltspUsername, Properties.Settings.Default.ltspPassword))
                {
                    scp.Connect();
                    commandResult = exec.RunCommand("bash ~/getBIOS.sh");
                    state.biosxml = null;

                    using (MemoryStream stream = new MemoryStream())
                    using (StreamWriter sw = new StreamWriter(stream))
                    {
                        scp.Download("currentbios.xml", stream);
                        sw.Flush();
                        stream.Position = 0;
                        using (StreamReader sr = new StreamReader(stream))
                        {
                            state.biosxml = sr.ReadToEnd();
                        }
                    }
                }
            }

            // All done, now we can power off and return.
            using (hypervisor_iLo_HTTP hyp = new hypervisor_iLo_HTTP(state.iLoIP, Properties.Settings.Default.iloUsername, Properties.Settings.Default.iloPassword))
            {
                hyp.connect();
                while (true)
                {
                    hyp.powerOff();
                    if (hyp.getPowerStatus() == false)
                        break;
                    Thread.Sleep(TimeSpan.FromSeconds(3));
                }
            }

            lock (connLock)
            {
                bladeSpec reqBlade = getBladeByIP(state.nodeIp);
                reqBlade.currentlyHavingBIOSDeployed = false;
                reqBlade.lastDeployedBIOS = state.biosxml;
                reqBlade.updateInDB(conn);
            }
            state.isFinished = true;
        }

        private static void SetBIOS(IAsyncResult ar)
        {
            biosThreadState state = (biosThreadState) ar.AsyncState;
            if (retryIfFailedConnect(ar, state))
                return;

            try
            {
                // Okay, now the box is up :)
                // SCP some needed files to it.
                copyDeploymentFilesToBlade(state.nodeIp, state.biosxml);

                // And execute the command to deploy the BIOS via SSH.
                SshCommand commandResult;
                using (SshClient exec = new SshClient(state.nodeIp, Properties.Settings.Default.ltspUsername, Properties.Settings.Default.ltspPassword))
                {
                    exec.Connect();
                    SshCommand status = exec.RunCommand("bash ~/applyBIOS.sh");

                    if (status.ExitStatus != 0)
                    {
                        addLogEvent(string.Format("Deploying BIOS on {0} resulted in error code {1}", state.nodeIp, status.ExitStatus));
                        Debug.WriteLine(DateTime.Now + "Faied bios deploy, error code " + status.ExitStatus);
                        Debug.WriteLine(DateTime.Now + "stdout " + status.Result);
                        Debug.WriteLine(DateTime.Now + "stderr " + status.Error);
                        state.result = resultCode.genericFail;
                    }
                    else
                    {
                        addLogEvent(string.Format("Deployed BIOS successfully to {0}", state.nodeIp));
                        state.result = resultCode.success;
                    }
                }


                // All done, now we can power off and return.
                using (hypervisor_iLo_HTTP hyp = new hypervisor_iLo_HTTP(state.iLoIP, Properties.Settings.Default.iloUsername, Properties.Settings.Default.iloPassword))
                {
                    hyp.connect();
                    while (true)
                    {
                        hyp.powerOff();
                        if (hyp.getPowerStatus() == false)
                            break;
                        Thread.Sleep(TimeSpan.FromSeconds(3));
                    }
                }

                lock (connLock)
                {
                    bladeSpec reqBlade = getBladeByIP(state.nodeIp);
                    reqBlade.currentlyHavingBIOSDeployed = false;
                    reqBlade.lastDeployedBIOS = state.biosxml;
                    reqBlade.updateInDB(conn);
                }
                state.isFinished = true;
            }
            catch (Exception e)
            {
                addLogEvent(string.Format("Deploying BIOS on {0} resulted in exception {1}", state.nodeIp, e.ToString()));
                state.result = resultCode.genericFail;
                state.isFinished = true;
            }
        }

        private static bool retryIfFailedConnect(IAsyncResult ar, biosThreadState state)
        {
            try
            {
                state.biosUpdateSocket.EndConnect(ar);
                if (state.biosUpdateSocket.Connected)
                    return false;
            }
            catch (SocketException)
            {
            }

            // We failed to connect, either because .EndConnect threw, or because the socket was not connected. Report failure 
            // (if our timeout has expired), or start another connection attempt if it has not.
            if (DateTime.Now > state.connectDeadline)
            {
                state.result = resultCode.genericFail;
                state.isFinished = true;
                lock (connLock)
                {
                    bladeSpec reqBlade = getBladeByIP(state.nodeIp);
                    reqBlade.currentlyHavingBIOSDeployed = false;
                    reqBlade.updateInDB(conn);

                    state.result = resultCode.genericFail;
                    state.isFinished = true;
                }
            }

            // Otherwise, queue up another connect attempt to just keep retrying.
            state.biosUpdateSocket.BeginConnect(new IPEndPoint(IPAddress.Parse(state.nodeIp), 22), state.onBootFinish, state);
            return true;
        }
    }

    public class bladeState
    {
        public bool isPoweredUp;
    }

    public class VMThreadState
    {
        public bladeSpec VMServer;
        public DateTime deployDeadline;
        public resultCodeAndBladeName currentProgress;
        public vmSpec childVM;
        public VMSoftwareSpec swSpec;
    }

    public class biosThreadState
    {
        public string biosxml;
        public string iLoIP;
        public string nodeIp;
        public bool isFinished;
        public resultCode result;
        public Socket biosUpdateSocket;
        public DateTime connectDeadline;
        public Thread rebootThread;
        public AsyncCallback onBootFinish;

        public biosThreadState(string nodeIp, string iLoIP, string biosxml)
        {
            this.nodeIp = nodeIp;
            this.iLoIP = iLoIP;
            this.biosxml = biosxml;
            isFinished = false;
        }
    }

    public enum GetBladeStatusResult
    {
        bladeNotFound,
        unused,
        yours,
        releasePending,
        notYours
    }

    public enum bladeStatus
    {
        unused,
        releaseRequested,
        inUseByDirector,
        inUse
    }

    public class resultCodeAndBladeName
    {
        public resultCode code;
        public string bladeName;
        public string waitToken;
    }

    public class resultCodeAndBIOSConfig
    {
        public resultCode code;
        public string BIOSConfig;

        // For XML de/ser
        // ReSharper disable once UnusedMember.Global
        public resultCodeAndBIOSConfig()
        {
        }

        public resultCodeAndBIOSConfig(resultCode newState)
        {
            this.code = newState;
            this.BIOSConfig = null;
        }

        public resultCodeAndBIOSConfig(biosThreadState state)
        {
            this.code = state.result;
            this.BIOSConfig = state.biosxml;
        }
    }

    public enum resultCode
    {
        success,
        bladeNotFound,
        bladeInUse,
        bladeQueueFull,
        pending,
        alreadyInProgress,
        genericFail,
        noNeedLah,
        unknown
    }
}