using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.ServiceModel;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using bladeDirector.bootMenuWCF;
using createDisks;
using hypervisors;

namespace bladeDirector
{
    /// <summary>
    /// Almost all main program logic. Note that the inheritor is expected to define the type of hypervisor we'll be working on.
    /// </summary>
    public abstract class hostStateDB_core
    {
        private List<string> logEvents = new List<string>();
        public TimeSpan keepAliveTimeout = TimeSpan.FromMinutes(1);

        private Object connLock = new object();
        private SQLiteConnection conn = null;
        public string dbFilename;

        private ConcurrentDictionary<string, biosThreadState> biosUpdateState = new ConcurrentDictionary<string, biosThreadState>();
        private Dictionary<string, VMThreadState> VMDeployState = new Dictionary<string, VMThreadState>();

        private Dictionary<string, resultCode> currentSnapshotSelections = new Dictionary<string, resultCode>();

        protected abstract hypervisor makeHypervisorForVM(vmSpec VM, bladeSpec parentBladeSpec);
        protected abstract hypervisor makeHypervisorForBlade_windows(bladeSpec bladeSpec);
        protected abstract hypervisor makeHypervisorForBlade_LTSP(bladeSpec bladeSpec);
        protected abstract hypervisor makeHypervisorForBlade_ESXi(bladeSpec bladeSpec);
        protected abstract void waitForESXiBootToComplete(hypervisor hyp);

        protected abstract void startBladePowerOff(bladeSpec nodeSpec, string iLoIp);
        protected abstract void startBladePowerOn(bladeSpec nodeSpec, string iLoIp);
        protected abstract void setCallbackOnTCPPortOpen(int nodePort, Action<biosThreadState> onError, Action<biosThreadState> action, DateTime deadline, biosThreadState biosThreadState);
        protected abstract NASAccess getNasForDevice(bladeSpec vmServer);

        private vmServerControl _vmServerControl;
        
        public hostStateDB_core(string basePath, vmServerControl newVmServerControl)
        {
            _vmServerControl = newVmServerControl;

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

        /// <summary>
        /// Init the hoststateDB with an in-memory database
        /// </summary>
        public hostStateDB_core(vmServerControl newVmServerControl)
        {
            _vmServerControl = newVmServerControl;

            lock (connLock)
            {
                dbFilename = ":memory:";

                conn = new SQLiteConnection("Data Source=" + dbFilename);
                conn.Open();

                createTables();
            }
        }
        
        private void dropDB()
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

        private void createTables()
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

        public void setKeepAliveTimeout(TimeSpan newTimeout)
        {
            keepAliveTimeout = newTimeout;
        }

        public void addLogEvent(string newEntry)
        {
            lock (logEvents)
            {
                logEvents.Add(DateTime.Now + " : " + newEntry);
            }
        }

        public string[] getAllBladeIP()
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

        public List<bladeSpec> getAllBladeInfo()
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

        public List<vmSpec> getAllVMInfo()
        {
            lock (connLock)
            {
                string sqlCommand = "select *, bladeOwnership.id as bladeOwnershipID, VMConfiguration.id as vmConfigurationID from bladeOwnership " +
                                    "join VMConfiguration on bladeOwnership.id = VMConfiguration.ownershipID ";
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    using (SQLiteDataReader reader = cmd.ExecuteReader())
                    {
                        List<vmSpec> toRet = new List<vmSpec>();

                        while (reader.Read())
                            toRet.Add(new vmSpec(reader));

                        return toRet.ToList();
                    }
                }
            }
        }
        public bladeOwnership getBladeOrVMOwnershipByIP(string IP)
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

        public bladeSpec getBladeByIP(string IP)
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

        public vmSpec getVMByIP(string bladeName)
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

        private vmSpec getVMByDBID(long VMID)
        {
            lock (connLock)
            {
                string sqlCommand = "select *, bladeOwnership.id as bladeOwnershipID, VMConfiguration.id as vmConfigurationID from bladeOwnership " +
                                    "join VMConfiguration on bladeOwnership.id = VMConfiguration.ownershipID " +
                                    "where VMConfiguration.id = $VMID";
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    cmd.Parameters.AddWithValue("$VMID", VMID);
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

        public vmSpec[] getVMByVMServerIP(string vmServerIP)
        {
            lock (connLock)
            {
                List<long> VMIDs = new List<long>();
                string sqlCommand = "select VMConfiguration.id as vmConfigurationID " +
                                    " from bladeConfiguration " +
                                    "join vmConfiguration on vmConfiguration.parentbladeID = bladeConfiguration.ID " +
                                    "join bladeownership on bladeownership.id = vmConfiguration.ownershipID " +
                                    "where bladeConfiguration.bladeIP = $vmServerIP";
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    cmd.Parameters.AddWithValue("$vmServerIP", vmServerIP);
                    using (SQLiteDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                            VMIDs.Add((long) reader["vmConfigurationID"]);
                    }
                }

                List<vmSpec> toRet = new List<vmSpec>();
                foreach (int vmID in VMIDs)
                {
                    toRet.Add(getVMByDBID(vmID));
                }
                return toRet.ToArray();
            }
        }

        public resultCode tryRequestNode(string bladeIP, string requestorID)
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

        private Dictionary<string, inProgressLogIn> currentlyRunningLogIns = new Dictionary<string, inProgressLogIn>(); 
        public string logIn(string hostIP)
        {
            lock (currentlyRunningLogIns)
            {
                string waitToken = hostIP.GetHashCode().ToString();

                // If there's already a login going on for this host, just use that one. Don't do two simultaneously.
                if (currentlyRunningLogIns.ContainsKey(waitToken))
                {
                    if (!currentlyRunningLogIns[waitToken].isFinished)
                        return currentlyRunningLogIns[waitToken].waitToken;
                    currentlyRunningLogIns.Remove(waitToken);
                }

                // Otherwise, make a new task and status, and start before we return.
                inProgressLogIn newLogIn = new inProgressLogIn()
                {
                    waitToken = waitToken,
                    hostIP = hostIP,
                    isFinished = false,
                    status = resultCode.pending
                };
                Task loginTask = new Task(() => { logInBlocking(newLogIn); });
                newLogIn.task = loginTask;
                currentlyRunningLogIns.Add(newLogIn.waitToken, newLogIn);
                loginTask.Start();

                return newLogIn.waitToken;
            }
        }

        public resultCode getLogInProgress(string waitToken)
        {
            lock (currentlyRunningLogIns)
            {
                if (currentlyRunningLogIns.ContainsKey(waitToken))
                    return currentlyRunningLogIns[waitToken].status;
                else
                    return resultCode.bladeNotFound;
            }
        }

        private void logInBlocking(inProgressLogIn login)
        {
            // Clean up anything that we are currently preparing for this owner
            IEnumerable<vmSpec> bootingVMs = getAllVMInfo().Where(x => x.currentOwner == "vmserver" && x.nextOwner == login.hostIP);
            foreach (vmSpec allocated in bootingVMs)
            {
                allocated.nextOwner = null;
                releaseVM(allocated);
            }

            // Clean up any hosts this blade has left over from any previous run
            lock (connLock)
            {
                IEnumerable<bladeSpec> allocedBlades = getAllBladeInfo().Where(x => x.currentOwner == login.hostIP);
                foreach (bladeSpec allocated in allocedBlades)
                    releaseBlade(allocated, login.hostIP, false);

                IEnumerable<vmSpec> allocedVMs = getAllVMInfo().Where(x => x.currentOwner == login.hostIP);
                foreach (vmSpec allocated in allocedVMs)
                    releaseVM(allocated);
            }
            lock (currentlyRunningLogIns)
            {
                login.status = resultCode.success;
                login.isFinished = true;
            }
        }

        private void checkKeepAlives(string reqBladeIP)
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

        private void checkKeepAlives(vmSpec reqVM)
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

        private bladeSpec checkKeepAlives(bladeSpec reqBlade)
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

        public string[] getBladesByAllocatedServer(string NodeIP)
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

        public void initWithBlades(string[] bladeIPs)
        {
            bladeSpec[] specs = new bladeSpec[bladeIPs.Length];
            int n = 0;
            foreach (string bladeIP in bladeIPs)
                specs[n++] = new bladeSpec(bladeIP, n.ToString(), n.ToString(), (ushort)n, false, null);

            initWithBlades(specs);
        }

        public void initWithBlades(bladeSpec[] bladeSpecs)
        {
            lock (connLock)
            {
                dropDB();
                createTables();

                foreach (bladeSpec spec in bladeSpecs)
                    addNode(spec);
            }
        }

        public void addNode(bladeSpec spec)
        {
            spec.createInDB(conn);
        }

        public GetBladeStatusResult getBladeStatus(string nodeIp, string requestorIp)
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

        public resultCode releaseBladeOrVM(string NodeIP, string RequestorIP, bool force = false)
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

        private void releaseBlade(bladeSpec reqBlade, string RequestorIP, bool force)
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

            // Kill off any pending BIOS deployments ASAP. Careful about how we lock, though, so we don't cause a deadlock.
            bool isLocked = false;
            try
            {
                Monitor.Enter(biosUpdateState);
                isLocked = true;

                if (biosUpdateState.ContainsKey(reqBlade.bladeIP))
                {
                    biosThreadState toCancel = biosUpdateState[reqBlade.bladeIP];
                    if (!toCancel.isFinished)
                    {
                        toCancel.connectDeadline = DateTime.MinValue;
                        // Wait until it exits. Release our lock while we wait.
                        Monitor.Exit(biosUpdateState);
                        isLocked = false;
                        while (!toCancel.isFinished)
                        {
                            logEvents.Add("Waiting for BIOS operation on " + reqBlade.bladeIP + " to cancel");
                            Thread.Sleep(TimeSpan.FromSeconds(10));
                        }
                        Monitor.Enter(biosUpdateState);
                        isLocked = true;
                    }
                    biosThreadState foo;
                    biosUpdateState.TryRemove(reqBlade.bladeIP, out foo);
                }
            }
            finally
            {
                if (isLocked)
                    Monitor.Exit(biosUpdateState);
            }

            // Reset any VM-server the blade may be
            reqBlade.currentlyBeingAVMServer = reqBlade.currentlyHavingBIOSDeployed = false;
            foreach (vmSpec child in getVMByVMServerIP(reqBlade.bladeIP))
                releaseVM(child);

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

        private void releaseVM(vmSpec toDel)
        {
            bladeSpec parentBladeSpec = getConfigurationOfBladeByID((int)toDel.parentBladeID);
            bool vmServerIsEmpty = getVMByVMServerIP(parentBladeSpec.bladeIP).Length == 1;

            // If we are currently being deployed, we must wait until we are at a point wherby we can abort the deploy. We need to
            // be careful how to lock here, otherwise we risk deadlocking.
            bool islocked = false;
            try
            {
                Monitor.Enter(VMDeployState);
                islocked = true;

                if (toDel.VMIP != null &&
                    VMDeployState.ContainsKey(toDel.VMIP.ToString()) &&
                    VMDeployState[toDel.VMIP].currentProgress.code != resultCode.pending)
                {
                    // Ahh, this VM is currently being deployed. We can't release it until the thread doing the deployment says so.
                    VMDeployState[toDel.VMIP].deployDeadline = DateTime.MinValue;
                    while (VMDeployState[toDel.VMIP].currentProgress.code == resultCode.pending)
                    {
                        logEvents.Add("Waiting for VM deploy on " + toDel.VMIP + " to cancel");

                        Monitor.Exit(VMDeployState);
                        islocked = false;
                        Thread.Sleep(TimeSpan.FromSeconds(10));

                        Monitor.Enter(VMDeployState);
                        islocked = true;
                    }
                }

                lock (connLock)
                {
                    toDel.deleteInDB(conn);
                }
            }
            finally
            {
                if (islocked)
                    Monitor.Exit(VMDeployState);
            }

            // VMs always get destroyed on release.
            toDel.deleteInDB(conn);

            // This might fail, since the VM may not have been added to the VM server yet.
            try
            {
                using (hypervisor hyp = makeHypervisorForVM(toDel, parentBladeSpec))
                {
                    hyp.powerOff();
                }
            }
            catch (SocketException) { }
            catch (WebException) { }
            catch (VMNotFoundException) { }

            // If the VM server is now empty, we can release it.
            if (vmServerIsEmpty)
                releaseBladeOrVM(parentBladeSpec.bladeIP, "vmserver");
        }

        public resultCode forceBladeAllocation(string nodeIp, string newOwner)
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

        public bladeSpec getConfigurationOfBlade(string nodeIp)
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

        public bladeSpec getConfigurationOfBladeByID(int nodeID)
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

        public string getCurrentSnapshotForBladeOrVM(string nodeIp)
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

        public resultCodeAndBladeName RequestAnySingleVM(string requestorIP, VMHardwareSpec hwSpec, VMSoftwareSpec swReq )
        {
            if (hwSpec.memoryMB % 4 != 0)
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
                    bladeSpec freeVMServer = allBladeInfo.FirstOrDefault((x) => x.currentlyBeingAVMServer && x.canAccommodate(conn, hwSpec));
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

                    // Create rows for the child VM in the DB. Before we write it to the database, check we aren't about to add a 
                    // VM which conflicts with anything. If it does conflict, check the conflict is with an unused VM, and then 
                    // delete it before we add.
                    vmSpec childVM = freeVMServer.createChildVM(conn, hwSpec, swReq, requestorIP);
                    // Check for conflicts
                    foreach (vmSpec currentlyExistingVM in getVMByVMServerIP(freeVMServer.bladeIP))
                    {
                        if (childVM.conflictsWith(currentlyExistingVM))
                        {
                            if (childVM.state != bladeStatus.unused)
                            {
                                // Uh-oh, what's going on here? A conflict with an existing, still-allocated VM? That sounds bad..
                                return new resultCodeAndBladeName() { code = resultCode.bladeInUse };
                            }

                            releaseVM(childVM);
                        }
                    }

                    string waitToken = childVM.VMIP.ToString();

                    // Is a deploy of this VM already in progress?
                    if (VMDeployState.ContainsKey(waitToken))
                    {
                        if (VMDeployState[waitToken].currentProgress.code != resultCode.pending)
                        {
                            // Yes, a deploy was in progress - but now it is finished.
                            VMDeployState.Remove(waitToken);
                        }
                        else
                        {
                            // Oh, a deploy is already in progress for this VM. This should never happen, since .createChildVM
                            // should never return an already-existing VM.
                            return new resultCodeAndBladeName() { code = resultCode.bladeInUse };
                        }
                    }

                    childVM.createInDB(conn);

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

        private void VMServerBootThread(object param)
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

        private object VMServerBootThreadLock = new object();
        private Dictionary<long, bladeState> physicalBladeLocks = new Dictionary<long, bladeState>();
        private void _VMServerBootThread(VMThreadState threadState)
        {
            Debug.WriteLine(DateTime.Now + threadState.childVM.VMIP + ": enter thread");
            // First, bring up the physical machine. It'll get the ESXi ISCSI config and boot up.
            using (hypervisor hyp = makeHypervisorForBlade_ESXi(threadState.VMServer))
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

                            waitForESXiBootToComplete(hyp);

                            // Once it's powered up, we ensure the datastore is mounted okay. Sometimes I'm seeing ESXi hosts boot
                            // with an inaccessible NFS datastore, so remount if neccessary. Retry this since it doesn't seem to 
                            // always work first time.
                            _vmServerControl.mountDataStore(hyp, "esxivms", "store.xd.lan", "/mnt/SSDs/esxivms");

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
                // exist. We power off by directory and also by name, just in case a previous provision has left the VM hanging around.
                string dstDatastoreDirEscaped = destDirDatastoreType.Replace("[", "\\[").Replace("]", "\\]");
                hypervisor.doWithRetryOnSomeExceptions(() => hyp.startExecutable("vim-cmd", "vmsvc/power.off `vim-cmd vmsvc/getallvms | grep \"" + dstDatastoreDirEscaped + "\"`"));
                hypervisor.doWithRetryOnSomeExceptions(() => hyp.startExecutable("vim-cmd", "vmsvc/power.off `vim-cmd vmsvc/getallvms | grep \"" + threadState.childVM.displayName + "\"`"));
                hypervisor.doWithRetryOnSomeExceptions(() => hyp.startExecutable("vim-cmd", "vmsvc/unregister `vim-cmd vmsvc/getallvms | grep \"" + dstDatastoreDirEscaped + "\"`"));
                hypervisor.doWithRetryOnSomeExceptions(() => hyp.startExecutable("vim-cmd", "vmsvc/unregister `vim-cmd vmsvc/getallvms | grep \"" + threadState.childVM.displayName  + "\"`"));

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
                // We do this with a retry, because I'm seeing it fail occasionally >_<
                hypervisor.doWithRetryOnSomeExceptions(() =>
                    doCmdAndCheckSuccess(hyp, "vim-cmd", " solo/registervm " + vmxPath)
                    ) ;

                Debug.WriteLine(DateTime.Now + threadState.childVM.VMIP + ": created");

                if (threadState.deployDeadline < DateTime.Now)
                    throw new TimeoutException();
            }

            // If the VM already has disks set up, delete them.
            // Note that we own the VM now, but want to make the VM as if the real requestor owned it.
            itemToAdd itm = threadState.childVM.toItemToAdd(true);

            if (threadState.deployDeadline < DateTime.Now)
                throw new TimeoutException();

            Debug.WriteLine(DateTime.Now + threadState.childVM.VMIP + ": deleting");
            NASAccess nas = getNasForDevice(threadState.VMServer);
            Program.deleteBlade(itm.cloneName, nas);

            if (threadState.deployDeadline < DateTime.Now)
                throw new TimeoutException();

            Debug.WriteLine(DateTime.Now + threadState.childVM.VMIP + ": adding");
            // Now create the disks, and customise the VM  by naming it appropriately.
            Program.addBlades(nas, new[] {itm}, itm.snapshotName, "localhost/bladeDirector", "bladebasestable-esxi", null, null,
                (a, b) => {
                    return (hypervisorWithSpec<hypSpec_vmware>) makeHypervisorForVM(threadState.childVM, threadState.VMServer);
                }, threadState.deployDeadline);

            Debug.WriteLine(DateTime.Now + threadState.childVM.VMIP + ": add complete");

            // TODO: Ability to deploy transportDriver

            if (threadState.deployDeadline < DateTime.Now)
                throw new TimeoutException();

            // Now we can select the new snapshot
            Debug.WriteLine(DateTime.Now + threadState.childVM.VMIP + ": selecting");
            resultCode snapshotRes = selectSnapshotForBladeOrVM(threadState.childVM.VMIP, itm.snapshotName);
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
                childVM.currentOwner = threadState.childVM.nextOwner;
                threadState.childVM.nextOwner = null;
                childVM.lastKeepAlive = DateTime.Now;
                childVM.updateInDB(conn);
            }
            threadState.currentProgress.bladeName = threadState.childVM.VMIP;
            threadState.currentProgress.code = resultCode.success;
        }

        private void doCmdAndCheckSuccess(hypervisor hyp, string cmd, string args)
        {
            executionResult res = hypervisor.doWithRetryOnSomeExceptions(() => hyp.startExecutable(cmd, args));
            if (res.resultCode != 0)
            {
                logEvents.Add(string.Format("Command '{0}' with args '{1}' returned failure code {2}; stdout is '{3} and stderr is '{4}'", cmd, args, res.resultCode, res.stdout, res.stderr));
                throw new hypervisorExecutionException("failed to execute ssh command");
            }
        }

        public resultCodeAndBladeName getProgressOfVMRequest(string waitToken)
        {
            if (waitToken == null)
                return new resultCodeAndBladeName() { code = resultCode.bladeNotFound, waitToken = waitToken};

            if (!Monitor.TryEnter(VMDeployState, TimeSpan.FromSeconds(15)))
            {
                return new resultCodeAndBladeName() { code = resultCode.unknown, waitToken = waitToken };
            }
            else
            {
                try
                {
                    lock (VMDeployState)
                    {
                        if (!VMDeployState.ContainsKey(waitToken))
                            return new resultCodeAndBladeName() { code = resultCode.unknown, waitToken = waitToken };

                        return VMDeployState[waitToken].currentProgress;
                    }
                }
                finally 
                {
                    Monitor.Exit(VMDeployState);
                }
            }
        }
        
        public resultCodeAndBladeName RequestAnySingleNode(string requestorIP)
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

        private void notifyBootDirectorOfNode(bladeSpec blade)
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

        public void keepAlive(string requestorIP)
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

        public bool isBladeMine(string nodeIp, string requestorIp)
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

        public List<string> getLogEvents()
        {
            lock (logEvents)
            {
                List<string> toRet = new List<string>(logEvents);
                return toRet;
            }
        }

        public resultCode selectSnapshotForBladeOrVM(string nodeIp, string newShot)
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
            // get sense from FreeNAS. It's not work doing this for virtual machines, since they are re-created on allocation
            if (!itm.isVirtualMachine)
            {
                lock (currentSnapshotSelections)
                {
                    if (currentSnapshotSelections.ContainsKey(nodeIp))
                        currentSnapshotSelections.Remove(nodeIp);
                    currentSnapshotSelections.Add(nodeIp, resultCode.pending);
                }

                Task t = new Task(() =>
                {
                    try
                    {
                        Program.repairBladeDeviceNodes(new [] { itm });
                        lock (currentSnapshotSelections)
                        {
                            currentSnapshotSelections[nodeIp] = resultCode.success;
                        }
                        return;
                    }
                    catch (Exception)
                    {
                        currentSnapshotSelections[nodeIp] = resultCode.genericFail;
                        throw;
                    }
                }
                    );
                t.Start();
                return resultCode.pending;
            }

            return resultCode.success;
        }

        public resultCode selectSnapshotForBladeOrVM_getProgress(string nodeIp)
        {
            lock (currentSnapshotSelections)
            {
                if (!currentSnapshotSelections.ContainsKey(nodeIp))
                    return resultCode.bladeNotFound;
                return currentSnapshotSelections[nodeIp];
            }
        }

        public string getLastDeployedBIOSForBlade(string nodeIp)
        {
            lock (connLock)
            {
                bladeSpec reqBlade = getBladeByIP(nodeIp);
                if (reqBlade == null)
                    return null;

                return reqBlade.lastDeployedBIOS;
            }
        }

        public resultCode rebootAndStartDeployingBIOSToBlade(string nodeIp, string requestorIP, string biosxml)
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

                biosThreadState newState = new biosThreadState(reqBlade, biosxml);
                lock (biosUpdateState)
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
                    newState.onBootFailure = handleReadOrWriteBIOSError;
                    newState.rebootThread = new Thread(ltspBootThread);
                    newState.rebootThread.Name = "Booting " + nodeIp + " to LTSP";
                    newState.rebootThread.Start(newState);
                }

                return resultCode.pending;
            }
        }

        public resultCode rebootAndStartReadingBIOSConfiguration(string nodeIp, string requestorIP)
        {
            lock (connLock)
            {
                bladeSpec reqBlade = getBladeByIP(nodeIp);

                if (reqBlade.currentOwner != requestorIP)
                    return resultCode.bladeInUse;

                // Mark the blade as BIOS-flashing. This will mean that, next time it boots, it will be served the LTSP image.
                reqBlade.currentlyHavingBIOSDeployed = true;
                reqBlade.updateInDB(conn);

                biosThreadState newState = new biosThreadState(reqBlade, null);
                lock (biosUpdateState)
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
                    newState.onBootFinish = GetBIOS;
                    newState.onBootFailure = handleReadOrWriteBIOSError;
                    newState.rebootThread = new Thread(ltspBootThread);
                    newState.rebootThread.Name = "Booting " + nodeIp + " to LTSP";
                    newState.rebootThread.Start(newState);

                    return resultCode.pending;
                }
            }
        }

        public resultCode checkBIOSWriteProgress(string nodeIp)
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

        public resultCodeAndBIOSConfig checkBIOSReadProgress(string nodeIp)
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

        private void ltspBootThread(Object o)
        {
            biosThreadState param = (biosThreadState)o;
            param.result = resultCode.genericFail;
            _ltspBootThreadStart(param);
        }

        private void _ltspBootThreadStart(biosThreadState param)
        {
            // Power cycle it

            startBladePowerOff(param.nodeSpec, param.nodeSpec.iLOIP);
            startBladePowerOn(param.nodeSpec, param.nodeSpec.iLOIP);

            // Wait for it to boot.  Note that we don't ping the client repeatedly here - since the Ping class can cause 
            // a BSoD.. ;_; Instead, we wait for port 22 (SSH) to be open.
            param.connectDeadline = DateTime.Now + TimeSpan.FromMinutes(5);
            setCallbackOnTCPPortOpen(22, param.onBootFinish, param.onBootFailure, param.connectDeadline, param);
        }

        private void copyDeploymentFilesToBlade(bladeSpec nodeSpec, string biosConfigFile)
        {
            using (hypervisor hyp = makeHypervisorForBlade_LTSP(nodeSpec))
            {
                Dictionary<string, string> toCopy = new Dictionary<string, string>()
                {
                    {"applyBIOS.sh", Properties.Resources.applyBIOS.Replace("\r\n", "\n")},
                    {"getBIOS.sh", Properties.Resources.getBIOS.Replace("\r\n", "\n")},
                    {"conrep.xml", Properties.Resources.conrep_xml.Replace("\r\n", "\n")},
                };
                if (biosConfigFile != null)
                    toCopy.Add("newbios.xml", biosConfigFile.Replace("\r\n", "\n"));

                foreach (KeyValuePair<string, string> kvp in toCopy)
                {
                    hypervisor.doWithRetryOnSomeExceptions(() => { hyp.copyToGuestFromBuffer(kvp.Key, kvp.Value); },
                        TimeSpan.FromSeconds(10), TimeSpan.FromMinutes(3));
                }
                // And copy this file specifically as binary.
                hypervisor.doWithRetryOnSomeExceptions(() =>
                {
                    hyp.copyToGuestFromBuffer("conrep", Properties.Resources.conrep);
                }, TimeSpan.FromSeconds(10), TimeSpan.FromMinutes(3));
            }
        }

        private void handleReadOrWriteBIOSError(biosThreadState state)
        {
            state.result = resultCode.genericFail;
            state.isFinished = true;
            lock (connLock)
            {
                bladeSpec reqBlade = getBladeByIP(state.nodeSpec.bladeIP);
                reqBlade.currentlyHavingBIOSDeployed = false;
                reqBlade.updateInDB(conn);

                state.result = resultCode.genericFail;
                state.isFinished = true;
            }
        }

        private void GetBIOS(biosThreadState state)
        {
            copyDeploymentFilesToBlade(state.nodeSpec, null);

            using (hypervisor hyp = makeHypervisorForBlade_LTSP(state.nodeSpec))
            {
                executionResult res = hyp.startExecutable("bash", "~/getBIOS.sh");
                if (res.resultCode != 0)
                {
                    addLogEvent(string.Format("Reading BIOS on {0} resulted in error code {1}", state.nodeSpec.bladeIP, res.resultCode));
                    Debug.WriteLine(DateTime.Now + "Faied bios deploy, error code " + res.resultCode);
                    Debug.WriteLine(DateTime.Now + "stdout " + res.stdout);
                    Debug.WriteLine(DateTime.Now + "stderr " + res.stderr);
                    state.result = resultCode.genericFail;
                }
                else
                {
                    addLogEvent(string.Format("Deployed BIOS successfully to {0}", state.nodeSpec.bladeIP));
                    state.result = resultCode.success;
                }

                // Retrieve the output
                state.biosxml = hyp.getFileFromGuest("currentbios.xml");

                // All done, now we can power off and return.
                hyp.powerOff();
            }

            lock (connLock)
            {
                bladeSpec reqBlade = getBladeByIP(state.nodeSpec.bladeIP);
                reqBlade.currentlyHavingBIOSDeployed = false;
                reqBlade.lastDeployedBIOS = state.biosxml;
                reqBlade.updateInDB(conn);
            }
            state.isFinished = true;
        }

        private void SetBIOS(biosThreadState state)
        {
            try
            {
                // Okay, now the box is up :)
                // SCP some needed files to it.
                copyDeploymentFilesToBlade(state.nodeSpec, state.biosxml);

                // And execute the command to deploy the BIOS via SSH.
                using (hypervisor hyp = makeHypervisorForBlade_LTSP(state.nodeSpec))
                {
                    executionResult res = hyp.startExecutable("bash", "~/applyBIOS.sh");
                    if (res.resultCode != 0)
                    {
                        addLogEvent(string.Format("Deploying BIOS on {0} resulted in error code {1}", state.nodeSpec.bladeIP, res.resultCode));
                        Debug.WriteLine(DateTime.Now + "Faied bios deploy, error code " + res.resultCode);
                        Debug.WriteLine(DateTime.Now + "stdout " + res.stdout);
                        Debug.WriteLine(DateTime.Now + "stderr " + res.stderr);
                        state.result = resultCode.genericFail;
                    }
                    else
                    {
                        addLogEvent(string.Format("Deployed BIOS successfully to {0}", state.nodeSpec.bladeIP));
                        state.result = resultCode.success;
                    }

                    // All done, now we can power off and return.
                    hyp.powerOff();
                }

                lock (connLock)
                {
                    bladeSpec reqBlade = getBladeByIP(state.nodeSpec.bladeIP);
                    reqBlade.currentlyHavingBIOSDeployed = false;
                    reqBlade.lastDeployedBIOS = state.biosxml;
                    reqBlade.updateInDB(conn);
                }
                state.isFinished = true;
            }
            catch (Exception e)
            {
                addLogEvent(string.Format("Deploying BIOS on {0} resulted in exception {1}", state.nodeSpec.bladeIP, e.ToString()));
                state.result = resultCode.genericFail;
                state.isFinished = true;
            }
        }

        public string getFreeNASSnapshotPath(string requestorIp, string nodeIp)
        {
            bladeOwnership ownership = getBladeOrVMOwnershipByIP(nodeIp);
            if (ownership == null)
                return String.Empty;
            return String.Format("{0}-{1}-{2}", nodeIp, requestorIp, ownership.currentSnapshot);
        }
    }

    public class inProgressLogIn
    {
        public string hostIP;
        public string waitToken;
        public Task task;
        public bool isFinished;
        public resultCode status;
    }

    public abstract class vmServerControl
    {
        public abstract void mountDataStore(hypervisor hyp, string dataStoreName, string serverName, string mountPath);
    }

    public class vmServerControl_mocked : vmServerControl
    {
        public override void mountDataStore(hypervisor hyp, string dataStoreName, string serverName, string mountPath)
        {
            // TODO: store somewhere
        }
    }

    public class vmServerControl_ESXi : vmServerControl
    {
        public override void mountDataStore(hypervisor hyp, string dataStoreName, string serverName, string mountPath)
        {
            string expectedLine = String.Format("{0} is {1} from {2} mounted available", dataStoreName, mountPath, serverName);

            string[] nfsMounts = hypervisor.doWithRetryOnSomeExceptions(() => hyp.startExecutable("esxcfg-nas", "-l")).stdout.Split(new[] { "\n", "\r" }, StringSplitOptions.RemoveEmptyEntries);
            string foundMount = nfsMounts.SingleOrDefault(x => x.Contains(expectedLine));
            while (foundMount == null)
            {
                hypervisor.doWithRetryOnSomeExceptions(() => hyp.startExecutable("esxcfg-nas", "-d " + dataStoreName));
                hypervisor.doWithRetryOnSomeExceptions(() => hyp.startExecutable("esxcfg-nas", "-a --host " + serverName + " --share " + mountPath + " " + dataStoreName));
                hypervisor.doWithRetryOnSomeExceptions(() => hyp.startExecutable("esxcfg-rescan", "--all"));

                nfsMounts = hypervisor.doWithRetryOnSomeExceptions(() => hyp.startExecutable("esxcfg-nas", "-l")).stdout.Split(new[] { "\n", "\r" }, StringSplitOptions.RemoveEmptyEntries);
                foundMount = nfsMounts.SingleOrDefault(x => x.Contains(expectedLine));
            }
        }
    }
}