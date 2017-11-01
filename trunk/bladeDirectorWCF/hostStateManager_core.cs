using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.ServiceModel;
using System.Threading;
using System.Threading.Tasks;
using bladeDirectorWCF.Properties;
using createDisks;
using hypervisors;

namespace bladeDirectorWCF
{
    public enum handleTypes
    {
        LGI, // Login Request
        REL, // Release of VM or blade
        DEP, // Deploying a VM server
        BOS, // Reading or writing BIOS configuration
        SHT, // Setting a blade snapshot
    }

    public enum resourceSharingMode
    {
        FirstComeFirstServed,
        Proportional
    }


    /// <summary>
    /// Almost all main program logic. Note that the inheritor is expected to define the type of hypervisor we'll be working on.
    /// </summary>
    public abstract class hostStateManager_core
    {
        private List<string> _logEvents = new List<string>();

        public IBiosReadWrite biosRWEngine { get; private set; }
        public TimeSpan keepAliveTimeout = TimeSpan.FromMinutes(1);

        public readonly hostDB db;

        public readonly resourceSharingMode shareMode;

        private Dictionary<waitToken, VMThreadState> _vmDeployState = new Dictionary<waitToken, VMThreadState>();

        private Dictionary<waitToken, inProgressOperation> _currentlyRunningLogIns = new Dictionary<waitToken, inProgressOperation>();
        private Dictionary<waitToken, inProgressOperation> _currentlyRunningReleases = new Dictionary<waitToken, inProgressOperation>();
        private Dictionary<waitToken, inProgressOperation> _currentBIOSOperations = new Dictionary<waitToken, inProgressOperation>();
        private Dictionary<waitToken, inProgressOperation> _currentlyRunningSnapshotSets = new Dictionary<waitToken, inProgressOperation>();

        public abstract hypervisor makeHypervisorForVM(lockableVMSpec VM, lockableBladeSpec parentBladeSpec);
        public abstract hypervisor makeHypervisorForBlade_windows(lockableBladeSpec bladeSpec);
        public abstract hypervisor makeHypervisorForBlade_LTSP(lockableBladeSpec bladeSpec);
        public abstract hypervisor makeHypervisorForBlade_ESXi(lockableBladeSpec bladeSpec);

        protected abstract void waitForESXiBootToComplete(hypervisor hyp);
        public abstract void startBladePowerOff(lockableBladeSpec blade);
        public abstract void startBladePowerOn(lockableBladeSpec blade);
        public abstract void setCallbackOnTCPPortOpen(int nodePort, ManualResetEvent onCompletion, ManualResetEvent onError , DateTime deadline, biosThreadState biosThreadState);
        protected abstract NASAccess getNasForDevice(bladeSpec vmServer);

        private readonly string _basePath;
        private vmServerControl _vmServerControl;
        private string _ipxeUrl;

        protected hostStateManager_core(string basePath, vmServerControl newVmServerControl, IBiosReadWrite newBiosReadWrite, resourceSharingMode newShareMode = resourceSharingMode.FirstComeFirstServed)
        {
            _basePath = basePath;
            _vmServerControl = newVmServerControl;
            biosRWEngine = newBiosReadWrite;
            shareMode = newShareMode;

            db = new hostDB(basePath);
        }

        /// <summary>
        /// Init the hoststateDB with an in-memory database
        /// </summary>
        protected hostStateManager_core(vmServerControl newVmServerControl, IBiosReadWrite newBiosReadWrite, resourceSharingMode newShareMode = resourceSharingMode.FirstComeFirstServed)
        {
            _vmServerControl = newVmServerControl;
            biosRWEngine = newBiosReadWrite;
            shareMode = newShareMode;
            db = new hostDB();
        }
       
        public void setKeepAliveTimeout(TimeSpan newTimeout)
        {
            keepAliveTimeout = newTimeout;
        }

        public TimeSpan getKeepAliveTimeout()
        {
            return keepAliveTimeout;
        }

        public void addLogEvent(string newEntry)
        {
            Debug.WriteLine(newEntry);
            Console.WriteLine(newEntry);
            lock (_logEvents)
            {
                _logEvents.Add(DateTime.Now + " : " + newEntry);
            }
        }

        public hypervisor makeHypervisorForVM(string bladeIP, string VMIP, bool permitAccessDuringBIOS, bool permitAccessDuringDeploy)
        {
            using (lockableBladeSpec blade = db.getBladeByIP(bladeIP, bladeLockType.lockIPAddresses, bladeLockType.lockNone, permitAccessDuringBIOS, permitAccessDuringDeploy))
            {
                using (lockableVMSpec VM = db.getVMByIP(VMIP,
                    bladeLockType.lockIPAddresses | bladeLockType.lockSnapshot | bladeLockType.lockOwnership | bladeLockType.lockVirtualHW,
                    bladeLockType.lockNone))
                {
                    return makeHypervisorForVM(VM, blade);
                }
            }
        }

        public hypervisor makeHypervisorForBlade_ESXi(string bladeIP)
        {
            using (lockableBladeSpec blade = db.getBladeByIP(bladeIP, bladeLockType.lockSnapshot, bladeLockType.lockNone, true, true))
            {
                return makeHypervisorForBlade_ESXi(blade);
            }
        }

        public hypervisor makeHypervisorForBlade_LTSP(string bladeIP)
        {
            using (var blade = db.getBladeByIP(bladeIP, bladeLockType.lockSnapshot, bladeLockType.lockNone))
            {
                return makeHypervisorForBlade_LTSP(blade);
            }
        }

        private result requestBlade(lockableBladeSpec reqBlade, string requestorID)
        {
            checkKeepAlives(reqBlade);

            string bladeIP = reqBlade.spec.bladeIP;

            // If the blade is currently unused, we can just take it.
            if (reqBlade.spec.state == bladeStatus.unused)
            {
                reqBlade.spec.currentOwner = requestorID;
                reqBlade.spec.state = bladeStatus.inUse;
                reqBlade.spec.lastKeepAlive = DateTime.Now;
                reqBlade.spec.currentlyBeingAVMServer = false;
                reqBlade.spec.vmDeployState = VMDeployStatus.notBeingDeployed;

                string msg = "Blade " + requestorID + " requested blade " + bladeIP + "(success, blade was idle)";
                addLogEvent(msg);
                notifyBootDirectorOfNode(reqBlade.spec);
                return new result(resultCode.success, msg);
            }

            // Otherwise, we need to request that the blade is released, and return 'pending'. 
            // Note that we don't permit a requestor to both own the blade, and be in the queue - this is because the
            // requestor would be unable to determine when its blade is allocated. We just return queuefull in that
            // situation.
            if (reqBlade.spec.currentOwner == requestorID)
            {
                string msg = "Blade " + requestorID + " requested blade " + bladeIP + "(failure, blade is already owned by this blade)";
                addLogEvent(msg);
                return new result(resultCode.bladeQueueFull, msg);
            }

            // If the blade is already queued as requested, just report OK and leave it there,
            if (reqBlade.spec.nextOwner == requestorID)
            {
                string msg = "Blade " + requestorID + " requested blade " + bladeIP + "(success, requestor was already in queue)";
                addLogEvent(msg);
                notifyBootDirectorOfNode(reqBlade.spec);
                return new result(resultCode.success, msg);
            }

            // See if the blade queue is actually full
            if (reqBlade.spec.nextOwner != null)
            {
                string msg = "Blade " + requestorID + " requested blade " + bladeIP + "(failure, blade queue is full)";
                addLogEvent(msg);
                return new result(resultCode.bladeQueueFull, msg);
            }

            // It's all okay, so add us to the queue.
            reqBlade.spec.state = bladeStatus.releaseRequested;
            reqBlade.spec.nextOwner = requestorID;

            addLogEvent("Blade " + requestorID + " requested blade " + bladeIP + "(success, requestor added to queue)");
            return new result(resultCode.pending, "Added to queue");
        }

#region async helpers
        public resultAndWaitToken logIn(string hostIP)
        {
            return doAsync(_currentlyRunningLogIns, hostIP, handleTypes.LGI, (e) => { logInBlocking(e); });
        }

        public resultAndWaitToken releaseBladeOrVM(string NodeIP, string requestorIP, bool force = false)
        {
            return doAsync(_currentlyRunningReleases, NodeIP, handleTypes.REL, (e) => { releaseBladeOrVMBlocking(e, NodeIP, requestorIP, force); });
        }

        private resultAndWaitToken doAsync(Dictionary<waitToken, inProgressOperation> currentlyRunning, string hostIP, handleTypes tokenPrefix, Action<inProgressOperation> taskCreator, Action<inProgressOperation> taskAfterStart = null)
        {
            lock (currentlyRunning)
            {
                waitToken waitToken = new waitToken(tokenPrefix, hostIP.GetHashCode().ToString());

                // If there's already a 'thing' going on for this host, just use that one. Don't do two simultaneously.
                if (currentlyRunning.ContainsKey(waitToken))
                {
                    if (!currentlyRunning[waitToken].isFinished)
                        return new resultAndWaitToken(resultCode.alreadyInProgress, currentlyRunning[waitToken].waitToken);
                    currentlyRunning.Remove(waitToken);
                }

                // Otherwise, make a new task and status, and start before we return.
                inProgressOperation newOperation = new inProgressOperation
                {
                    waitToken = waitToken,
                    hostIP = hostIP,
                    isFinished = false,
                    status = new result(resultCode.pending)
                };

                currentlyRunning.Add(waitToken, newOperation);

                Task loginTask = new Task(() => { taskCreator(newOperation); });
                loginTask.Start();

                if (taskAfterStart != null)
                    taskAfterStart.Invoke(newOperation);

                return new resultAndWaitToken(resultCode.pending, newOperation.waitToken);
            }
        }

        public resultAndWaitToken getProgress(waitToken waitToken)
        {
            Dictionary<waitToken, inProgressOperation> toOperateOn;
            switch (waitToken.handleType)
            {
                case handleTypes.LGI:
                    toOperateOn = _currentlyRunningLogIns;
                    break;
                case handleTypes.DEP:
                    return getProgressOfVMRequest(waitToken);
                case handleTypes.BOS:
                    return checkBIOSOperationProgress(waitToken);
                case handleTypes.SHT:
                    toOperateOn = _currentlyRunningSnapshotSets;
                    break;
                case handleTypes.REL:
                    toOperateOn = _currentlyRunningReleases;
                    break;
                default:
                    throw new ArgumentException("waitToken.handleType");
            }

            lock (toOperateOn)
            {
                if (toOperateOn.ContainsKey(waitToken))
                    return new resultAndWaitToken(toOperateOn[waitToken].status, waitToken);
                return new resultAndWaitToken(resultCode.bladeNotFound);
            }
        }
#endregion

        private void logInBlocking(inProgressOperation login)
        {
            try
            {
                _logInBlocking(login);
            }
            catch (Exception e)
            {
                lock (_currentlyRunningLogIns)
                {
                    login.status= new result(resultCode.genericFail, e.Message + "\n\n" + e.StackTrace);
                    login.isFinished = true;
                }
                throw;
            }
        }

        private void _logInBlocking(inProgressOperation login)
        {
            // Kill off VMs first, and then physical blades.
            // TODO: lock properly, so that no new VMs can be created before we destroy the physical blades

            // Destroy any VMs currently being deployed. Bear in mind that the deployment process may modify the ownership, and we
            // should be careful not to hold a write lock on ownership for a long time anyway since it will block PXE-script gen,
            // so we only do one VM at a time, allowing releaseVM to downgrade locks while it waits.
            while (true)
            {
                using (disposingList<lockableVMSpec> bootingVMs = db.getAllVMInfo(
                    x => (x.currentOwner == "vmserver" && x.nextOwner == login.hostIP), 
                    bladeLockType.lockOwnership, bladeLockType.lockOwnership))
                {
                    // 'Double lock' here, so that we don't hold the lock (on ownership) for other VMs while we wait for one to be 
                    // released. We release each lock and then lock/re-check on each individually.
                    foreach (lockableVMSpec allocated in bootingVMs)
                    {
                        // Release everything we hold, except read of IP address, since we need that one internally.
                        bladeLocks toRelease = allocated.getCurrentLocks();
                        toRelease.read &= ~bladeLockType.lockIPAddresses;
                        allocated.downgradeLocks(toRelease);
                    }

                    foreach (lockableVMSpec allocated in bootingVMs)
                    {
                        allocated.upgradeLocks(
                            bladeLockType.lockVMCreation | bladeLockType.lockBIOS | bladeLockType.lockSnapshot | bladeLockType.lockNASOperations | bladeLockType.lockvmDeployState | bladeLockType.lockVirtualHW | bladeLockType.lockOwnership,
                            bladeLockType.lockVMCreation | bladeLockType.lockBIOS | bladeLockType.lockSnapshot | bladeLockType.lockNASOperations | bladeLockType.lockvmDeployState);

                        // Check out double locking before we release.
                        if (allocated.spec.currentOwner == "vmserver" && 
                            allocated.spec.nextOwner == login.hostIP)
                        { 
                            releaseVM(allocated,
                                bladeLockType.lockVMCreation | bladeLockType.lockBIOS | bladeLockType.lockSnapshot | bladeLockType.lockNASOperations | bladeLockType.lockvmDeployState | bladeLockType.lockVirtualHW | bladeLockType.lockOwnership,
                                bladeLockType.lockVMCreation | bladeLockType.lockBIOS | bladeLockType.lockSnapshot | bladeLockType.lockNASOperations | bladeLockType.lockvmDeployState);
                        }
                    }

                    // If there are no VMs booting, we should move on to those that have finished boot.
                    // FIXME: there's a race here, what if a new VM is allocated before we break?
                    if (bootingVMs.Count == 0)
                    {
                        break;
                    }
                }
            }

            // And any VMs that have finished allocation.
            using (disposingList<lockableVMSpec> bootingVMs = db.getAllVMInfo(
                x => (x.currentOwner == login.hostIP),
                bladeLockType.lockOwnership, bladeLockType.lockOwnership))
            {
                foreach (lockableVMSpec allocated in bootingVMs)
                {
                    allocated.upgradeLocks(bladeLockType.lockVMCreation | bladeLockType.lockBIOS | bladeLockType.lockSnapshot | bladeLockType.lockNASOperations | bladeLockType.lockvmDeployState | bladeLockType.lockVirtualHW,
                        bladeLockType.lockVMCreation | bladeLockType.lockBIOS | bladeLockType.lockSnapshot | bladeLockType.lockNASOperations | bladeLockType.lockvmDeployState);
                    releaseVM(allocated,
                        bladeLockType.lockVMCreation | bladeLockType.lockBIOS | bladeLockType.lockSnapshot | bladeLockType.lockNASOperations | bladeLockType.lockvmDeployState | bladeLockType.lockVirtualHW,
                        bladeLockType.lockVMCreation | bladeLockType.lockBIOS | bladeLockType.lockSnapshot | bladeLockType.lockNASOperations | bladeLockType.lockvmDeployState);
                }
            }

            // Clean up any fully-allocated blades this blade has left over from any previous run
            using (disposingList<lockableBladeSpec> currentlyOwned = db.getAllBladeInfo(
                x => x.currentOwner == login.hostIP, bladeLockType.lockOwnership, bladeLockType.lockOwnership, true, true))
            {
                foreach (lockableBladeSpec allocated in currentlyOwned)
                {
                    allocated.upgradeLocks(bladeLockType.lockVMCreation | bladeLockType.lockBIOS | bladeLockType.lockSnapshot | bladeLockType.lockNASOperations | bladeLockType.lockvmDeployState | bladeLockType.lockVirtualHW,
                        bladeLockType.lockVMCreation | bladeLockType.lockBIOS | bladeLockType.lockSnapshot | bladeLockType.lockNASOperations | bladeLockType.lockvmDeployState);
                    releaseBlade(allocated);
                    allocated.downgradeLocks(bladeLockType.lockVMCreation | bladeLockType.lockBIOS | bladeLockType.lockSnapshot | bladeLockType.lockNASOperations | bladeLockType.lockvmDeployState | bladeLockType.lockVirtualHW,
                        bladeLockType.lockVMCreation | bladeLockType.lockBIOS | bladeLockType.lockSnapshot | bladeLockType.lockNASOperations | bladeLockType.lockvmDeployState);
                }
            }

            // And now report that the login is complete.
            lock (_currentlyRunningLogIns)
            {
                login.status.code = resultCode.success;
                login.isFinished = true;
            }
        }
        
        public void initWithBlades(string[] bladeIPs)
        {
            bladeSpec[] specs = new bladeSpec[bladeIPs.Length];
            int n = 0;
            foreach (string bladeIP in bladeIPs)
                specs[n++] = new bladeSpec(db.conn, bladeIP, n.ToString(), n.ToString(),
                    (ushort)n, false, VMDeployStatus.notBeingDeployed, "bioscontents", 
                    bladeLockType.lockAll, bladeLockType.lockAll);

            initWithBlades(specs);
        }

        public void initWithBlades(bladeSpec[] bladeSpecs)
        {
            db.initWithBlades(bladeSpecs);
        }

        private void releaseBladeOrVMBlocking(inProgressOperation blockingOperation, string NodeIP, string requestorIP, bool force = false)
        {
            result toRet = new result(resultCode.genericFail, null);

            if (db.getAllBladeIP().Contains(NodeIP))
            {
                toRet = releaseBlade(NodeIP, requestorIP, force);
            }
            else if (db.getAllVMIP().Contains(NodeIP))
            {
                toRet = releaseVM(NodeIP);
            }
            else
            {
                // Neither a blade nor a VM
                string msg = "Requestor " + requestorIP + " attempted to release blade " + NodeIP + " (blade not found)";
                addLogEvent(msg);
                toRet = new result(resultCode.bladeNotFound, msg);
            }

            lock (_currentlyRunningLogIns)
            {
                blockingOperation.status = toRet;
                blockingOperation.isFinished = true;
            }
        }

        private result releaseBlade(string reqBladeIP, string requestorIP, bool force)
        {
            // Lock with almost everything, but not the IP-addresses lock, since we don't change that. Note that we permit access
            // during BIOS or VM deployment here, since we check and wait for that in releaseBlade(lockableBladeSpec).
            using (lockableBladeSpec reqBlade = 
                db.getBladeByIP(reqBladeIP, 
                bladeLockType.lockVMCreation | bladeLockType.lockBIOS | bladeLockType.lockSnapshot | bladeLockType.lockNASOperations | bladeLockType.lockOwnership  | bladeLockType.lockvmDeployState, 
                bladeLockType.lockVMCreation | bladeLockType.lockBIOS | bladeLockType.lockSnapshot | bladeLockType.lockNASOperations | bladeLockType.lockOwnership  | bladeLockType.lockvmDeployState,
                permitAccessDuringBIOS: true, permitAccessDuringDeployment: true))
            {
                if (!force)
                {
                    if (reqBlade.spec.currentOwner != requestorIP)
                    {
                        string errMsg = "Requestor " + requestorIP + " attempted to release blade " + reqBlade.spec.bladeIP + " (failure: blade is not owned by requestor)";
                        addLogEvent(errMsg);
                        return new result(resultCode.bladeInUse, errMsg);
                    }
                }
                return releaseBlade(reqBlade);
            }
        }

        private result releaseBlade(lockableBladeSpec toRelease, bool omitChildVMs = false)
        {
            // Kill off any pending BIOS deployments ASAP.
            if (toRelease.spec.currentlyHavingBIOSDeployed)
            {
                toRelease.spec.currentlyHavingBIOSDeployed = false;
                biosRWEngine.cancelOperationsForBlade(toRelease.spec.bladeIP);

                result BIOSProgress = biosRWEngine.checkBIOSOperationProgress(toRelease.spec.bladeIP);
                while (BIOSProgress.code == resultCode.pending)
                {
                    addLogEvent("Waiting for blade " + toRelease.spec.bladeIP + " to cancel BIOS operation...");

                    // We need to drop these locks temporarily, since they are needed for the BIOS deploy to cancel.
                    toRelease.downgradeLocks(bladeLockType.lockBIOS | bladeLockType.lockvmDeployState, 
                        bladeLockType.lockBIOS | bladeLockType.lockvmDeployState);

                    Thread.Sleep(TimeSpan.FromSeconds(3));

                    toRelease.upgradeLocks(bladeLockType.lockBIOS | bladeLockType.lockvmDeployState, 
                        bladeLockType.lockBIOS | bladeLockType.lockvmDeployState);

                    BIOSProgress = biosRWEngine.checkBIOSOperationProgress(toRelease.spec.bladeIP);
                }
                // We don't even care if the cancelled operation fails instead of cancelling, at this point. As long as it's over
                // we're good.
            }

            // Reset any VM server the blade may be
            if (!omitChildVMs)
            {
                if (toRelease.spec.currentlyBeingAVMServer)
                {
                    toRelease.spec.currentlyBeingAVMServer = false;

                    // Is it currently being deployed?
                    if (toRelease.spec.vmDeployState != VMDeployStatus.notBeingDeployed &&
                        toRelease.spec.vmDeployState != VMDeployStatus.failed)
                    {
                        // Oh, it is being deployed. Cancel any deploys that reference this blade.
                        KeyValuePair<waitToken, VMThreadState>[] deployingChildVMs;
                        lock (_vmDeployState)
                        {
                            deployingChildVMs = _vmDeployState.Where(x => x.Value.vmServerIP == toRelease.spec.bladeIP).ToArray();

                            foreach (KeyValuePair<waitToken, VMThreadState> kvp in deployingChildVMs)
                                kvp.Value.deployDeadline = DateTime.MinValue;
                        }

                        foreach (KeyValuePair<waitToken, VMThreadState> childVM in deployingChildVMs)
                        {
                            while (true)
                            {
                                if (childVM.Value.currentProgress.result.code != resultCode.pending)
                                    break;
                                addLogEvent("Waiting for VM " + childVM.Value.childVMIP + " to cancel VM deployment operation...");
                                Thread.Sleep(TimeSpan.FromSeconds(3));
                            }
                        }
                    }

                    // OK, all deployments are finished. Release any VMs that succeeded.
                    List<vmSpec> childVMs = db.getVMByVMServerIP_nolocking(toRelease.spec.bladeIP);
                    foreach (vmSpec child in childVMs)
                    {
                        result res = releaseVM(child.VMIP);
                        if (res.code != resultCode.success)
                            return res;
                    }
                }
            }

            // If there's someone waiting, allocate it to that blade.
            if (toRelease.spec.state == bladeStatus.releaseRequested)
            {
                addLogEvent("Blade release : " + toRelease.spec.bladeIP + " (success, blade is now owned by queue entry " + toRelease.spec.nextOwner + ")");

                toRelease.spec.state = bladeStatus.inUse;
                toRelease.spec.currentOwner = toRelease.spec.nextOwner;
                toRelease.spec.nextOwner = null;
                toRelease.spec.lastKeepAlive = DateTime.Now;
            }
            else
            {
                // There's no-one waiting, so set it to idle.
                toRelease.spec.state = bladeStatus.unused;
                toRelease.spec.currentOwner = null;
                toRelease.spec.nextOwner = null;
                addLogEvent("Blade release : " + toRelease.spec.bladeIP + " (success, blade is now idle)");
            }

            return new result(resultCode.success);
        }

        private result releaseVM(string reqBladeIP)
        {
            using (lockableVMSpec lockedVM = db.getVMByIP(reqBladeIP,
                bladeLockType.lockOwnership | bladeLockType.lockSnapshot | bladeLockType.lockVirtualHW,
                bladeLockType.lockOwnership))
            {
                return releaseVM(lockedVM, 
                    bladeLockType.lockOwnership | bladeLockType.lockSnapshot | bladeLockType.lockVirtualHW,
                    bladeLockType.lockOwnership);
            }
        }

        private result releaseVM(lockableVMSpec lockedVM, bladeLockType additionalPrivsToDowngradeRead, bladeLockType additionalPrivsToDowngradeWrite)
        {
            // If we are currently being deployed, we must wait until we are at a point wherby we can abort the deploy. We need
            // to be careful how to lock here, otherwise we risk deadlocking.
            bool islocked = false;
            try
            {
                Monitor.Enter(_vmDeployState);
                islocked = true;

                waitToken waitToken = new waitToken(handleTypes.DEP, lockedVM.spec.VMIP.GetHashCode().ToString());

                if (_vmDeployState.ContainsKey(waitToken) &&
                    _vmDeployState[waitToken].currentProgress.result.code == resultCode.pending)
                {
                    // Ahh, this VM is currently being deployed. We can't release it until the thread doing the deployment says so.
                    _vmDeployState[waitToken].deployDeadline = DateTime.MinValue;
                    while (_vmDeployState[waitToken].currentProgress.result.code == resultCode.pending)
                    {
                        addLogEvent("Waiting for VM deploy on " + lockedVM.spec.VMIP + " to cancel");

                        Monitor.Exit(_vmDeployState);
                        islocked = false;
                        // Drop all locks on the VM (except IP address) while we wait for it to release. This will permit 
                        // allocation to finish.
                        bladeLocks locks = lockedVM.getCurrentLocks();
                        locks.read &= ~bladeLockType.lockIPAddresses;
                        lockedVM.downgradeLocks(locks);
                        Thread.Sleep(TimeSpan.FromSeconds(10));

                        lockedVM.upgradeLocks(locks.read, locks.write);
                        Monitor.Enter(_vmDeployState);
                        islocked = true;
                    }
                }
            }
            finally
            {
                if (islocked)
                    Monitor.Exit(_vmDeployState);
            }
            /*
            // VMs always get destroyed on release. First, though, power the relevant blade off on the hypervisor.
            try
            {
                using (lockableBladeSpec parentBlade = db.getBladeByIP(lockedVM.spec.parentBladeIP, bladeLockType.lockOwnership, bladeLockType.lockNone))
                {
                    using (hypervisor hyp = makeHypervisorForVM(lockedVM, parentBlade))
                    {
                        hyp.powerOff();
                    }
                }
            }
            catch (SocketException) { }
            catch (WebException) { }
            catch (VMNotFoundException) { } // ?!*/

            // Make a note of the VM server IP so we can use it later
            string vmserverip = lockedVM.spec.parentBladeIP;

            lockedVM.downgradeLocks(additionalPrivsToDowngradeRead, additionalPrivsToDowngradeWrite);

            // Now we dispose the VM, and then specify that the next release of the VM (ie, that done by the parent) should
            // be inhibited. This means the caller is left with a disposed, invalid, VM, but is able to call .Dispose (or leave a
            // using() {..} block) as normal.
            lockedVM.deleteOnRelease = true;
            lockedVM.Dispose();
            lockedVM.inhibitNextDisposal();

            // Now, if the VM server is empty, we can power it off. Otherwise, just power off the VM instead.
            vmserverTotals VMTotals = db.getVMServerTotalsByVMServerIP(vmserverip);
            if (VMTotals.VMs == 0)
            {
                using (lockableBladeSpec parentBlade = db.getBladeByIP(vmserverip,
                    bladeLockType.lockOwnership | bladeLockType.lockBIOS | bladeLockType.lockvmDeployState,
                    bladeLockType.lockOwnership | bladeLockType.lockBIOS))
                {
                    VMTotals = db.getVMServerTotalsByVMServerIP(vmserverip);

                    // double lock for performance.
                    if (VMTotals.VMs == 0)
                        return releaseBlade(parentBlade);
                }
            }
            
            return new result(resultCode.success);
        }

        private string getCurrentSnapshotForBladeOrVM(string nodeIp)
        {
            if (db.getAllBladeIP().Contains(nodeIp))
            {
                using (lockableBladeSpec reqBlade = db.getBladeByIP(nodeIp, bladeLockType.lockSnapshot, bladeLockType.lockNone))
                {
                    return String.Format("{0}-{1}", reqBlade.spec.bladeIP, reqBlade.spec.currentSnapshot);
                }
            }

            using (lockableVMSpec vmSpec = db.getVMByIP(nodeIp, bladeLockType.lockSnapshot | bladeLockType.lockOwnership, bladeLockType.lockNone))
            {
                return String.Format("{0}-{1}", vmSpec.spec.VMIP, vmSpec.spec.currentSnapshot);
            }
        }

        public resultAndBladeName RequestAnySingleVM(string requestorIP, VMHardwareSpec hwSpec, VMSoftwareSpec swReq )
        {
            if (hwSpec.memoryMB % 4 != 0)
            {
                // Fun fact: ESXi VM memory size must be a multiple of 4mb.
                string msg = "Failed VM alloc: memory size " + hwSpec.memoryMB + " is not a multiple of 4MB";
                addLogEvent(msg);
                return new resultAndBladeName(resultCode.genericFail, null, msg);
            }

            lock (_vmDeployState) // lock this since we're accessing the _vmDeployState variable
            {
                lockableBladeSpec freeVMServer = findAndLockBladeForNewVM(hwSpec);
                if (freeVMServer == null)
                    return new resultAndBladeName(resultCode.bladeQueueFull, null, "Cluster is full");
                using (freeVMServer)
                {
                    // Create rows for the child VM in the DB. Before we write it to the database, check we aren't about to add a 
                    // VM which conflicts with anything. If it does conflict, check the conflict is with an unused VM, and then 
                    // delete it before we add.
                    Thread worker;
                    waitToken waitToken;
                    using (lockableVMSpec childVM = freeVMServer.spec.createChildVM(db.conn, db, hwSpec, swReq, requestorIP))
                    {
                        waitToken = new waitToken(handleTypes.DEP, childVM.spec.VMIP.GetHashCode().ToString());

                        if (_vmDeployState.ContainsKey(waitToken))
                        {
                            if (_vmDeployState[waitToken].currentProgress.result.code == resultCode.pending )
                            {
                                // Oh, a deploy is already in progress for this VM. This should never happen, since .createChildVM
                                // should never return an already-existing VM.
                                return new resultAndBladeName(resultCode.bladeInUse, waitToken, "Newly-created blade is already being deployed");
                            }
                            _vmDeployState.Remove(waitToken);
                        }

                        // Now start a new thread, which will ensure the VM server is powered up and will then add the child VMs.
                        worker = new Thread(VMServerBootThread)
                        {
                            Name = "VMAllocationThread for VM " + childVM.spec.VMIP + " on server " + freeVMServer.spec.bladeIP
                        };
                        VMThreadState deployState = new VMThreadState
                        {
                            vmServerIP = freeVMServer.spec.bladeIP,
                            childVMIP = childVM.spec.VMIP,
                            deployDeadline = DateTime.Now + TimeSpan.FromMinutes(25),
                            currentProgress = new resultAndBladeName(resultCode.pending, waitToken, null)
                        };
                        _vmDeployState.Add(waitToken, deployState);
                    }
                    worker.Start(waitToken);
                    return new resultAndBladeName(resultCode.pending, waitToken, "Thread created");
                }
            }
        }
        
        private lockableBladeSpec findAndLockBladeForNewVM(VMHardwareSpec hwSpec)
        {
            lockableBladeSpec freeVMServer = null;
//            checkKeepAlives();
            // First, we need to find a blade to use as a VM server. Do we have a free VM server? If so, just use that.
            // We create a new bladeSpec to make sure that we don't double-release when the disposingList is released.
            using (disposingList<lockableBladeSpec> serverList = db.getAllBladeInfo( x => true,
                bladeLockType.lockVMCreation | bladeLockType.lockOwnership,
                bladeLockType.lockNone, true, true))
            {
                lockableBladeSpec[] freeVMServerList = serverList.Where(x => 
                    x.spec.currentlyBeingAVMServer &&       // must be a VM server
                    x.spec.canAccommodate(db, hwSpec)       // the blade must not be full
                    ).ToArray();

                if (freeVMServerList.Length != 0)
                {
                    // Great, the cluster is not full, and we cound at least one server which can accomodate our new VM.
                    // Just use the one we found. Again, we return this freeVMServer locked, so don't 'use (..' it here.
                    freeVMServer = freeVMServerList.First();
                    freeVMServer.inhibitNextDisposal();
                }
                else
                {
                    // Nope, no free VM server. Maybe we have free blades, and can can make a new one.
                    foreach (lockableBladeSpec spec in serverList)
                    {
                        spec.upgradeLocks(bladeLockType.lockvmDeployState | bladeLockType.lockBIOS, bladeLockType.lockNone);
                        if (spec.spec.currentOwner != null ||
                            spec.spec.currentlyBeingAVMServer ||
                            spec.spec.currentlyHavingBIOSDeployed)
                        {
                            spec.downgradeLocks(bladeLockType.lockvmDeployState | bladeLockType.lockBIOS, bladeLockType.lockNone);
                            continue;
                        }
                        spec.upgradeLocks(bladeLockType.lockNone, 
                            bladeLockType.lockvmDeployState | bladeLockType.lockBIOS | bladeLockType.lockOwnership);

                        try
                        {
                            result resp = requestBlade(spec, "vmserver");
                            if (resp.code == resultCode.success)
                            {
                                // Great, we allocated a new VM server. Note that we return this freeVMServer locked, so we don't 'use ..'
                                // it here.
                                freeVMServer = spec;
                                db.makeIntoAVMServer(freeVMServer);
                                freeVMServer.inhibitNextDisposal();
                                break;
                            }
                        }
                        finally
                        {
                            spec.downgradeLocks(bladeLockType.lockvmDeployState | bladeLockType.lockBIOS,
                                bladeLockType.lockvmDeployState | bladeLockType.lockBIOS | bladeLockType.lockOwnership);
                        }
                    }

                    if (freeVMServer == null)
                    {
                        // No free blades and no free VMs. The cluster cannot accomodate the VM we're being asked for, but maybe we
                        // can destroy some existing VMs to make room for it, if the sharing mode allows us to do so.
                        if (shareMode == resourceSharingMode.Proportional)
                        {
                            //db.
                        }
                        return null;
                    }
                }
            }
            return freeVMServer;
        }

        public resultAndWaitToken rebootAndStartDeployingBIOSToBlade(string requestorIp, string nodeIp, string biosxml, bool ignoreOwnership = false)
        {
            using (lockableBladeSpec reqBlade = db.getBladeByIP(nodeIp, bladeLockType.lockOwnership, bladeLockType.lockNone, false, true))
            {
                if (reqBlade.spec.currentOwner != "vmserver" && 
                    reqBlade.spec.currentOwner != requestorIp)
                    return new resultAndWaitToken(resultCode.bladeInUse);

                return doAsync(_currentBIOSOperations, nodeIp, handleTypes.BOS, (op) =>
                {
                    biosRWEngine.rebootAndStartWritingBIOSConfiguration(this, nodeIp, biosxml);
                }, (op) =>
                {
                    // Wait for the new thread to be spawned before we return. This prevents race conditions.
                    while (!biosRWEngine.hasOperationStarted(nodeIp))
                    {
                        Thread.Sleep(TimeSpan.FromMilliseconds(10));
                    }
                }
                );
            }
        }

        public resultAndWaitToken rebootAndStartReadingBIOSConfiguration(string nodeIp, string requestorIp)
        {
            using (lockableBladeSpec reqBlade = db.getBladeByIP(nodeIp, bladeLockType.lockOwnership, bladeLockType.lockNone))
            {
                if (reqBlade.spec.currentOwner != requestorIp)
                    return new resultAndBIOSConfig(new result(resultCode.bladeInUse, "This blade is not yours"), null);

                return doAsync(_currentBIOSOperations, nodeIp, handleTypes.BOS, (op) =>
                {
                    biosRWEngine.rebootAndStartReadingBIOSConfiguration(this, nodeIp);
                }, (op) =>
                {
                    // Wait for the new thread to be spawned before we return. This prevents race conditions.
                    while (!biosRWEngine.hasOperationStarted(nodeIp))
                    {
                        Thread.Sleep(TimeSpan.FromMilliseconds(10));
                    }
                }
                );
            }
        }

        private void VMServerBootThread(object param)
        {
            waitToken operationHandle = (waitToken) param;
            VMThreadState threadState;
            lock (_vmDeployState)
            {
                threadState = _vmDeployState[operationHandle];
            }

            vmSpec newVM = db.getVMByIP_withoutLocking(threadState.childVMIP);
            try
            {
                result res = _VMServerBootThread(threadState.vmServerIP, newVM, threadState);
                threadState.currentProgress.bladeName = threadState.childVMIP;
                threadState.currentProgress.result = res;
            }
            catch (Exception e)
            {
                string msg = "VMServer boot thread fatal exception: " + formatWithInner(e);
                addLogEvent(msg);
                
                using (lockableBladeSpec VMServer = db.getBladeByIP(threadState.vmServerIP,
//                    bladeLockType.lockvmDeployState | bladeLockType.lockOwnership,
bladeLockType.lockvmDeployState,  // <-- TODO/FIXME: write perms shuold imply read perms, but everything breaks if we don't specify a read lockOwnership here!
                    bladeLockType.lockvmDeployState | bladeLockType.lockOwnership, true, true))
                {
                }
                
                threadState.currentProgress.result = new result(resultCode.genericFail, msg);
            }
            finally
            {
                if (threadState.currentProgress.result.code == resultCode.pending)
                    threadState.currentProgress.result.code = resultCode.genericFail;
            }
        }

        private result _VMServerBootThread(string vmServerIP, vmSpec childVM_unsafe, VMThreadState threadState)
        {
            // First, bring up the physical machine. It'll get the ESXi ISCSI config and boot up.
            // Ensure only one thread tries to power on each VM server at once, by locking the physical blade. The first thread 
            // will get the responsilibility of observing the vmServerBootState and power on/off as neccessary, and then will set
            // the .vmServerBootState to waitingForPowerUp, and then finally to readyForDeployment.
            //
            // We are careful throughout this whole process not to lock either the newly-created VM nor the owning blade for any
            // long length of time. This is because the .loginblocking/.release code path needs to be able to lock both for write
            // to almost all of the fields. We carefully copy fields we need, and rely on the .getBladeByIP path denying access
            // while .VMDeployStatus is not notBeingDeployed.
            string VMServerBladeIPAddress;

            bool thisThreadWillPowerUp = false;
            using (lockableBladeSpec VMServer = db.getBladeByIP(vmServerIP,
                bladeLockType.lockvmDeployState, bladeLockType.lockvmDeployState, true, true))
            {
                if (VMServer.spec.vmDeployState == VMDeployStatus.needsPowerCycle)
                {
                    thisThreadWillPowerUp = true;
                    VMServer.spec.vmDeployState = VMDeployStatus.waitingForPowerUp;
                }
                VMServerBladeIPAddress = VMServer.spec.bladeIP;
            }
            if (thisThreadWillPowerUp)
            {
                try
                {
                    using (hypervisor hyp = makeHypervisorForBlade_ESXi(VMServerBladeIPAddress))
                    {
                        if (threadState.deployDeadline < DateTime.Now)
                            throw new TimeoutException();

                        hyp.connect();

                        // Write the correct BIOS to the blade, and block until this is complete
                        resultAndWaitToken res = rebootAndStartDeployingBIOSToBlade(null, VMServerBladeIPAddress, Resources.VMServerBIOS);
                        if (res.result.code != resultCode.pending)
                            return res.result;

                        resultAndBIOSConfig progress = checkBIOSOperationProgress(res.waitToken);
                        while ( progress.result.code == resultCode.pending || 
                                progress.result.code == resultCode.unknown   )
                        {
                            // If we timeout or are cancelled, tell the bios operation we are cancelling
                            if (threadState.deployDeadline < DateTime.Now)
                                biosRWEngine.cancelOperationsForBlade(VMServerBladeIPAddress);

                            // Otherwise, poll for operation finish
                            Thread.Sleep(TimeSpan.FromSeconds(3));
                            progress = checkBIOSOperationProgress(res.waitToken);
                        }
                        if (progress.result.code != resultCode.success &&
                            progress.result.code != resultCode.pending)
                        {
                            // The BIOS write has failed, or been cancelled.
                            // TODO: handle this correctly by marking the blade server as dead or whatever.
                            return progress.result;
                        }
                        hyp.powerOn(threadState.deployDeadline);

                        waitForESXiBootToComplete(hyp);

                        // Once it's powered up, we ensure the datastore is mounted okay. Sometimes I'm seeing ESXi hosts boot
                        // with an inaccessible NFS datastore, so remount if neccessary. Retry this since it doesn't seem to 
                        // always work first time.
                        _vmServerControl.mountDataStore(hyp, "esxivms", "store.xd.lan", "/mnt/SSDs/esxivms");
                    }
                }
                catch (Exception)
                {
                    using (lockableBladeSpec VMServer = db.getBladeByIP(vmServerIP,
                        bladeLockType.lockvmDeployState | bladeLockType.lockOwnership,
                        bladeLockType.lockvmDeployState | bladeLockType.lockOwnership, true, true))
                    {
                        VMServer.spec.vmDeployState = VMDeployStatus.failed;
                        VMServer.spec.currentlyBeingAVMServer = false;
                    }

                    throw;
                }

                using (lockableBladeSpec VMServer = db.getBladeByIP(vmServerIP,
                    bladeLockType.lockvmDeployState, bladeLockType.lockvmDeployState, true, true))
                {
                    VMServer.spec.vmDeployState = VMDeployStatus.readyForDeployment;
                }
            }
            else
            {
                // Wait for another thread to finish powering this blade on.
                while (true)
                {
                    using (lockableBladeSpec VMServer = db.getBladeByIP(vmServerIP,
                        bladeLockType.lockvmDeployState, bladeLockType.lockNone, true, true))
                    {
                        if (VMServer.spec.vmDeployState == VMDeployStatus.readyForDeployment)
                            break;
                        if (VMServer.spec.vmDeployState == VMDeployStatus.failed)
                            return new result(resultCode.cancelled, "Cancelled due to failure to configure on hardware blade");
                    }
                    Thread.Sleep(TimeSpan.FromSeconds(1));

                    if (threadState.deployDeadline < DateTime.Now)
                        throw new TimeoutException();
                }
            }

            // Anything we do after this point is fatal to the new VM, not to the VM server.
            try
            {
                using (hypervisor hyp = makeHypervisorForBlade_ESXi(vmServerIP))
                {
                    // now SSH to the blade and actually create the VM.
                    string destDir = "/vmfs/volumes/esxivms/" + VMServerBladeIPAddress + "_" + childVM_unsafe.vmConfigKey;
                    string destDirDatastoreType = "[esxivms] " + VMServerBladeIPAddress + "_" + childVM_unsafe.vmConfigKey;
                    string vmxPath = destDir + "/PXETemplate.vmx";

                    if (threadState.deployDeadline < DateTime.Now)
                        throw new TimeoutException();

                    // Remove the VM if it's already there. We don't mind if these commands fail - which they will, if the VM doesn't
                    // exist. We power off by directory and also by name, just in case a previous provision has left the VM hanging around.
                    string dstDatastoreDirEscaped = destDirDatastoreType.Replace("[", "\\[").Replace("]", "\\]");
                    hypervisor.doWithRetryOnSomeExceptions(() =>
                    {
                        throwIfTimedOut(threadState.deployDeadline);
                        hyp.startExecutable("vim-cmd", "vmsvc/power.off `vim-cmd vmsvc/getallvms | grep \"" + dstDatastoreDirEscaped + "\"`");
                    }, deadline: threadState.deployDeadline);
                    hypervisor.doWithRetryOnSomeExceptions(() => hyp.startExecutable("vim-cmd", "vmsvc/power.off `vim-cmd vmsvc/getallvms | grep \"" + childVM_unsafe.displayName + "\"`"), deadline: threadState.deployDeadline);
                    hypervisor.doWithRetryOnSomeExceptions(() => hyp.startExecutable("vim-cmd", "vmsvc/unregister `vim-cmd vmsvc/getallvms | grep \"" + dstDatastoreDirEscaped + "\"`"), deadline: threadState.deployDeadline);
                    hypervisor.doWithRetryOnSomeExceptions(() => hyp.startExecutable("vim-cmd", "vmsvc/unregister `vim-cmd vmsvc/getallvms | grep \"" + childVM_unsafe.displayName + "\"`"), deadline: threadState.deployDeadline);

                    // copy the template VM into a new directory
                    doCmdAndCheckSuccess(hyp, "rm", " -rf " + destDir, threadState.deployDeadline);
                    doCmdAndCheckSuccess(hyp, "cp", " -R /vmfs/volumes/esxivms/PXETemplate " + destDir, threadState.deployDeadline);
                    // and then customise it.
                    doCmdAndCheckSuccess(hyp, "sed", " -e 's/ethernet0.address[= ].*/ethernet0.address = \"" + childVM_unsafe.eth0MAC + "\"/g' -i " + vmxPath, threadState.deployDeadline);
                    doCmdAndCheckSuccess(hyp, "sed", " -e 's/ethernet1.address[= ].*/ethernet1.address = \"" + childVM_unsafe.eth1MAC + "\"/g' -i " + vmxPath, threadState.deployDeadline);
                    doCmdAndCheckSuccess(hyp, "sed", " -e 's/displayName[= ].*/displayName = \"" + childVM_unsafe.displayName + "\"/g' -i " + vmxPath, threadState.deployDeadline);
                    doCmdAndCheckSuccess(hyp, "sed", " -e 's/memSize[= ].*/memSize = \"" + childVM_unsafe.memoryMB + "\"/g' -i " + vmxPath, threadState.deployDeadline);
                    doCmdAndCheckSuccess(hyp, "sed", " -e 's/sched.mem.min[= ].*/sched.mem.min = \"" + childVM_unsafe.memoryMB + "\"/g' -i " + vmxPath, threadState.deployDeadline);
                    doCmdAndCheckSuccess(hyp, "sed", " -e 's/sched.mem.minSize[= ].*/sched.mem.minSize = \"" + childVM_unsafe.memoryMB + "\"/g' -i " + vmxPath, threadState.deployDeadline);
                    doCmdAndCheckSuccess(hyp, "sed", " -e 's/numvcpus[= ].*/numvcpus = \"" + childVM_unsafe.cpuCount + "\"/g' -i " + vmxPath, threadState.deployDeadline);
                    doCmdAndCheckSuccess(hyp, "sed", " -e 's/uuid.bios[= ].*//g' -i " + vmxPath, threadState.deployDeadline);
                    doCmdAndCheckSuccess(hyp, "sed", " -e 's/uuid.location[= ].*//g' -i " + vmxPath, threadState.deployDeadline);
                    // doCmdAndCheckSuccess(hyp, "sed", " -e 's/serial0.fileName[= ].*/" + "serial0.fileName = \"telnet://:" + (1000 + threadState.childVM.vmSpecID) + "\"/g' -i " + vmxPath), threadState.deployDeadline);

                    // Now add that VM to ESXi, and the VM is ready to use.
                    // We do this with a retry, because I'm seeing it fail occasionally >_<
                    hypervisor.doWithRetryOnSomeExceptions(() => doCmdAndCheckSuccess(hyp, "vim-cmd", " solo/registervm " + vmxPath, threadState.deployDeadline));
                }

                itemToAdd itm = childVM_unsafe.toItemToAdd(true);

                if (threadState.deployDeadline < DateTime.Now)
                    throw new TimeoutException();

                // Now we can select the new snapshot. We must be very careful and do this quickly, with the appropriate write lock
                // held, because it will block the ipxe script creation until we unlock.
                using (lockableVMSpec childVMFromDB = db.getVMByIP(childVM_unsafe.VMIP, bladeLockType.lockSnapshot, bladeLockType.lockSnapshot))
                {
                    childVMFromDB.spec.currentSnapshot = itm.snapshotName;
                }

                using (lockableBladeSpec bladeSpec = db.getBladeByIP(vmServerIP,
                    bladeLockType.lockNASOperations,
                    bladeLockType.lockNone, true, true))
                {
                    NASAccess nas = getNasForDevice(bladeSpec.spec);
                    createDisks.Program.deleteBlade(itm.cloneName, nas);

                    if (threadState.deployDeadline < DateTime.Now)
                        throw new TimeoutException();

                    // TODO: handle timeouts in makeHypervisorForVM

                    // Now create the disks, and customise the VM by naming it appropriately.
                    createDisks.Program.addBlades(nas, new[] {itm}, itm.snapshotName, _basePath, "bladebasestable-esxi", null, null,
                        (a, b) => {
                            return (hypervisorWithSpec<hypSpec_vmware>) makeHypervisorForVM(bladeSpec.spec.bladeIP, childVM_unsafe.VMIP, true, true);
                        }, threadState.deployDeadline);
                }
                // TODO: Ability to deploy transportDriver

                if (threadState.deployDeadline < DateTime.Now)
                    throw new TimeoutException();

                // All done. We can mark the blade as in use. Again, we are careful to hold write locks for as short a time as is
                // possible, to avoid blocking the PXE-script generation.
                using (lockableVMSpec childVMFromDB = db.getVMByIP(childVM_unsafe.VMIP,
                    bladeLockType.lockOwnership | bladeLockType.lockvmDeployState | bladeLockType.lockSnapshot,
                    bladeLockType.lockOwnership | bladeLockType.lockvmDeployState))
                {
                    childVMFromDB.spec.state = bladeStatus.inUse;
                    childVMFromDB.spec.currentOwner = childVMFromDB.spec.nextOwner;
                    childVMFromDB.spec.nextOwner = null;
                    childVMFromDB.spec.lastKeepAlive = DateTime.Now;
                }
                return new result(resultCode.success);
            }
            catch (Exception e)
            {
                // A failure during VM deployment! We will keep this VM, but mark it as unusable. The user can try to get another
                // or something.
                using (lockableVMSpec childVMFromDB = db.getVMByIP(childVM_unsafe.VMIP,
                    bladeLockType.lockOwnership | bladeLockType.lockvmDeployState,
                    bladeLockType.lockOwnership | bladeLockType.lockvmDeployState))
                {
                    childVMFromDB.spec.state = bladeStatus.unusable;
                    childVMFromDB.spec.currentOwner = null;
                    childVMFromDB.spec.nextOwner = null;
                    childVMFromDB.spec.lastKeepAlive = DateTime.Now;
                }
                return new result(resultCode.genericFail);
            }
        }

        private static void throwIfTimedOut(DateTime deployDeadline)
        {
            if (deployDeadline < DateTime.Now)
                throw new TimeoutException();
        }

        private object formatWithInner(Exception e)
        {
            string toRet = e.ToString();
            if (e.InnerException != null)
                toRet += formatWithInner(e.InnerException);
            return toRet;
        }

        private void doCmdAndCheckSuccess(hypervisor hyp, string cmd, string args, DateTime deadline)
        {
            executionResult res = hypervisor.doWithRetryOnSomeExceptions(() => hyp.startExecutable(cmd, args, deadline: deadline));
            if (res.resultCode != 0)
            {
                addLogEvent(string.Format("Command '{0}' with args '{1}' returned failure code {2}; stdout is '{3} and stderr is '{4}'", cmd, args, res.resultCode, res.stdout, res.stderr));
                throw new hypervisorExecutionException("failed to execute ssh command");
            }
        }

        public resultAndBladeName getProgressOfVMRequest(waitToken waitToken)
        {
            if (waitToken == null)
                return new resultAndBladeName(new result(resultCode.bladeNotFound, "No waitToken supplied"), null);
            
            if (!Monitor.TryEnter(_vmDeployState, TimeSpan.FromSeconds(15)))
            {
                return new resultAndBladeName(new result(resultCode.unknown, "unable to acquire lock on vmDeployState after 15 seconds"), waitToken);
            }
            try
            {
                lock (_vmDeployState)
                {
                    if (!_vmDeployState.ContainsKey(waitToken))
                        return new resultAndBladeName(new result(resultCode.bladeNotFound, "Blade not currently being deployed"), waitToken);

                    return _vmDeployState[waitToken].currentProgress;
                }
            }
            finally 
            {
                Monitor.Exit(_vmDeployState);
            }
        }
       
        public resultAndBladeName RequestAnySingleNode(string requestorIP)
        {
            resultAndBladeName toRet = null;

            // Put blades in an order of preference. First come unused blades, then used blades with an empty queue.
            using (disposingList<lockableBladeSpec> blades = db.getAllBladeInfo(
                //x => x.currentlyBeingAVMServer == false && x.currentlyHavingBIOSDeployed == false,
                x => true,
                bladeLockType.lockOwnership | bladeLockType.lockvmDeployState | bladeLockType.lockBIOS,
                bladeLockType.lockOwnership | bladeLockType.lockvmDeployState))
            {
                IEnumerable<lockableBladeSpec> unusedBlades = blades.Where(x => x.spec.currentOwner == null);
                IEnumerable<lockableBladeSpec> emptyQueueBlades = blades.Where(x => x.spec.currentOwner != null && x.spec.nextOwner == null);
                IEnumerable<lockableBladeSpec> orderedBlades = unusedBlades.Concat(emptyQueueBlades);

                foreach (lockableBladeSpec reqBlade in orderedBlades)
                {
                    result res = requestBlade(reqBlade, requestorIP);
                    // If this request succeded, great, we can just return it.
                    if (res.code == resultCode.success || res.code == resultCode.pending)
                    {
                        toRet = new resultAndBladeName(res) {bladeName = reqBlade.spec.bladeIP, result = res};
                        break;
                    }
                }

                if (toRet == null)
                {
                    // Otherwise, all blades have full queues.
                    return new resultAndBladeName(resultCode.bladeQueueFull, null, "All blades are full");
                }
            }

            checkFairness();
            return toRet;
        }
        
        private void checkFairness()
        {
            // If a blade owner is under its quota, then promote it in any queues where the current owner is over-quota.
            currentOwnerStat[] stats = db.getFairnessStats_withoutLocking();
            string[] owners = stats.Where(x => x.ownerName != "vmserver").Select(x => x.ownerName).ToArray();
            if (owners.Length == 0)
                return;
            int fairShare = db.getAllBladeIP().Length / owners.Length;

            currentOwnerStat[] ownersOverQuota = stats.Where(x => x.allocatedBlades > fairShare).ToArray();
            List<currentOwnerStat> ownersUnderQuota = stats.Where(x => x.allocatedBlades < fairShare).ToList();

            //foreach (var migrateFrom in ownersOverQuota)
            {
                foreach (var migrateTo in ownersUnderQuota)
                {
                    using (disposingList<lockableBladeSpec> migratory = db.getAllBladeInfo(x => 
                            (
                                // Migrate if the dest is currently owned by someone over-quota
                                (ownersOverQuota.Count(y => y.ownerName == x.currentOwner) > 0 ) ||
                                // Or if it is a VM server, and currently holds VMs that are _all_ allocated to over-quota users
                                ( 
                                    x.currentOwner == "vmserver" &&

                                    db.getVMByVMServerIP_nolocking(x.bladeIP).All(vm =>
                                        (ownersOverQuota.Count(overQuotaUser => overQuotaUser.ownerName == vm.currentOwner) > 0)
                                    )
                                )
                            )
                            &&
                            x.nextOwner == migrateTo.ownerName &&
                            (x.state == bladeStatus.inUse || x.state == bladeStatus.inUseByDirector),
                        bladeLockType.lockOwnership,
                        bladeLockType.lockOwnership,
                        true, true,
                        max: 1))
                    {
                        if (migratory.Count == 0)
                        {
                            // There is nowhere to migrate this owner from. Try another.
                            continue;
                        }

                        if (migratory[0].spec.currentlyBeingAVMServer)
                        {
                            // It's a VM server. Migrate all the VMs off it (ie, request them to be destroyed).
                            migratory[0].spec.nextOwner = migrateTo.ownerName;
                            using (disposingList<lockableVMSpec> childVMs = db.getVMByVMServerIP(migratory[0].spec.bladeIP))
                            {
                                foreach (lockableVMSpec VM in childVMs)
                                {
                                    Debug.WriteLine("Requesting release for VM " + VM.spec.VMIP);
                                    VM.spec.state = bladeStatus.releaseRequested;
                                }
                            }
                            migratory[0].spec.nextOwner = migrateTo.ownerName;
                            migratory[0].spec.state = bladeStatus.releaseRequested;
                        }
                        else
                        {
                            // It's a physical server. Just mark it as .releaseRequested.
                            Debug.WriteLine("Requesting release for blade " + migratory[0].spec.bladeIP);
                            migratory[0].spec.nextOwner = migrateTo.ownerName;
                            migratory[0].spec.state = bladeStatus.releaseRequested;
                        }
                    }
                }
            }
        }

        private void notifyBootDirectorOfNode(bladeSpec blade)
        {
            Uri wcfURI = new Uri("http://localhost/bootMenuController");
            BasicHttpBinding myBind = new BasicHttpBinding();

            using (bootMenuController.BootMenuWCFClient client = new bootMenuController.BootMenuWCFClient(myBind, new EndpointAddress(wcfURI)))
            {
                try
                {
                    client.addMachine(blade.iLOIP);
                }
                catch (EndpointNotFoundException)
                {
                    addLogEvent("Cannot find bootMenuController endpoint at " + wcfURI);
                }
            }
        }

        public void keepAlive(string requestorIP)
        {
            db.refreshKeepAliveForRequestor(requestorIP);
        }

        public bool isBladeMine(string nodeIp, string requestorIp, bool ignoreDeployments = false)
        {
            checkKeepAlives(requestorIp);

            using (var blade = db.getBladeByIP(nodeIp, bladeLockType.lockOwnership | bladeLockType.lockBIOS, bladeLockType.lockNone, true, true))
            {
                if (!ignoreDeployments)
                {
                    if (blade.spec.currentlyBeingAVMServer == true || blade.spec.currentlyHavingBIOSDeployed == true)
                        return false;
                }
                return blade.spec.currentOwner == requestorIp;
            }
        }

        public List<string> getLogEvents()
        {
            lock (_logEvents)
            {
                List<string> toRet = new List<string>(_logEvents);
                return toRet;
            }
        }

        public resultAndWaitToken selectSnapshotForBladeOrVM(string requestorIP, string bladeName, string newShot)
        {
            if (db.getAllBladeIP().Contains(bladeName))
            {
                return selectSnapshotForBlade(requestorIP, bladeName, newShot);
            }
            else if (db.getAllVMIP().Contains(bladeName))
            {
                return selectSnapshotForVM(requestorIP, bladeName, newShot);
            }
            else
            {
                return new resultAndWaitToken(resultCode.bladeNotFound);
            }
        }

        public resultAndWaitToken selectSnapshotForBlade(string requestorIP, string bladeName, string newShot)
        {
            return doAsync(_currentlyRunningSnapshotSets, bladeName, handleTypes.SHT,
                (e) =>
                {
                    using (var blade = db.getBladeByIP(bladeName,
                        bladeLockType.lockSnapshot | bladeLockType.lockNASOperations | bladeLockType.lockOwnership,
                        bladeLockType.lockSnapshot | bladeLockType.lockNASOperations))
                    {
                        try
                        {
                            if (blade.spec.currentOwner != requestorIP)
                            {
                                _currentlyRunningSnapshotSets[e.waitToken].status = new result(resultCode.bladeInUse, "Blade is not yours");
                                return;
                            }

                            blade.spec.currentSnapshot = newShot;
                            itemToAdd itm = blade.spec.toItemToAdd(false);
                            createDisks.Program.repairBladeDeviceNodes(new[] {itm});

                            _currentlyRunningSnapshotSets[e.waitToken].status = new result(resultCode.success);
                        }
                        catch (Exception excep)
                        {
                            _currentlyRunningSnapshotSets[e.waitToken].status = new result(resultCode.genericFail, excep.Message + " @ " + excep.StackTrace);
                        }
                        finally
                        {
                            _currentlyRunningSnapshotSets[e.waitToken].isFinished = true;
                        }
                    }
                });
        }

        public resultAndWaitToken selectSnapshotForVM(string requestorIP, string vmName, string newShot)
        {
            using (lockableVMSpec vm = db.getVMByIP(vmName, 
                bladeLockType.lockSnapshot | bladeLockType.lockOwnership, 
                bladeLockType.lockSnapshot))
            {
                if (vm.spec.currentOwner != requestorIP)
                    return new resultAndWaitToken(resultCode.bladeInUse, "VM is not yours");

                vm.spec.currentSnapshot = newShot;

                return new resultAndWaitToken(resultCode.success);
            }
        }

        private string getFreeNASSnapshotPath(string nodeIp)
        {
            if (db.getAllBladeIP().Contains(nodeIp))
            {
                using (lockableBladeSpec ownership = db.getBladeByIP(nodeIp,
                    bladeLockType.lockSnapshot | bladeLockType.lockOwnership,
                    bladeLockType.lockNone))
                {
                    return String.Format("{0}-{1}-{2}", nodeIp, ownership.spec.currentOwner, ownership.spec.currentSnapshot);
                }
            }

            using (lockableVMSpec ownership = db.getVMByIP(nodeIp,
                bladeLockType.lockSnapshot | bladeLockType.lockOwnership,
                bladeLockType.lockNone))
            {
                return String.Format("{0}-{1}-{2}", nodeIp, ownership.spec.currentOwner, ownership.spec.currentSnapshot);
            }
        }
        
        public void markLastKnownBIOS(lockableBladeSpec reqBlade, string biosxml)
        {
            lock (_currentBIOSOperations)
            {
                waitToken waitToken = new waitToken(handleTypes.BOS, reqBlade.spec.bladeIP.GetHashCode().ToString());
                _currentBIOSOperations[waitToken].isFinished = true;
            }

            reqBlade.spec.currentlyHavingBIOSDeployed = false;
            reqBlade.spec.lastDeployedBIOS = biosxml;
        }

        private resultAndBIOSConfig checkBIOSOperationProgress(waitToken waitToken)
        {
            lock (_currentBIOSOperations)
            {
                if (!_currentBIOSOperations.ContainsKey(waitToken))
                    return new resultAndBIOSConfig(new result(resultCode.bladeNotFound, "This blade is not being deployed right now"), waitToken);

                string hostIP = _currentBIOSOperations[waitToken].hostIP;

                result status = biosRWEngine.checkBIOSOperationProgress(hostIP);

                if (status.code == resultCode.success)
                {
                    using (lockableBladeSpec blade = db.getBladeByIP(hostIP, 
                        bladeLockType.lockBIOS, 
                        bladeLockType.lockNone, 
                        permitAccessDuringDeployment: true, // BIOS deploys can happen during the VM deployment process
                        permitAccessDuringBIOS: true))
                    {
                        result res = new result(resultCode.success, "Read BIOS OK");
                        return new resultAndBIOSConfig(res, null, blade.spec.lastDeployedBIOS);
                    }
                }

                return new resultAndBIOSConfig(status, waitToken);
            }
        }

        private void checkKeepAlives(string forOwnerIP)
        {
            using (disposingList<lockableBladeSpec> bladeInfo = db.getAllBladeInfo(
                x => x.currentOwner == forOwnerIP, 
                bladeLockType.lockOwnership | bladeLockType.lockBIOS,
                bladeLockType.lockOwnership, true, true))
            {
                foreach (lockableBladeSpec blade in bladeInfo)
                    checkKeepAlives(blade);
            }
        }

        private void checkKeepAlives(lockableBladeSpec reqBlade)
        {
            if (reqBlade.spec.state == bladeStatus.unused)
                return;

            if (reqBlade.spec.state == bladeStatus.inUseByDirector)
            {
                if (reqBlade.spec.currentlyHavingBIOSDeployed)
                {
                    // This situation can never timeout. Maybe it's a good idea if it does, but it can't for now.
                }
                else if (reqBlade.spec.currentlyBeingAVMServer)
                {
                    // If all VMs attached to this VM server are unused then we can destroy it.
                    List<vmSpec> childVMs = db.getVMByVMServerIP_nolocking(reqBlade.spec.bladeIP);
                    foreach (vmSpec childVM in childVMs)
                    {
                        // VMs are slightly different to blades, since they are always implicitly owned and destroyed on 
                        // release. This makes them much easier to check.
                        if (childVM.lastKeepAlive + keepAliveTimeout < DateTime.Now)
                        {
                            if (childVM.state != bladeStatus.inUseByDirector) // This will be true during deployment.
                            {
                                // Oh no, the blade owner failed to send a keepalive in time! Release it.
                                releaseBladeOrVM(childVM.VMIP, childVM.currentOwner);
                            }
                        }
                    }

                    childVMs = db.getVMByVMServerIP_nolocking(reqBlade.spec.bladeIP);
                    if (childVMs.Count == 0)
                    {
                        using (disposingList<lockableVMSpec> childVMsLocked = db.getVMByVMServerIP(reqBlade.spec.bladeIP))
                        {
                            if (childVMsLocked.Count == 0)
                            {
                                reqBlade.upgradeLocks(bladeLockType.lockNone, bladeLockType.lockBIOS);
                                releaseBlade(reqBlade);
                                reqBlade.upgradeLocks(bladeLockType.lockNone, bladeLockType.lockNone);
                            }
                        }
                    }
                }
            }
            else
            {
                if (reqBlade.spec.lastKeepAlive + keepAliveTimeout < DateTime.Now)
                {
                    // Oh no, the blade owner failed to send a keepalive in time! Release it.
                    addLogEvent("Requestor " + reqBlade.spec.currentOwner + " failed to keepalive for " + reqBlade.spec.bladeIP + ", releasing blade");
                    releaseBlade(reqBlade);
                }
            }
        }

        public string[] getBladesByAllocatedServer(string requestorIP)
        {
            checkKeepAlives(requestorIP);
            return db.getBladesByAllocatedServer(requestorIP);
        }

        public GetBladeStatusResult getBladeStatus(string requestorIp, string nodeIp)
        {
            checkKeepAlives(requestorIp);
            return db.getBladeStatus(nodeIp, requestorIp);
        }

        public GetBladeStatusResult getVMStatus(string requestorIp, string nodeIp)
        {
            checkKeepAlives(requestorIp);
            return db.getVMStatus(nodeIp, requestorIp);
        }

        public string generateIPXEScript(string srcIP)
        {
            try
            {
                return _generateIPXEScript(srcIP);
            }
            catch (bladeNotFoundException)
            {
                return "#!ipxe\r\n" +
                       "prompt Cannot find blade at this IP address. Hit [enter] to reboot.\r\n" +
                       "reboot\r\n";
            }
        }

        private string _generateIPXEScript(string srcIP)
        {
            // First off, are we working on a VM or a physical host?
            bool isPhysicalBlade = db.getAllBladeIP().Contains(srcIP);

            if (isPhysicalBlade)
            {
                using (lockableBladeSpec sourceBlade = db.getBladeByIP(srcIP, 
                    bladeLockType.lockOwnership | bladeLockType.lockBIOS | bladeLockType.lockSnapshot, 
                    bladeLockType.lockNone,
                    permitAccessDuringBIOS: true, permitAccessDuringDeployment: true))
                {
                    return sourceBlade.spec.generateIPXEScript();
                }
            }
            else
            {
                using (lockableVMSpec vmState = db.getVMByIP(srcIP, bladeLockType.lockOwnership | bladeLockType.lockSnapshot, bladeLockType.lockNone))
                {
                    if (vmState == null)
                        return "No blade at this IP address";
                    return vmState.spec.generateIPXEScript();
                }
            }
        }

        public string[] getAllBladeIP()
        {
            return db.getAllBladeIP();
        }

        public bladeSpec getBladeByIP_withoutLocking(string bladeIP)
        {
            return db.getBladeByIP_withoutLocking(bladeIP);
        }

        public vmSpec getVMByIP_withoutLocking(string vmip)
        {
            return db.getVMByIP_withoutLocking(vmip);
        }

        public resultCode addNode(string newIP, string newISCSIIP, string newILOIP, ushort newDebugPort)
        {
            bladeSpec newSpec = new bladeSpec(db.conn, newIP, newISCSIIP, newILOIP, newDebugPort, false, VMDeployStatus.notBeingDeployed, null, bladeLockType.lockAll, bladeLockType.lockAll);
            db.addNode(newSpec);
            return resultCode.success;
        }

        public vmSpec[] getVMByVMServerIP_nolocking(string bladeIP)
        {
            return db.getVMByVMServerIP_nolocking(bladeIP).ToArray();
        }

        public vmServerCredentials _getCredentialsForVMServerByVMIP(string vmip)
        {
            // All VMs have the same credentials for now.
            return new vmServerCredentials()
            {
                username = Properties.Settings.Default.esxiUsername,
                password = Properties.Settings.Default.esxiPassword
            };
        }

        public snapshotDetails _getCurrentSnapshotDetails(string hostIP)
        {
            return  new snapshotDetails()
            {
                friendlyName = getCurrentSnapshotForBladeOrVM(hostIP), 
                path = getFreeNASSnapshotPath(hostIP)
            };
        }

        public string getWebSvcURL(string srcIP)
        {
            string toRet = _ipxeUrl;

            if (toRet == null)
                return "(none)";

            if (toRet.Contains("0.0.0.0"))
            {
                string clientIP = ipUtils.getBestRouteTo(IPAddress.Parse(srcIP)).ToString();
                toRet = toRet.Replace("0.0.0.0", clientIP);
            }

            return toRet;
        }

        public void setWebSvcURL(string newURL)
        {
            _ipxeUrl = newURL;
        }

        public resultAndBladeName[] requestAsManyVMAsPossible(string requestorIP, VMHardwareSpec hwSpec, VMSoftwareSpec swSpec)
        {
            List<resultAndBladeName> toRet = new List<resultAndBladeName>();

            while(true)
            {
                resultAndBladeName res = RequestAnySingleVM(requestorIP, hwSpec, swSpec);

                if (res.result.code == resultCode.pending)
                {
                    toRet.Add(res);
                    continue;
                }
                else
                {
                    break;
                }
            }

            return toRet.ToArray();
        }
    }
}