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

        private Dictionary<string, VMThreadState> _VMDeployState = new Dictionary<string, VMThreadState>();

        private Dictionary<string, inProgressOperation> _currentlyRunningLogIns = new Dictionary<string, inProgressOperation>();
        private Dictionary<string, inProgressOperation> _currentlyRunningReleases = new Dictionary<string, inProgressOperation>();
        private Dictionary<string, inProgressOperation> _currentBIOSOperations = new Dictionary<string, inProgressOperation>();

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

        protected hostStateManager_core(string basePath, vmServerControl newVmServerControl, IBiosReadWrite newBiosReadWrite)
        {
            _basePath = basePath;
            _vmServerControl = newVmServerControl;
            biosRWEngine = newBiosReadWrite;

            db = new hostDB(basePath);
        }

        /// <summary>
        /// Init the hoststateDB with an in-memory database
        /// </summary>
        protected hostStateManager_core(vmServerControl newVmServerControl, IBiosReadWrite newBiosReadWrite)
        {
            _vmServerControl = newVmServerControl;
            biosRWEngine = newBiosReadWrite;
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
            lock (_logEvents)
            {
                _logEvents.Add(DateTime.Now + " : " + newEntry);
            }
        }

        public hypervisor makeHypervisorForVM(string bladeIP, string VMIP)
        {
            using(var blade = db.getBladeByIP(bladeIP, bladeLockType.lockNone, bladeLockType.lockNone))
            {
                using (lockableVMSpec VM = db.getVMByIP(VMIP, bladeLockType.lockNone, bladeLockType.lockNone))
                {
                    return makeHypervisorForVM(VM, blade);
                }
            }
        }

        public hypervisor makeHypervisorForBlade_ESXi(string bladeIP)
        {
            using (var blade = db.getBladeByIP(bladeIP, bladeLockType.lockSnapshot, bladeLockType.lockNone))
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

            // It's all okay, so request the release.
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

        private resultAndWaitToken doAsync(Dictionary<string, inProgressOperation> currentlyRunning, string hostIP, handleTypes tokenPrefix, Action<inProgressOperation> taskCreator, Action<inProgressOperation> taskAfterStart = null)
        {
            lock (currentlyRunning)
            {
                string waitToken = tokenPrefix + "_" + hostIP.GetHashCode().ToString();

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

        public resultAndWaitToken getProgress(string waitToken)
        {
            string tokenPrefix = waitToken.Split('_')[0];
            handleTypes handleType;
            if (!Enum.TryParse(tokenPrefix, true, out handleType))
                return new resultAndWaitToken(resultCode.bladeNotFound, "Invalid token prefix of " + tokenPrefix);

            Dictionary<string, inProgressOperation> toOperateOn;
            switch (handleType)
            {
                case handleTypes.LGI:
                    toOperateOn = _currentlyRunningLogIns;
                    break;
                case handleTypes.DEP:
                    return getProgressOfVMRequest(waitToken);
                case handleTypes.BOS:
                    return checkBIOSOperationProgress(waitToken);
                case handleTypes.REL:
                    toOperateOn = _currentlyRunningReleases;
                    break;
                default:
                    return new resultAndWaitToken(resultCode.bladeNotFound, "Invalid token prefix of " + tokenPrefix);
            }

            lock (toOperateOn)
            {
                if (toOperateOn.ContainsKey(waitToken))
                    return new resultAndWaitToken(toOperateOn[waitToken].status, waitToken);
                return new resultAndWaitToken(resultCode.bladeNotFound, "");
            }
        }
#endregion

        private void logInBlocking(inProgressOperation login)
        {
            // Lock with almost everything, but not the IP-addresses lock, since we don't change that. 
            using (disposingListOfBladesAndVMs currentlyOwned = db.getBladesAndVMs(
                x =>  x.currentOwner == login.hostIP,
                x => (x.currentOwner == "vmserver" && x.nextOwner == login.hostIP) |  x.currentOwner == login.hostIP ,
                bladeLockType.lockVMCreation | bladeLockType.lockBIOS | bladeLockType.lockSnapshot | bladeLockType.lockNASOperations | bladeLockType.lockOwnership | bladeLockType.lockVMDeployState | bladeLockType.lockVirtualHW,
                bladeLockType.lockVMCreation | bladeLockType.lockBIOS | bladeLockType.lockSnapshot | bladeLockType.lockNASOperations | bladeLockType.lockOwnership | bladeLockType.lockVMDeployState))
            {
                // Lock all hosts that are either owner by this owner, or that we are preparing for this owner.
                IEnumerable<lockableVMSpec> bootingVMs = currentlyOwned.VMs.Where(x => x.spec.currentOwner == "vmserver");
                IEnumerable<lockableVMSpec> allocedVMs = currentlyOwned.VMs.Where(x => x.spec.currentOwner == login.hostIP);

                // Clean up anything that we are currently preparing for this owner
                foreach (lockableVMSpec allocated in bootingVMs)
                    releaseVM(allocated);

                // Clean up any hosts this blade has left over from any previous run
                foreach (lockableBladeSpec allocated in currentlyOwned.blades)
                    releaseBlade(allocated);

                // Clean up any VMs that have finished allocation
                foreach (lockableVMSpec allocated in allocedVMs)
                    releaseVM(allocated);

                // And now report that the login is complete.
                lock (_currentlyRunningLogIns)
                {
                    login.status.code = resultCode.success;
                    login.isFinished = true;
                }
            }
        }
        
        public void initWithBlades(string[] bladeIPs)
        {
            bladeSpec[] specs = new bladeSpec[bladeIPs.Length];
            int n = 0;
            foreach (string bladeIP in bladeIPs)
                specs[n++] = new bladeSpec(db.conn, bladeIP, n.ToString(), n.ToString(), (ushort)n, false, VMDeployStatus.needsPowerCycle, "bioscontents", bladeLockType.lockAll, bladeLockType.lockAll);

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
            // Lock with almost everything, but not the IP-addresses lock, since we don't change that. 
            using (lockableBladeSpec reqBlade = 
                db.getBladeByIP(reqBladeIP, 
                bladeLockType.lockVMCreation | bladeLockType.lockBIOS | bladeLockType.lockSnapshot | bladeLockType.lockNASOperations | bladeLockType.lockOwnership  | bladeLockType.lockVMDeployState, 
                bladeLockType.lockVMCreation | bladeLockType.lockBIOS | bladeLockType.lockSnapshot | bladeLockType.lockNASOperations | bladeLockType.lockOwnership  | bladeLockType.lockVMDeployState))
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

        private result releaseBlade(lockableBladeSpec toRelease)
        {
            // Kill off any pending BIOS deployments ASAP.
            if (toRelease.spec.currentlyHavingBIOSDeployed)
            {
                toRelease.spec.currentlyHavingBIOSDeployed = false;
                biosRWEngine.cancelOperationsForBlade(toRelease.spec.bladeIP);

                result BIOSProgress = biosRWEngine.checkBIOSOperationProgress(toRelease.spec.bladeIP);
                while (BIOSProgress.code == resultCode.pending)
                {
                    Debug.WriteLine("Waiting for blade " + toRelease.spec.bladeIP + " to cancel BIOS operation...");
                    Thread.Sleep(TimeSpan.FromSeconds(3));
                    BIOSProgress = biosRWEngine.checkBIOSOperationProgress(toRelease.spec.bladeIP);
                }
                // We don't even care if the cancelled operation fails instead of cancelling, at this point. As long as it's over
                // we're good.
            }

            // Reset any VM server the blade may be
            if (toRelease.spec.currentlyBeingAVMServer)
            {
                // TODO: cancel VM deployment process

                toRelease.spec.currentlyBeingAVMServer = false;
                List<vmSpec> childVMs = db.getVMByVMServerIP_nolocking(toRelease.spec.bladeIP);
                foreach (vmSpec child in childVMs)
                {
                    result res = releaseVM(child.VMIP);
                    if (res.code != resultCode.success)
                        return res;
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
                bladeLockType.lockOwnership | bladeLockType.lockIPAddresses | bladeLockType.lockSnapshot | bladeLockType.lockVirtualHW,
                bladeLockType.lockOwnership))
            {
                return releaseVM(lockedVM);
            }
        }

        private result releaseVM(lockableVMSpec lockedVM)
        {
            // If we are currently being deployed, we must wait until we are at a point wherby we can abort the deploy. We need
            // to be careful how to lock here, otherwise we risk deadlocking.
            bool islocked = false;
            try
            {
                Monitor.Enter(_VMDeployState);
                islocked = true;

                string waitToken = handleTypes.REL + "_" + lockedVM.spec.VMIP.GetHashCode();

                if (_VMDeployState.ContainsKey(waitToken) &&
                    _VMDeployState[waitToken].currentProgress.result.code != resultCode.pending)
                {
                    // Ahh, this VM is currently being deployed. We can't release it until the thread doing the deployment says so.
                    _VMDeployState[waitToken].deployDeadline = DateTime.MinValue;
                    while (_VMDeployState[waitToken].currentProgress.result.code == resultCode.pending)
                    {
                        _logEvents.Add("Waiting for VM deploy on " + lockedVM.spec.VMIP + " to cancel");

                        Monitor.Exit(_VMDeployState);
                        islocked = false;
                        Thread.Sleep(TimeSpan.FromSeconds(10));

                        Monitor.Enter(_VMDeployState);
                        islocked = true;
                    }
                }
            }
            finally
            {
                if (islocked)
                    Monitor.Exit(_VMDeployState);
            }

            // VMs always get destroyed on release. First, though, power the relevant VM off on the hypervisor.
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
            catch (VMNotFoundException) { } // ?!

            // Now we dispose the blade, and then specify that the next release of the blade (ie, that done by the parent) should
            // be inhibited. This means the caller is left with a disposed, invalid, VM, but is able to call .Dispose (or leave a
            // using() {..} block) as normal.
            lockedVM.deleteOnRelease = true;
            lockedVM.Dispose();
            lockedVM.inhibitNextDisposal();

            // Now, if the VM server is empty, we can power it off. Otherwise, just power off the VM instead.
            vmserverTotals VMTotals = db.getVMServerTotalsByVMServerIP(lockedVM.spec.parentBladeIP);
            {
                if (VMTotals.VMs == 0)
                {
                    using (lockableBladeSpec parentBlade = db.getBladeByIP(lockedVM.spec.parentBladeIP, 
                        bladeLockType.lockOwnership | bladeLockType.lockBIOS,
                        bladeLockType.lockOwnership | bladeLockType.lockBIOS))
                    {
                        return releaseBlade(parentBlade);
                    }
                }
            }
            return new result(resultCode.success);
        }

        public string getCurrentSnapshotForBladeOrVM(string nodeIp)
        {
            using (var reqBlade = db.getBladeByIP(nodeIp, bladeLockType.lockSnapshot, bladeLockType.lockNone))
            {
                if (reqBlade.spec != null)
                    return String.Format("{0}-{1}", reqBlade.spec.bladeIP, reqBlade.spec.currentSnapshot);
            }

            using (lockableVMSpec vmSpec = db.getVMByIP(nodeIp, bladeLockType.lockSnapshot | bladeLockType.lockOwnership, bladeLockType.lockNone))
            {
                if (vmSpec.spec != null)
                {
                    return String.Format("{0}-{1}", vmSpec.spec.VMIP, vmSpec.spec.currentSnapshot);
                }
            }

            return null;
        }

        public resultAndBladeName RequestAnySingleVM(string requestorIP, VMHardwareSpec hwSpec, VMSoftwareSpec swReq )
        {
            if (hwSpec.memoryMB % 4 != 0)
            {
                // Fun fact: ESXi VM memory size must be a multiple of 4mb.
                string msg = "Failed VM alloc: memory size " + hwSpec.memoryMB + " is not a multiple of 4MB";
                _logEvents.Add(msg);
                return new resultAndBladeName(resultCode.genericFail, null, msg);
            }

            lock (_VMDeployState) // lock this since we're accessing the _VMDeployState variable
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
                    string waitToken;
                    using (lockableVMSpec childVM = freeVMServer.spec.createChildVM(db.conn, db, hwSpec, swReq, requestorIP))
                    {
                        waitToken = handleTypes.DEP + "_" + childVM.spec.VMIP.GetHashCode();

                        if (_VMDeployState.ContainsKey(waitToken))
                        {
                            if (_VMDeployState[waitToken].currentProgress.result.code == resultCode.pending )
                            {
                                // Oh, a deploy is already in progress for this VM. This should never happen, since .createChildVM
                                // should never return an already-existing VM.
                                return new resultAndBladeName(resultCode.bladeInUse, waitToken, "Newly-created blade is already being deployed");
                            }
                            _VMDeployState.Remove(waitToken);
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
                        _VMDeployState.Add(waitToken, deployState);
                        // We must dispose of our childVM before we start the worker thread, to ensure it is flushed to the DB.
                        // FIXME/TODO: how do we ensure that no-one else will use the child VM before our thread claims it?
                        // ^^^ by downgrading and holding the lock?
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
            using (disposingList<lockableBladeSpec> serverList = db.getAllBladeInfo(x => true,
                bladeLockType.lockOwnership | bladeLockType.lockVMDeployState | bladeLockType.lockVMCreation,
                bladeLockType.lockOwnership | bladeLockType.lockVMDeployState | bladeLockType.lockVMCreation 
                ))
            {
                lockableBladeSpec[] freeVMServerList = serverList.Where(x => 
                    x.spec.currentlyBeingAVMServer && x.spec.canAccommodate(db, hwSpec)).ToArray();

                if (freeVMServerList.Length != 0)
                {
                    // Just use the one we found. Again, we return this freeVMServer locked, so don't 'use (..' it here.
                    freeVMServer = freeVMServerList.First();
                    freeVMServer.inhibitNextDisposal();
                }
                else
                {
                    // Nope, no free VM server. Maybe we can make a new one.
                    IEnumerable<lockableBladeSpec> freeNodes = serverList.Where(x => x.spec.currentOwner == null);
                    foreach (lockableBladeSpec freeNode in freeNodes)
                    {
                        result resp = requestBlade(freeNode, "vmserver");
                        if (resp.code == resultCode.success)
                        {
                            // Great, we allocated a new VM server. Note that we return this freeVMServer locked, so we don't 'use ..'
                            // it here.
                            freeVMServer = freeNode;
                            db.makeIntoAVMServer(freeVMServer);
                            freeVMServer.inhibitNextDisposal();
                            break;
                        }
                    }

                    if (freeVMServer == null)
                    {
                        // No blades were found - the cluster is full.
                        return null;
                    }
                }
            }
            return freeVMServer;
        }
        
        public resultAndWaitToken rebootAndStartDeployingBIOSToBlade(string requestorIp, string nodeIp, string biosxml)
        {
            using (lockableBladeSpec reqBlade = db.getBladeByIP(nodeIp, bladeLockType.lockOwnership, bladeLockType.lockNone))
            {
                if (reqBlade.spec.currentOwner != requestorIp)
                    return new resultAndWaitToken(resultCode.bladeInUse, "");

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
            string operationHandle = (string)param;
            VMThreadState threadState;
            lock (_VMDeployState)
            {
                threadState = _VMDeployState[operationHandle];
            }

            using (lockableVMSpec newVM = db.getVMByIP(threadState.childVMIP,
                bladeLockType.lockOwnership | bladeLockType.lockIPAddresses | bladeLockType.lockSnapshot | bladeLockType.lockVirtualHW,
                bladeLockType.lockNone))
            {
                try
                {
                    result res = _VMServerBootThread(threadState.vmServerIP, newVM, threadState.deployDeadline);
                    threadState.currentProgress.bladeName = threadState.childVMIP;
                    threadState.currentProgress.result = res;
                }
                catch (Exception e)
                {
                    string msg = "VMServer boot thread fatal exception: " + e.Message + " at " + e.StackTrace;
                    _logEvents.Add(msg);
                    threadState.currentProgress.result = new result(resultCode.genericFail, msg);
                }
            }
        }

        private result _VMServerBootThread(string vmServerIP, lockableVMSpec childVM, DateTime deployDeadline)
        {
            // First, bring up the physical machine. It'll get the ESXi ISCSI config and boot up.
            // Ensure only one thread tries to power on each VM server at once, by locking the physical blade. The first thread 
            // can get the responsilibility of observing the vmServerBootState and power on/off as neccessary.
            long VMServerBladeID;
            using (lockableBladeSpec VMServer = db.getBladeByIP(vmServerIP, 
                bladeLockType.lockVMDeployState | bladeLockType.lockSnapshot,
                bladeLockType.lockVMDeployState ))
            {
                if (VMServer.spec.VMDeployState == VMDeployStatus.needsPowerCycle)
                {
                    using (hypervisor hyp = makeHypervisorForBlade_ESXi(VMServer))
                    {
                        if (deployDeadline < DateTime.Now)
                            throw new TimeoutException();

                        hyp.connect();

                        // Write the correct BIOS to the blade, and block until this is complete
                        result res = biosRWEngine.rebootAndStartWritingBIOSConfiguration(this, VMServer.spec.bladeIP, Resources.VMServerBIOS);
                        if (res.code != resultCode.pending)
                            return res;

                        result progress = biosRWEngine.checkBIOSOperationProgress(VMServer.spec.bladeIP);
                        while (progress.code == resultCode.pending || progress.code == resultCode.unknown)
                        {
                            Thread.Sleep(TimeSpan.FromSeconds(3));
                            progress = biosRWEngine.checkBIOSOperationProgress(VMServer.spec.bladeIP);
                        }
                        if (progress.code != resultCode.success && progress.code != resultCode.pending)
                            return progress;
                        hyp.powerOn(deployDeadline);

                        waitForESXiBootToComplete(hyp);

                        // Once it's powered up, we ensure the datastore is mounted okay. Sometimes I'm seeing ESXi hosts boot
                        // with an inaccessible NFS datastore, so remount if neccessary. Retry this since it doesn't seem to 
                        // always work first time.
                        _vmServerControl.mountDataStore(hyp, "esxivms", "store.xd.lan", "/mnt/SSDs/esxivms");
                    }
                    VMServer.spec.VMDeployState = VMDeployStatus.readyForDeployment;
                }
                // Store some info about the VM server here.
                VMServerBladeID = VMServer.spec.bladeID.Value;
            }

            using (hypervisor hyp = makeHypervisorForBlade_ESXi(vmServerIP))
            {
                // now SSH to the blade and actually create the VM.
                string destDir = "/vmfs/volumes/esxivms/" + VMServerBladeID + "_" + childVM.spec.vmConfigKey;
                string destDirDatastoreType = "[esxivms] " + VMServerBladeID + "_" + childVM.spec.vmConfigKey;
                string vmxPath = destDir + "/PXETemplate.vmx";

                if (deployDeadline < DateTime.Now)
                    throw new TimeoutException();

                // Remove the VM if it's already there. We don't mind if these commands fail - which they will, if the VM doesn't
                // exist. We power off by directory and also by name, just in case a previous provision has left the VM hanging around.
                string dstDatastoreDirEscaped = destDirDatastoreType.Replace("[", "\\[").Replace("]", "\\]");
                hypervisor.doWithRetryOnSomeExceptions(() => hyp.startExecutable("vim-cmd", "vmsvc/power.off `vim-cmd vmsvc/getallvms | grep \"" + dstDatastoreDirEscaped + "\"`"));
                hypervisor.doWithRetryOnSomeExceptions(() => hyp.startExecutable("vim-cmd", "vmsvc/power.off `vim-cmd vmsvc/getallvms | grep \"" + childVM.spec.displayName + "\"`"));
                hypervisor.doWithRetryOnSomeExceptions(() => hyp.startExecutable("vim-cmd", "vmsvc/unregister `vim-cmd vmsvc/getallvms | grep \"" + dstDatastoreDirEscaped + "\"`"));
                hypervisor.doWithRetryOnSomeExceptions(() => hyp.startExecutable("vim-cmd", "vmsvc/unregister `vim-cmd vmsvc/getallvms | grep \"" + childVM.spec.displayName + "\"`"));

                // copy the template VM into a new directory
                doCmdAndCheckSuccess(hyp, "rm", " -rf " + destDir);
                doCmdAndCheckSuccess(hyp, "cp", " -R /vmfs/volumes/esxivms/PXETemplate " + destDir);
                // and then customise it.
                doCmdAndCheckSuccess(hyp, "sed", " -e 's/ethernet0.address[= ].*/ethernet0.address = \"" + childVM.spec.eth0MAC + "\"/g' -i " + vmxPath);
                doCmdAndCheckSuccess(hyp, "sed", " -e 's/ethernet1.address[= ].*/ethernet1.address = \"" + childVM.spec.eth1MAC + "\"/g' -i " + vmxPath);
                doCmdAndCheckSuccess(hyp, "sed", " -e 's/displayName[= ].*/displayName = \"" + childVM.spec.displayName + "\"/g' -i " + vmxPath);
                doCmdAndCheckSuccess(hyp, "sed", " -e 's/memSize[= ].*/memSize = \"" + childVM.spec.memoryMB + "\"/g' -i " + vmxPath);
                doCmdAndCheckSuccess(hyp, "sed", " -e 's/sched.mem.min[= ].*/sched.mem.min = \"" + childVM.spec.memoryMB + "\"/g' -i " + vmxPath);
                doCmdAndCheckSuccess(hyp, "sed", " -e 's/sched.mem.minSize[= ].*/sched.mem.minSize = \"" + childVM.spec.memoryMB + "\"/g' -i " + vmxPath);
                doCmdAndCheckSuccess(hyp, "sed", " -e 's/numvcpus[= ].*/numvcpus = \"" + childVM.spec.cpuCount + "\"/g' -i " + vmxPath);
                doCmdAndCheckSuccess(hyp, "sed", " -e 's/uuid.bios[= ].*//g' -i " + vmxPath);
                doCmdAndCheckSuccess(hyp, "sed", " -e 's/uuid.location[= ].*//g' -i " + vmxPath);
                // doCmdAndCheckSuccess(hyp, "sed", " -e 's/serial0.fileName[= ].*/" + "serial0.fileName = \"telnet://:" + (1000 + threadState.childVM.vmSpecID) + "\"/g' -i " + vmxPath));

                // Now add that VM to ESXi, and the VM is ready to use.
                // We do this with a retry, because I'm seeing it fail occasionally >_<
                hypervisor.doWithRetryOnSomeExceptions(() =>
                    doCmdAndCheckSuccess(hyp, "vim-cmd", " solo/registervm " + vmxPath)
                    );

                if (deployDeadline < DateTime.Now)
                    throw new TimeoutException();
            }

            // If the VM already has disks set up, delete them.
            // Note that we own the VM now, but want to make the VM as if the real requestor owned it.
            itemToAdd itm = childVM.spec.toItemToAdd(true);

            if (deployDeadline < DateTime.Now)
                throw new TimeoutException();

            // Now we can select the new snapshot. We must be very careful and do this quickly, with the appropriate write lock
            // held, because it will block the ipxe script creation until we unlock.
            childVM.upgradeLocks(bladeLockType.lockNone, bladeLockType.lockSnapshot);
            resultCode snapshotRes = selectSnapshotForVM(childVM, itm.snapshotName);
            childVM.downgradeLocks(bladeLockType.lockNone, bladeLockType.lockSnapshot);

            if (snapshotRes != resultCode.success)
            {
                Debug.WriteLine(DateTime.Now + childVM.spec.VMIP + ": Failed to select snapshot,  " + snapshotRes);
                _logEvents.Add(DateTime.Now + childVM.spec.VMIP + ": Failed to select snapshot,  " + snapshotRes);
            }

            using (lockableBladeSpec bladeSpec = db.getBladeByIP(vmServerIP, 
                bladeLockType.lockNASOperations,
                bladeLockType.lockNone))
            {
                NASAccess nas = getNasForDevice(bladeSpec.spec);
                createDisks.Program.deleteBlade(itm.cloneName, nas);

                if (deployDeadline < DateTime.Now)
                    throw new TimeoutException();

                // Now create the disks, and customise the VM by naming it appropriately.
                // TODO: is _basePath correct?
                createDisks.Program.addBlades(nas, new[] { itm }, itm.snapshotName, _basePath, "bladebasestable-esxi", null, null,
                    (a, b) => {
                        return (hypervisorWithSpec<hypSpec_vmware>)makeHypervisorForVM(childVM, bladeSpec);
                    }, deployDeadline);
            }
            // TODO: Ability to deploy transportDriver

            if (deployDeadline < DateTime.Now)
                throw new TimeoutException();

            // All done. We can mark the blade as in use. Again, we are careful to hold write locks for as short a time as is
            // possible, to avoid blocking the PXE-script generation.
            childVM.upgradeLocks(bladeLockType.lockNone, bladeLockType.lockOwnership);
            childVM.spec.state = bladeStatus.inUse;
            childVM.spec.currentOwner = childVM.spec.nextOwner;
            childVM.spec.nextOwner = null;
            childVM.spec.lastKeepAlive = DateTime.Now;
            childVM.downgradeLocks(bladeLockType.lockNone, bladeLockType.lockOwnership);

            return new result(resultCode.success);
        }

        private void doCmdAndCheckSuccess(hypervisor hyp, string cmd, string args)
        {
            executionResult res = hypervisor.doWithRetryOnSomeExceptions(() => hyp.startExecutable(cmd, args));
            if (res.resultCode != 0)
            {
                _logEvents.Add(string.Format("Command '{0}' with args '{1}' returned failure code {2}; stdout is '{3} and stderr is '{4}'", cmd, args, res.resultCode, res.stdout, res.stderr));
                throw new hypervisorExecutionException("failed to execute ssh command");
            }
        }

        public resultAndBladeName getProgressOfVMRequest(string waitToken)
        {
            if (waitToken == null)
                return new resultAndBladeName(new result(resultCode.bladeNotFound, "No waitToken supplied"), null);
            
            if (!Monitor.TryEnter(_VMDeployState, TimeSpan.FromSeconds(15)))
            {
                return new resultAndBladeName(new result(resultCode.unknown, "unable to acquire lock on VMDeployState after 15 seconds"), waitToken);
            }
            try
            {
                lock (_VMDeployState)
                {
                    if (!_VMDeployState.ContainsKey(waitToken))
                        return new resultAndBladeName(new result(resultCode.bladeNotFound, "Blade not currently being deployed"), waitToken);

                    return _VMDeployState[waitToken].currentProgress;
                }
            }
            finally 
            {
                Monitor.Exit(_VMDeployState);
            }
        }
       
        public resultAndBladeName RequestAnySingleNode(string requestorIP)
        {
            // Put blades in an order of preference. First come unused blades, then used blades with an empty queue.
            using (disposingList<lockableBladeSpec> blades = db.getAllBladeInfo(x => true, bladeLockType.lockOwnership, bladeLockType.lockOwnership))
            {
                IEnumerable<lockableBladeSpec> unusedBlades = blades.Where(x => x.spec.currentOwner == null);
                IEnumerable<lockableBladeSpec> emptyQueueBlades = blades.Where(x => x.spec.currentOwner != null && x.spec.nextOwner == null);
                IEnumerable<lockableBladeSpec> orderedBlades = unusedBlades.Concat(emptyQueueBlades);

                foreach (lockableBladeSpec reqBlade in orderedBlades)
                {
                    result res = requestBlade(reqBlade, requestorIP);
                    if (res.code == resultCode.success || res.code == resultCode.pending)
                        return new resultAndBladeName(res) { bladeName = reqBlade.spec.bladeIP };
                }
                // Otherwise, all blades have full queues.
                return new resultAndBladeName(resultCode.bladeQueueFull, null, "All blades are full");
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
                    _logEvents.Add("Cannot find bootMenuController endpoint at " + wcfURI);
                }
            }
        }

        public void keepAlive(string requestorIP)
        {
            db.refreshKeepAliveForRequestor(requestorIP);
        }

        public bool isBladeMine(string nodeIp, string requestorIp)
        {
            checkKeepAlives(requestorIp);

            using (disposingList<lockableBladeSpec> coll = db.getAllBladeInfo(
                x => (x.currentOwner == requestorIp) && (x.bladeIP == nodeIp), bladeLockType.lockOwnership, bladeLockType.lockNone))
            {
                return coll.Count > 0;
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
        
        public resultCode selectSnapshotForBlade(string bladeName, string newShot)
        {
            using (var blade = db.getBladeByIP(bladeName, bladeLockType.lockSnapshot | bladeLockType.lockNASOperations, bladeLockType.lockSnapshot | bladeLockType.lockNASOperations))
            {
                blade.spec.currentSnapshot = newShot;
                itemToAdd itm = blade.spec.toItemToAdd(false);
                createDisks.Program.repairBladeDeviceNodes(new[] { itm });

                return resultCode.success;
            }
        }

        public resultCode selectSnapshotForVM(string vmName, string newShot)
        {
            using (lockableVMSpec vm = db.getVMByIP(vmName, bladeLockType.lockSnapshot, bladeLockType.lockSnapshot))
            {
                return selectSnapshotForVM(vm, newShot);
            }
        }

        private static resultCode selectSnapshotForVM(lockableVMSpec VM, string newShot)
        {
            VM.spec.currentSnapshot = newShot;

            return resultCode.success;
        }

        public string getFreeNASSnapshotPath(string requestorIp, string nodeIp)
        {
            using (var ownership = db.getBladeByIP(nodeIp, bladeLockType.lockNone, bladeLockType.lockNone))
            {
                if (ownership.spec != null)
                    return String.Format("{0}-{1}-{2}", nodeIp, requestorIp, ownership.spec.currentSnapshot);
            }

            using (lockableVMSpec ownership = db.getVMByIP(nodeIp, bladeLockType.lockSnapshot, bladeLockType.lockNone))
            {
                if (ownership.spec != null)
                    return String.Format("{0}-{1}-{2}", nodeIp, requestorIp, ownership.spec.currentSnapshot);
            }

            throw new Exception("todo: report blade not found error");
        }
        
        public void markLastKnownBIOS(lockableBladeSpec reqBlade, string biosxml)
        {
            lock (_currentBIOSOperations)
            {
                string waitToken = handleTypes.BOS + "_" + reqBlade.spec.bladeIP.GetHashCode().ToString();
                _currentBIOSOperations[waitToken].isFinished = true;
            }

            reqBlade.spec.currentlyHavingBIOSDeployed = false;
            reqBlade.spec.lastDeployedBIOS = biosxml;

        }

        private resultAndBIOSConfig checkBIOSOperationProgress(string waitToken)
        {
            lock (_currentBIOSOperations)
            {
                if (!_currentBIOSOperations.ContainsKey(waitToken))
                    return new resultAndBIOSConfig(new result(resultCode.bladeNotFound, "This blade is not being deployed right now"), waitToken);

                string hostIP = _currentBIOSOperations[waitToken].hostIP;

                result status = biosRWEngine.checkBIOSOperationProgress(hostIP);

                if (status.code == resultCode.success)
                {
                    using (lockableBladeSpec blade = db.getBladeByIP(hostIP, bladeLockType.lockBIOS, bladeLockType.lockNone))
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
                bladeLockType.lockOwnership))
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
                    bladeLockType.lockNone))
                {
                    return sourceBlade.spec.generateIPXEScript();
                }
            }
            else
            {
                using (lockableVMSpec vmState = db.getVMByIP(srcIP, bladeLockType.lockOwnership | bladeLockType.lockSnapshot, bladeLockType.lockNone))
                {
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
            bladeSpec newSpec = new bladeSpec(db.conn, newIP, newISCSIIP, newILOIP, newDebugPort, false, VMDeployStatus.needsPowerCycle, null, bladeLockType.lockAll, bladeLockType.lockAll);
            db.addNode(newSpec);
            return resultCode.success;
        }

        public vmSpec[] getVMByVMServerIP_nolocking(string bladeIP)
        {
            return db.getVMByVMServerIP_nolocking(bladeIP).ToArray();
        }
    }

}