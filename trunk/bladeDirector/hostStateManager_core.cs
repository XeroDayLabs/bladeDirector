using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.ServiceModel;
using System.Threading;
using System.Threading.Tasks;
using bladeDirector.bootMenuWCF;
using bladeDirector.Properties;
using createDisks;
using hypervisors;

namespace bladeDirector
{
    public class waitTokenType
    {
        public string val;
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

        private Dictionary<waitTokenType, VMThreadState> _VMDeployState = new Dictionary<waitTokenType, VMThreadState>();

        private Dictionary<string, inProgressLogIn> _currentlyRunningLogIns = new Dictionary<string, inProgressLogIn>(); 

        protected abstract hypervisor makeHypervisorForVM(lockableVMSpec VM, lockableBladeSpec parentBladeSpec);
        protected abstract hypervisor makeHypervisorForBlade_windows(lockableBladeSpec bladeSpec);
        protected abstract hypervisor makeHypervisorForBlade_LTSP(lockableBladeSpec bladeSpec);
        protected abstract hypervisor makeHypervisorForBlade_ESXi(lockableBladeSpec bladeSpec);

        // FIXME
        protected abstract void waitForESXiBootToComplete(hypervisor hyp);
        public abstract void startBladePowerOff(lockableBladeSpec blade);
        public abstract void startBladePowerOn(lockableBladeSpec blade);
        public abstract void setCallbackOnTCPPortOpen(int nodePort, Action<biosThreadState> onError, Action<biosThreadState> action, DateTime deadline, biosThreadState biosThreadState);
        protected abstract NASAccess getNasForDevice(bladeSpec vmServer);

        private vmServerControl _vmServerControl;

        protected hostStateManager_core(string basePath, vmServerControl newVmServerControl, IBiosReadWrite newBiosReadWrite)
        {
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

        public void addLogEvent(string newEntry)
        {
            lock (_logEvents)
            {
                _logEvents.Add(DateTime.Now + " : " + newEntry);
            }
        }

        public hypervisor makeHypervisorForVM(string bladeIP, string VMIP)
        {
            using(var blade = db.getBladeByIP(bladeIP, bladeLockType.lockNone))
            {
                using (lockableVMSpec VM = db.getVMByIP(VMIP))
                {
                    return makeHypervisorForVM(VM, blade);
                }
            }
        }

        public hypervisor makeHypervisorForBlade_ESXi(string bladeIP)
        {
            using (var blade = db.getBladeByIP(bladeIP, bladeLockType.lockNone))
            {
                return makeHypervisorForBlade_ESXi(blade);
            }
        }

        public hypervisor makeHypervisorForBlade_LTSP(string bladeIP)
        {
            using (var blade = db.getBladeByIP(bladeIP, bladeLockType.lockNone))
            {
                return makeHypervisorForBlade_LTSP(blade);
            }
        }

        private resultCode tryRequestNode(lockableBladeSpec reqBlade, string requestorID)
        {
            checkKeepAlives(reqBlade);

            string bladeIP = reqBlade.spec.bladeIP;

            // If the blade is currently unused, we can just take it.
            if (reqBlade.spec.state == bladeStatus.unused)
            {
                reqBlade.spec.currentOwner = requestorID;
                reqBlade.spec.state = bladeStatus.inUse;
                reqBlade.spec.lastKeepAlive = DateTime.Now;

                addLogEvent("Blade " + requestorID + " requested blade " + bladeIP + "(success, blade was idle)");
                notifyBootDirectorOfNode(reqBlade.spec);
                return resultCode.success;
            }

            // Otherwise, we need to request that the blade is released, and return 'pending'. 
            // Note that we don't permit a requestor to both own the blade, and be in the queue - this is because the
            // requestor would be unable to determine when its blade is allocated. We just return queuefull in that
            // situation.
            if (reqBlade.spec.currentOwner == requestorID)
            {
                addLogEvent("Blade " + requestorID + " requested blade " + bladeIP + "(failure, blade is already owned by this blade)");
                return resultCode.bladeQueueFull;
            }

            // If the blade is already queued as requested, just report OK and leave it there,
            if (reqBlade.spec.nextOwner == requestorID)
            {
                addLogEvent("Blade " + requestorID + " requested blade " + bladeIP + "(success, requestor was already in queue)");
                notifyBootDirectorOfNode(reqBlade.spec);
                return resultCode.success;
            }

            // See if the blade queue is actually full
            if (reqBlade.spec.nextOwner != null)
            {
                addLogEvent("Blade " + requestorID + " requested blade " + bladeIP + "(failure, blade queue is full)");
                return resultCode.bladeQueueFull;
            }

            // It's all okay, so request the release.
            reqBlade.spec.state = bladeStatus.releaseRequested;
            reqBlade.spec.nextOwner = requestorID;

            addLogEvent("Blade " + requestorID + " requested blade " + bladeIP + "(success, requestor added to blade queue)");
            return resultCode.pending;
        }

        public string logIn(string hostIP)
        {
            lock (_currentlyRunningLogIns)
            {
                string waitToken = hostIP.GetHashCode().ToString();

                // If there's already a login going on for this host, just use that one. Don't do two simultaneously.
                if (_currentlyRunningLogIns.ContainsKey(waitToken))
                {
                    if (!_currentlyRunningLogIns[waitToken].isFinished)
                        return _currentlyRunningLogIns[waitToken].waitToken;
                    _currentlyRunningLogIns.Remove(waitToken);
                }

                // Otherwise, make a new task and status, and start before we return.
                inProgressLogIn newLogIn = new inProgressLogIn
                {
                    waitToken = waitToken,
                    hostIP = hostIP,
                    isFinished = false,
                    status = resultCode.pending
                };
                Task loginTask = new Task(() => { logInBlocking(newLogIn); });
                _currentlyRunningLogIns.Add(newLogIn.waitToken, newLogIn);
                loginTask.Start();

                return newLogIn.waitToken;
            }
        }

        public resultCode getLogInProgress(string waitToken)
        {
            lock (_currentlyRunningLogIns)
            {
                if (_currentlyRunningLogIns.ContainsKey(waitToken))
                    return _currentlyRunningLogIns[waitToken].status;
                return resultCode.bladeNotFound;
            }
        }

        private void logInBlocking(inProgressLogIn login)
        {
            using (disposingListOfBladesAndVMs currentlyOwned = db.getBladesAndVMs(x => x.currentOwner == login.hostIP,
                x => (x.currentOwner == "vmserver" && x.nextOwner == login.hostIP) |  x.currentOwner == login.hostIP , 
                bladeLockType.lockAllExceptLongRunning))
            {
                // Lock all hosts that are either owner by this owner, or that we are preparing for this owner.
                IEnumerable<lockableVMSpec> bootingVMs = currentlyOwned.VMs.Where(x => x.spec.currentOwner == "vmserver");
                IEnumerable<lockableVMSpec> allocedVMs = currentlyOwned.VMs.Where(x => x.spec.currentOwner == login.hostIP);

                // Clean up anything that we are currently preparing for this owner
                foreach (lockableVMSpec allocated in bootingVMs)
                    releaseVM(allocated.spec.VMIP);

                // Clean up any hosts this blade has left over from any previous run
                foreach (lockableBladeSpec allocated in currentlyOwned.blades)
                    releaseBlade(allocated);

                // Clean up any VMs that have finished allocation
                foreach (lockableVMSpec allocated in allocedVMs)
                    releaseVM(allocated.spec.VMIP);

                // And now report that the login is complete.
                lock (_currentlyRunningLogIns)
                {
                    login.status = resultCode.success;
                    login.isFinished = true;
                }
            }
        }
        
        public void initWithBlades(string[] bladeIPs)
        {
            bladeSpec[] specs = new bladeSpec[bladeIPs.Length];
            int n = 0;
            foreach (string bladeIP in bladeIPs)
                specs[n++] = new bladeSpec(bladeIP, n.ToString(), n.ToString(), (ushort)n, false, VMDeployStatus.needsPowerCycle, "bioscontents", bladeLockType.lockAll);

            initWithBlades(specs);
        }

        public void initWithBlades(bladeSpec[] bladeSpecs)
        {
            db.initWithBlades(bladeSpecs);
        }

        public resultCode releaseBladeOrVM(string NodeIP, string requestorIP, bool force = false)
        {
            if (db.getAllBladeIP().Contains(NodeIP))
                return releaseBlade(NodeIP, requestorIP, force);
            if (db.getAllVMIP().Contains(NodeIP))
                return releaseVM(NodeIP);

            // Neither a blade nor a VM
            addLogEvent("Requestor " + requestorIP + " attempted to release blade " + NodeIP + " (blade not found)");
            return resultCode.bladeNotFound;
        }

        private resultCode releaseBlade(string reqBladeIP, string requestorIP, bool force)
        {
            using (lockableBladeSpec reqBlade = 
                db.getBladeByIP(reqBladeIP, bladeLockType.lockAllExceptLongRunning))
            {
                if (!force)
                {
                    if (reqBlade.spec.currentOwner != requestorIP)
                    {
                        addLogEvent("Requestor " + requestorIP + " attempted to release blade " + reqBlade.spec.bladeIP + " (failure: blade is not owned by requestor)");
                        return resultCode.bladeInUse;
                    }
                }
                resultCode success = releaseBlade(reqBlade);

                return success;
            }
        }

        private resultCode releaseBlade(lockableBladeSpec toRelease)
        {
            // Kill off any pending BIOS deployments ASAP.
            if (toRelease.spec.currentlyHavingBIOSDeployed)
            {
                toRelease.spec.currentlyHavingBIOSDeployed = false;
                biosRWEngine.cancelOperationsForBlade(toRelease.spec.bladeIP);
                while (biosRWEngine.checkBIOSOperationProgress(toRelease.spec.bladeIP) == resultCode.pending)
                {
                    Thread.Sleep(TimeSpan.FromSeconds(3));
                    Debug.WriteLine("Waiting for blade " + toRelease.spec.bladeIP + " to cancel BIOS operation...");
                }
            }

            // Reset any VM server the blade may be
            if (toRelease.spec.currentlyBeingAVMServer)
            {
                // TODO: cancel VM deployment process

                toRelease.spec.currentlyBeingAVMServer = false;
                List<vmSpec> childVMs = db.getVMByVMServerIP_nolocking(toRelease.spec.bladeIP);
                foreach (vmSpec child in childVMs)
                    releaseVM(child.VMIP);
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
                //if (reqBlade.state == bladeStatus.inUse || force)
                toRelease.spec.state = bladeStatus.unused;
                toRelease.spec.currentOwner = null;
                toRelease.spec.nextOwner = null;
                addLogEvent("Blade release : " + toRelease.spec.bladeIP + " (success, blade is now idle)");
            }

            return resultCode.success;
        }

        private resultCode releaseVM(string reqBladeIP)
        {
            vmSpec toDel = db.getVMByIP_withoutLocking(reqBladeIP);
            // If we are currently being deployed, we must wait until we are at a point wherby we can abort the deploy. We need
            // to be careful how to lock here, otherwise we risk deadlocking.
            bool islocked = false;
            try
            {
                Monitor.Enter(_VMDeployState);
                islocked = true;

                waitTokenType waitToken = makeDeployWaitToken(toDel);

                if (_VMDeployState.ContainsKey(waitToken) &&
                    _VMDeployState[waitToken].currentProgress.result.code != resultCode.pending)
                {
                    // Ahh, this VM is currently being deployed. We can't release it until the thread doing the deployment says so.
                    _VMDeployState[waitToken].deployDeadline = DateTime.MinValue;
                    while (_VMDeployState[waitToken].currentProgress.result.code == resultCode.pending)
                    {
                        _logEvents.Add("Waiting for VM deploy on " + toDel.VMIP + " to cancel");

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

            // Now we are no longer deploying, it is safe to lock the VM (todo: also check bios operations)
            string parent;
            using (lockableVMSpec lockedVM = db.getVMByIP(reqBladeIP))
            {
                parent = lockedVM.spec.parentBladeIP;
                // VMs always get destroyed on release.
                try
                {
                    using (lockableBladeSpec parentBlade = db.getBladeByIP(parent, bladeLockType.lockOwnership))
                    {
                        using (hypervisor hyp = makeHypervisorForVM(lockedVM, parentBlade))
                        {
                            hyp.powerOff();
                        }
                    }
                }
                catch (SocketException) { }
                catch (WebException) { }
                catch (VMNotFoundException) { }

                lockedVM.deleteOnRelease = true;
            }

            // Now, if the VM server is empty, we can power it off. Otherwise, just power off the VM instead.
            using (lockableBladeSpec parentBlade = db.getBladeByIP(parent, bladeLockType.lockOwnership | bladeLockType.lockVMCreation))
            {
                vmserverTotals VMTotals = db.getVMServerTotalsByVMServerIP(parentBlade.spec.bladeIP);
                {
                    if (VMTotals.VMs == 0)
                        releaseBlade(parentBlade);
                }

            }
            return resultCode.success;
        }

        public string getCurrentSnapshotForBladeOrVM(string nodeIp)
        {
            using (var reqBlade = db.getBladeByIP(nodeIp, bladeLockType.lockSnapshot))
            {
                if (reqBlade.spec != null)
                    return String.Format("{0}-{1}", reqBlade.spec.bladeIP, reqBlade.spec.currentSnapshot);
            }

            using (lockableVMSpec vmSpec = db.getVMByIP(nodeIp))
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
                return new resultAndBladeName(resultCode.genericFail, msg);
            }

            lock (_VMDeployState) // ugh
            {
                using (lockableBladeSpec freeVMServer = findAndLockBladeForNewVM(hwSpec))
                {
                    // Create rows for the child VM in the DB. Before we write it to the database, check we aren't about to add a 
                    // VM which conflicts with anything. If it does conflict, check the conflict is with an unused VM, and then 
                    // delete it before we add.
                    using (lockableVMSpec childVM = freeVMServer.spec.createChildVM(db, hwSpec, swReq, requestorIP))
                    {
                        if (getDeployStatus(childVM) != resultCode.pending &&
                            getDeployStatus(childVM) != resultCode.bladeNotFound)
                        {
                            // Oh, a deploy is already in progress for this VM. This should never happen, since .createChildVM
                            // should never return an already-existing VM.
                            return new resultAndBladeName(resultCode.bladeInUse, "Newly-created blade is already being deployed");
                        }
                        waitTokenType waitToken = makeDeployWaitToken(childVM);

                        // Now start a new thread, which will ensure the VM server is powered up and will then add the child VMs.
                        Thread worker = new Thread(VMServerBootThread)
                        {
                            Name = "VMAllocationThread for VM " + childVM.spec.VMIP + " on server " + freeVMServer.spec.bladeIP
                        };
                        VMThreadState deployState = new VMThreadState
                        {
                            vmServerIP = freeVMServer.spec.bladeIP,
                            childVM = childVM,
                            deployDeadline = DateTime.Now + TimeSpan.FromMinutes(25),
                            currentProgress = new resultAndBladeName(resultCode.pending)
                        };
                        _VMDeployState.Add(waitToken, deployState);

                        worker.Start(waitToken);
                        return new resultAndBladeName(resultCode.pending, waitToken);
                        // FIXME: make sure no-one can touch the blade before it gets locked by the VMThreadState
                    }
                }
            }
        }

        private static waitTokenType makeDeployWaitToken(lockableVMSpec childVm)
        {
            return new waitTokenType {val = childVm.spec.VMIP};
        }

        private static waitTokenType makeDeployWaitToken(vmSpec childVm)
        {
            return new waitTokenType { val = childVm.VMIP };
        }

        private resultCode getDeployStatus(lockableVMSpec vmSpec)
        {
            waitTokenType waitToken = makeDeployWaitToken(vmSpec);

            lock (_VMDeployState)
            {
                if (!_VMDeployState.ContainsKey(waitToken))
                    return resultCode.bladeNotFound;

                return _VMDeployState[waitToken].currentProgress.result.code;
            }
        }

        private lockableBladeSpec findAndLockBladeForNewVM(VMHardwareSpec hwSpec)
        {
            lockableBladeSpec freeVMServer = null;
//            checkKeepAlives();
            // First, we need to find a blade to use as a VM server. Do we have a free VM server? If so, just use that.
            // We create a new bladeSpec to make sure that we don't double-release when the disposingList is released.
            using (disposingList<lockableBladeSpec> serverList = db.getAllBladeInfo(x => true, bladeLockType.lockOwnership | bladeLockType.lockVMDeployState | bladeLockType.lockVMCreation | bladeLockType.lockOwnership))
            {
                lockableBladeSpec[] freeVMServerList = serverList.Where(x => x.spec.currentlyBeingAVMServer && x.spec.canAccommodate(db, hwSpec)).ToArray();

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
                        resultCode resp = tryRequestNode(freeNode,"vmserver");
                        if (resp == resultCode.success)
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
                        throw new Exception(); // fixmee, throwing is baaad
                    }
                }
            }
            return freeVMServer;
        }

        private void VMServerBootThread(object param)
        {
            waitTokenType operationHandle = (waitTokenType)param;
            VMThreadState threadState;
            lock (_VMDeployState)
            {
                threadState = _VMDeployState[operationHandle];
            }

            threadState.childVM = new lockableVMSpec(db.conn, threadState.childVM.spec);
            try
            {
                if (_VMServerBootThread(threadState.vmServerIP, threadState.childVM, threadState.deployDeadline) == resultCode.success)
                    threadState.currentProgress.result.code = resultCode.success;
                else
                    threadState.currentProgress.result.code = resultCode.genericFail;
            }
            catch (Exception e)
            {
                threadState.currentProgress.result.code = resultCode.genericFail;
                _logEvents.Add("VMServer boot thread fatal exception: " + e.Message + " at " + e.StackTrace);
            }
            finally
            {
                threadState.currentProgress.bladeName = threadState.childVM.spec.VMIP;
                threadState.childVM.Dispose();
            }
        }

        public resultCode rebootAndStartDeployingBIOSToBlade(string nodeIp, string requestorIp, string biosxml)
        {
            using (var reqBlade = db.getBladeByIP(nodeIp, bladeLockType.lockBIOS))
            {
                if (reqBlade.spec.currentOwner != requestorIp)
                    return resultCode.bladeInUse;

                // First, check if this BIOS is already deployed to this machine.
                if (reqBlade.spec.lastDeployedBIOS == biosxml)
                    return resultCode.noNeedLah;

                // Mark the blade as BIOS-flashing. This will mean that, next time it boots, it will be served the LTSP image.
                reqBlade.spec.currentlyHavingBIOSDeployed = true;

                return biosRWEngine.rebootAndStartWritingBIOSConfiguration(this, reqBlade.spec.bladeIP, biosxml);
            }
        }

        public resultCode rebootAndStartReadingBIOSConfiguration(string nodeIp, string requestorIp)
        {
            using (lockableBladeSpec reqBlade = db.getBladeByIP(nodeIp, bladeLockType.lockBIOS))
            {
                if (reqBlade.spec.currentOwner != requestorIp)
                    return resultCode.bladeInUse;

                // Mark the blade as BIOS-flashing. This will mean that, next time it boots, it will be served the LTSP image.
                reqBlade.spec.currentlyHavingBIOSDeployed = true;

                return biosRWEngine.rebootAndStartReadingBIOSConfiguration(this, reqBlade.spec.bladeIP);
            }
        }

        private resultCode _VMServerBootThread(string vmServerIP, lockableVMSpec childVM, DateTime deployDeadline)
        {
            // First, bring up the physical machine. It'll get the ESXi ISCSI config and boot up.
            // Ensure only one thread tries to power on each VM server at once, by locking the physical blade. The first thread 
            // can get the responsilibility of observing the vmServerBootState and power on/off as neccessary.
            long VMServerBladeID;
            using (var VMServer = db.getBladeByIP(vmServerIP, bladeLockType.lockVMDeployState))
            {
                if (VMServer.spec.VMDeployState == VMDeployStatus.needsPowerCycle)
                {
                    using (hypervisor hyp = makeHypervisorForBlade_ESXi(VMServer.spec.bladeIP))
                    {
                        if (deployDeadline < DateTime.Now)
                            throw new TimeoutException();

                        hyp.connect();

                        if (getLastDeployedBIOSForBlade(VMServer.spec.bladeIP) != Resources.VMServerBIOS)
                        {
                            biosRWEngine.rebootAndStartWritingBIOSConfiguration(this, VMServer.spec.bladeIP, Resources.VMServerBIOS);

                            resultCode progress = resultCode.pending;
                            while (progress == resultCode.pending)
                            {
                                progress = biosRWEngine.checkBIOSOperationProgress(VMServer.spec.bladeIP);
                                if (progress != resultCode.pending &&
                                    progress != resultCode.unknown &&
                                    progress != resultCode.success)
                                {
                                    throw new Exception("BIOS deploy failed, returning " + progress);
                                }
                            }
                        }
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
                VMServerBladeID = VMServer.spec.bladeID;
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
                doCmdAndCheckSuccess(hyp, "sed", " -e 's/memSize[= ].*/memSize = \"" + childVM.spec.hwSpec.memoryMB + "\"/g' -i " + vmxPath);
                doCmdAndCheckSuccess(hyp, "sed", " -e 's/sched.mem.min[= ].*/sched.mem.min = \"" + childVM.spec.hwSpec.memoryMB + "\"/g' -i " + vmxPath);
                doCmdAndCheckSuccess(hyp, "sed", " -e 's/sched.mem.minSize[= ].*/sched.mem.minSize = \"" + childVM.spec.hwSpec.memoryMB + "\"/g' -i " + vmxPath);
                doCmdAndCheckSuccess(hyp, "sed", " -e 's/numvcpus[= ].*/numvcpus = \"" + childVM.spec.hwSpec.cpuCount + "\"/g' -i " + vmxPath);
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

            using (lockableBladeSpec bladeSpec = db.getBladeByIP(vmServerIP, bladeLockType.lockNASOperations | bladeLockType.lockOwnership))
            {
                NASAccess nas = getNasForDevice(bladeSpec.spec);
                Program.deleteBlade(itm.cloneName, nas);

                if (deployDeadline < DateTime.Now)
                    throw new TimeoutException();

                // Now create the disks, and customise the VM  by naming it appropriately.
                Program.addBlades(nas, new[] {itm}, itm.snapshotName, "localhost/bladeDirector", "bladebasestable-esxi", null, null,
                    (a, b) => {
                        return (hypervisorWithSpec<hypSpec_vmware>)makeHypervisorForVM(childVM, bladeSpec);
                    }, deployDeadline);
            }
            // TODO: Ability to deploy transportDriver

            if (deployDeadline < DateTime.Now)
                throw new TimeoutException();

            // Now we can select the new snapshot
            resultCode snapshotRes = selectSnapshotForVM(childVM, itm.snapshotName);
            if (snapshotRes != resultCode.success)
            {
                Debug.WriteLine(DateTime.Now + childVM.spec.VMIP + ": Failed to select snapshot,  " + snapshotRes);
                _logEvents.Add(DateTime.Now + childVM.spec.VMIP + ": Failed to select snapshot,  " + snapshotRes);
            }

            // All done.
            childVM.spec.state = bladeStatus.inUse;
            childVM.spec.currentOwner = childVM.spec.nextOwner;
            childVM.spec.nextOwner = null;
            childVM.spec.lastKeepAlive = DateTime.Now;

            return resultCode.success;
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

        public resultAndBladeName getProgressOfVMRequest(waitTokenType waitToken)
        {
            if (waitToken == null)
                return new resultAndBladeName(resultCode.bladeNotFound, waitToken);

            if (!Monitor.TryEnter(_VMDeployState, TimeSpan.FromSeconds(15)))
            {
                return new resultAndBladeName(resultCode.unknown, "unable to acquire lock on VMDeployState?!", waitToken);
            }
            try
            {
                lock (_VMDeployState)
                {
                    if (!_VMDeployState.ContainsKey(waitToken))
                        return new resultAndBladeName(resultCode.bladeNotFound, "Blade not currently being deployed", waitToken);

                    return _VMDeployState[waitToken].currentProgress;
                }
            }
            finally 
            {
                Monitor.Exit(_VMDeployState);
            }
        }
       
        public resultCodeAndBladeName RequestAnySingleNode(string requestorIP)
        {
            // Put blades in an order of preference. First come unused blades, then used blades with an empty queue.
            using (disposingList<lockableBladeSpec> blades = db.getAllBladeInfo(x => true, bladeLockType.lockOwnership))
            {
                IEnumerable<lockableBladeSpec> unusedBlades = blades.Where(x => x.spec.currentOwner == null);
                IEnumerable<lockableBladeSpec> emptyQueueBlades = blades.Where(x => x.spec.currentOwner != null && x.spec.nextOwner == null);
                IEnumerable<lockableBladeSpec> orderedBlades = unusedBlades.Concat(emptyQueueBlades);

                foreach (lockableBladeSpec reqBlade in orderedBlades)
                {
                    resultCode res = tryRequestNode(reqBlade, requestorIP);
                    if (res == resultCode.success || res == resultCode.pending)
                        return new resultCodeAndBladeName {bladeName = reqBlade.spec.bladeIP, code = res};
                }
                // Otherwise, all blades have full queues.
                return new resultCodeAndBladeName {bladeName = null, code = resultCode.bladeQueueFull};
            }
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
                x => (x.currentOwner == requestorIp) && (x.bladeIP == nodeIp), bladeLockType.lockNone))
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
        
        public string getLastDeployedBIOSForBlade(string nodeIp)
        {
            using (var reqBlade = db.getBladeByIP(nodeIp, bladeLockType.lockNone))
            {
                return reqBlade.spec.lastDeployedBIOS;
            }
        }

        public resultCode selectSnapshotForBlade(string bladeName, string newShot)
        {
            using (var blade = db.getBladeByIP(bladeName, bladeLockType.lockSnapshot | bladeLockType.lockNASOperations))
            {
                blade.spec.currentSnapshot = newShot;
                itemToAdd itm = blade.spec.toItemToAdd(false);
                Program.repairBladeDeviceNodes(new[] { itm });

                return resultCode.success;
            }
        }

        public resultCode selectSnapshotForVM(string vmName, string newShot)
        {
            using (lockableVMSpec vm = db.getVMByIP(vmName))
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
            using (var ownership = db.getBladeByIP(nodeIp, bladeLockType.lockNone))
            {
                if (ownership.spec != null)
                    return String.Format("{0}-{1}-{2}", nodeIp, requestorIp, ownership.spec.currentSnapshot);
            }

            using (lockableVMSpec ownership = db.getVMByIP(nodeIp))
            {
                if (ownership.spec != null)
                    return String.Format("{0}-{1}-{2}", nodeIp, requestorIp, ownership.spec.currentSnapshot);
            }

            throw new Exception("todo: report blade not found error");
        }
        
        public void markLastKnownBIOS(string bladeIP, string biosxml)
        {
            using (lockableBladeSpec reqBlade = db.getBladeByIP(bladeIP, bladeLockType.lockBIOS))
            {
                reqBlade.spec.currentlyHavingBIOSDeployed = false;
                reqBlade.spec.lastDeployedBIOS = biosxml;
            }            
        }

        public resultCodeAndBIOSConfig checkBIOSReadProgress(string nodeIp)
        {
            resultCode status = biosRWEngine.checkBIOSOperationProgress(nodeIp);
            if (status == resultCode.success)
            {
                using (var blade = db.getBladeByIP(nodeIp, bladeLockType.lockBIOS))
                {
                    return new resultCodeAndBIOSConfig(resultCode.success, blade.spec.lastDeployedBIOS);
                }
            }

            return new resultCodeAndBIOSConfig(status);
        }

        public resultCode checkBIOSWriteProgress(string nodeIp)
        {
            return biosRWEngine.checkBIOSOperationProgress(nodeIp);
        }

        protected void checkKeepAlives(string forOwnerIP)
        {
            using (disposingList<lockableBladeSpec> bladeInfo = db.getAllBladeInfo(x => x.currentOwner == forOwnerIP, bladeLockType.lockOwnership | bladeLockType.lockBIOS))
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
                                releaseBlade(reqBlade);
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
    }
}