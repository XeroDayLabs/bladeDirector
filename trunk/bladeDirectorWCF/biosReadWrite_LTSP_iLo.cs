using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using bladeDirectorWCF.Properties;
using hypervisors;

namespace bladeDirectorWCF
{
    public class mockedBiosThreadParams
    {
        public result result;
        public bool isFinished;
        public string nodeIP;
        public bool isCancelled;
        public hostDB db;
        public DateTime deadline;
        public bool isStarted;
        public hostStateManager_core parent;
        public string BIOSToWrite;
    }

    /// <summary>
    /// This class handles async operations involving BIOS config read/write.
    /// </summary>
    public class biosReadWrite_LTSP_iLo : IBiosReadWrite
    {


        private ConcurrentDictionary<string, biosThreadState> _currentlyDeployingNodes = new ConcurrentDictionary<string, biosThreadState>();

        private hostStateManager_core _hostManager;

        public result rebootAndStartReadingBIOSConfiguration(hostStateManager_core hostManager, string bladeIP)
        {
            _hostManager = hostManager;
            return rebootAndStartPerformingBIOSOperation(bladeIP, null, getBIOS);
        }

        public result rebootAndStartWritingBIOSConfiguration(hostStateManager_core hostManager, string bladeIP, string biosXML)
        {
            _hostManager = hostManager;
            return rebootAndStartPerformingBIOSOperation(bladeIP, biosXML, setBIOS);
        }

        private result rebootAndStartPerformingBIOSOperation(string bladeIP, string biosxml, Action<biosThreadState> onCompletion)
        {
            //  We need to:
            //  1) set this blade to boot into LTSP
            //  2) start the blade
            //  3) wait for it to boot
            //  4) SSH into it, and run conrep to configure the BIOS.

            // Init this node to have a null threadState, ie, to be idle, but only if it isn't there already.
            _currentlyDeployingNodes.TryAdd(bladeIP, null);

            // Add a new bios thread state if the blade is currently idle.
            bool failedToAdd = false; // FIXME: does this cause the lambda to leak?
            biosThreadState newState = _currentlyDeployingNodes.AddOrUpdate(bladeIP, (biosThreadState)null, (key, oldVal) =>
            {
                // If the previously-existing deploy status is finished, we can just add a new one.
                if (oldVal == null || oldVal.isFinished)
                    return new biosThreadState(bladeIP, biosxml, bladeIP);
                // Otherwise, oh no, another BIOS operation it is still in progress!
                failedToAdd = true;
                return null;
            });
            // If we failed to add, then abort the request
            if (failedToAdd)
                return new result(resultCode.bladeInUse, "Adding to _currentlyDeployingNodes failed");

            // Now, go ahead and spin up a new thread to handle this update, and start it.
            newState.onBootFinish = onCompletion;
            newState.onBootFailure = handleReadOrWriteBIOSError;
            newState.rebootThread = new Thread(ltspBootThread)
            {
                Name = "Booting " + bladeIP + " to LTSP"
            };
            newState.rebootThread.Start(newState);

            while (!newState.isStarted)
                Thread.Sleep(TimeSpan.FromMilliseconds(10));

            return new result(resultCode.pending, "LTSP thread created");
        }

        public result checkBIOSOperationProgress(string nodeIp)
        {
            biosThreadState newState;

            if (_currentlyDeployingNodes.TryGetValue(nodeIp, out newState) == false || newState == null)
                return new result(resultCode.bladeNotFound, "Blade is not being BIOS read nor written right now");

            if (!newState.isFinished)
                return new result(resultCode.pending, null);

            return newState.result;
        }

        public bool hasOperationStarted(string bladeIP)
        {
            lock (_currentlyDeployingNodes)
            {
                if (_currentlyDeployingNodes.ContainsKey(bladeIP) && 
                    _currentlyDeployingNodes[bladeIP] != null     &&
                    _currentlyDeployingNodes[bladeIP].isStarted)
                    return true;
                return false;
            }
        }

        private static void handleReadOrWriteBIOSError(biosThreadState state)
        {
            state.result = new result(resultCode.genericFail, "handleReadOrWriteBIOSError called");
            state.isFinished = true;
            state.blade.spec.currentlyHavingBIOSDeployed = false;
        }

        private void getBIOS(biosThreadState state)
        {
            try
            {
                _GetBIOS(state);
            }
            catch (Exception e)
            {
                string msg = string.Format("Reading BIOS from {0} resulted in exception {1}", state.nodeIP, e);
                _hostManager.addLogEvent(msg);
                state.result = new result(resultCode.genericFail, msg);

                using (var tmp = new tempLockElevation(state.blade, bladeLockType.lockNone, bladeLockType.lockBIOS))
                {
                    _hostManager.markLastKnownBIOS(state.blade, "unknown");
                }

                state.isFinished = true;
            }
        }

        private void _GetBIOS(biosThreadState state)
        {
            copyDeploymentFilesToBlade(state.blade, null, state.connectDeadline);

            using (hypervisor hyp = _hostManager.makeHypervisorForBlade_LTSP(state.blade))
            {
                executionResult res = hyp.startExecutable("bash", "~/getBIOS.sh");
                if (res.resultCode != 0)
                {
                    string msg = string.Format("Executing getBIOS.sh on {0} resulted in error code {1}", state.nodeIP, res.resultCode);
                    msg += "stdout: " + res.stdout;
                    msg += "stderr: " + res.stderr;
                    _hostManager.addLogEvent(msg);
                    state.result = new result(resultCode.genericFail, msg);
                }
                else
                {
                    string msg = string.Format("Deployed BIOS successfully to {0}", state.nodeIP);
                    _hostManager.addLogEvent(msg);
                    state.result = new result(resultCode.success, msg);
                }

                // Retrieve the output
                state.biosxml = hyp.getFileFromGuest("currentbios.xml");

                // All done, now we can power off and return.
                hyp.powerOff(state.connectDeadline);
            }

            using (var tmp = new tempLockElevation(state.blade, bladeLockType.lockNone, bladeLockType.lockBIOS))
            {
                _hostManager.markLastKnownBIOS(state.blade, state.biosxml);
            }

            state.isFinished = true;
        }

        private void setBIOS(biosThreadState state)
        {
            try
            {
                _SetBIOS(state);
            }
            catch (Exception e)
            {
                string msg = string.Format("Writing BIOS to {0} resulted in exception {1}", state.nodeIP, e);
                _hostManager.addLogEvent(msg);
                state.result = new result(resultCode.genericFail, msg);

                using (var tmp = new tempLockElevation(state.blade, bladeLockType.lockNone, bladeLockType.lockBIOS))
                {
                    _hostManager.markLastKnownBIOS(state.blade, "unknown");
                }

                state.isFinished = true;
            }
        }

        private void _SetBIOS(biosThreadState state)
        {
            // SCP some needed files to it.
            copyDeploymentFilesToBlade(state.blade, state.biosxml, state.connectDeadline);

            // And execute the command to deploy the BIOS via SSH.
            using (hypervisor hyp = _hostManager.makeHypervisorForBlade_LTSP(state.blade))
            {
                executionResult res = hyp.startExecutable("bash", "~/applyBIOS.sh");
                if (res.resultCode != 0)
                {
                    string msg = string.Format("Executing applyBIOS.sh on {0} resulted in error code {1}", state.nodeIP, res.resultCode);
                    msg += "stdout: " + res.stdout;
                    msg += "stderr: " + res.stderr;
                    _hostManager.addLogEvent(msg);
                    state.result = new result(resultCode.genericFail, msg);
                }
                else
                {
                    _hostManager.addLogEvent(string.Format("Deployed BIOS successfully to {0}", state.nodeIP));

                    using (var tmp = new tempLockElevation(state.blade, bladeLockType.lockNone, bladeLockType.lockBIOS))
                    {
                        _hostManager.markLastKnownBIOS(state.blade, state.biosxml);
                    }

                    state.result = new result(resultCode.success);
                }

                // All done, now we can power off and return.
                hyp.powerOff(state.connectDeadline);
            }

            state.isFinished = true;
        }

        private void copyDeploymentFilesToBlade(lockableBladeSpec nodeSpec, string biosConfigFile, DateTime deadline)
        {
            using (hypervisor hyp = _hostManager.makeHypervisorForBlade_LTSP(nodeSpec))
            {
                Dictionary<string, string> toCopy = new Dictionary<string, string>
                {
                    {"applyBIOS.sh", Resources.applyBIOS.Replace("\r\n", "\n")},
                    {"getBIOS.sh", Resources.getBIOS.Replace("\r\n", "\n")},
                    {"conrep.xml", Resources.conrep_xml.Replace("\r\n", "\n")}
                };
                if (biosConfigFile != null)
                    toCopy.Add("newbios.xml", biosConfigFile.Replace("\r\n", "\n"));

                foreach (KeyValuePair<string, string> kvp in toCopy)
                {
                    hypervisor.doWithRetryOnSomeExceptions(() => { hyp.copyToGuestFromBuffer(kvp.Key, kvp.Value); },
                        TimeSpan.FromSeconds(10), deadline: deadline);
                }
                // And copy this file specifically as binary.
                hypervisor.doWithRetryOnSomeExceptions(() =>
                {
                    hyp.copyToGuestFromBuffer("conrep", Resources.conrep);
                }, TimeSpan.FromSeconds(10), deadline: deadline);
            }
        }

        private void ltspBootThread(Object o)
        {
            biosThreadState param = (biosThreadState)o;
            try
            {
                param.result = new result(resultCode.pending);
                _ltspBootThreadStart(param);
            }
            catch (Exception e)
            {
                param.result = new result(resultCode.genericFail, e.Message);
            }
            finally
            {
                param.isFinished = true;
            }
        }

        private void _ltspBootThreadStart(biosThreadState param)
        {
            using (lockableBladeSpec blade = _hostManager.db.getBladeByIP(param.nodeIP, bladeLockType.lockBIOS, bladeLockType.lockBIOS, 
                permitAccessDuringBIOS: true, permitAccessDuringDeployment: true))
            {
                blade.spec.currentlyHavingBIOSDeployed = true;
            }
            param.connectDeadline = DateTime.Now + TimeSpan.FromMinutes(5);
            param.isStarted = true;

            using (lockableBladeSpec blade = _hostManager.db.getBladeByIP(param.nodeIP,
                bladeLockType.lockOwnership | bladeLockType.lockSnapshot,
                bladeLockType.lockNone, permitAccessDuringBIOS: true, permitAccessDuringDeployment: true))
            {
                // Power cycle it
                _hostManager.startBladePowerOff(blade, param.connectDeadline);
                _hostManager.startBladePowerOn(blade, param.connectDeadline);

                param.blade = blade;

                // Wait for it to boot.  Note that we don't ping the client repeatedly here - since the Ping class can cause 
                // a BSoD.. ;_; Instead, we wait for port 22 (SSH) to be open.
                _hostManager.setCallbackOnTCPPortOpen(22, param.onBootFinishEvent, param.onBootFailureEvent, param.connectDeadline, param);

                // Wait for the boot to either complete or to fail.
                while (true)
                {
                    if (!param.onBootFinishEvent.WaitOne(TimeSpan.FromMilliseconds(500)))
                    {
                        param.onBootFinish(param);
                        break;
                    }
                    if (!param.onBootFailureEvent.WaitOne(TimeSpan.FromMilliseconds(500)))
                    {
                        param.onBootFailure(param);
                        break;
                    }
                }
            }
        }

        public void cancelOperationsForBlade(string bladeIP)
        {
            result res = checkBIOSOperationProgress(bladeIP);

            if (res.code != resultCode.pending)
            {
                // None in progress.
                return;
            }

            // Okay, this blade has a pending BIOS read/write. We need to request that the relevant thread exists, and not
            // return until it has.
            biosThreadState toCancel = _currentlyDeployingNodes[bladeIP];
            while (!toCancel.isFinished)
            {
                toCancel.connectDeadline = DateTime.MinValue;

                // If we can't cancel within 30 seconds, we write a crashdump so that an operator can work out why.
                DateTime dumpTime = DateTime.Now + TimeSpan.FromSeconds(30);

                while (!toCancel.isFinished)
                {
                    _hostManager.addLogEvent("Waiting for BIOS operation on " + bladeIP + " to cancel");

                    if (DateTime.Now > dumpTime)
                    {
                        _hostManager.addLogEvent("Cancel has taken more than 30 seconds; writing dump");
                        miniDumpUtils.dumpSelf(Path.Combine(Settings.Default.internalErrorDumpPath, "slow_bios_" + Guid.NewGuid().ToString() + ".dmp"));

                        dumpTime = DateTime.MaxValue;
                    }

                    Thread.Sleep(TimeSpan.FromSeconds(1));
                }
            }

            biosThreadState foo;
            _currentlyDeployingNodes.TryRemove(bladeIP, out foo);
        }
    }
}