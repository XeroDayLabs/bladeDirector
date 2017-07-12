using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using bladeDirector.Properties;
using hypervisors;

namespace bladeDirector
{
    /// <summary>
    /// Impliment this to deploy a BIOS configuration to a hardware blade.
    /// </summary>
    public interface IBiosReadWrite
    {
        void cancelOperationsForBlade(string nodeIP);
        resultCode rebootAndStartWritingBIOSConfiguration(hostStateManager_core parent, string nodeIp, string biosxml);
        resultCode rebootAndStartReadingBIOSConfiguration(hostStateManager_core parent, string nodeIp);
        resultCode checkBIOSOperationProgress(string bladeIp);
    }

    public class mockedBiosThreadParams
    {
        /// <summary>
        /// This gets set when it is safe for the starting thread to return to the caller.
        /// </summary>
        public ManualResetEvent bladeLockedEvent;

        public resultCode result;
        public bool isFinished;
        public string nodeIP;
        public bool isCancelled;
        public hostDB db;
        public DateTime deadline;
    }

    /// <summary>
    /// This class handles async operations involving BIOS config read/write.
    /// </summary>
    public class biosReadWrite_LTSP_iLo : IBiosReadWrite
    {
        private ConcurrentDictionary<string, biosThreadState> _currentlyDeployingNodes = new ConcurrentDictionary<string, biosThreadState>();

        private hostStateManager_core _hostManager;

        public resultCode rebootAndStartReadingBIOSConfiguration(hostStateManager_core hostManager, string bladeIP)
        {
            _hostManager = hostManager;
            return rebootAndStartPerformingBIOSOperation(bladeIP, null, getBIOS);
        }

        public resultCode rebootAndStartWritingBIOSConfiguration(hostStateManager_core hostManager, string bladeIP, string biosXML)
        {
            _hostManager = hostManager;
            return rebootAndStartPerformingBIOSOperation(bladeIP, biosXML, setBIOS);
        }

        private resultCode rebootAndStartPerformingBIOSOperation(string bladeIP, string biosxml, Action<biosThreadState> onCompletion)
        {
            // Wait, do we even need thread safety in sqlite?
            // yes, we do, but not here
            //string sqliteOpts = SQLiteConnection.SQLiteCompileOptions;
            //if (!sqliteOpts.Contains("THREADSAFE=1"))
            //    throw new Exception();
            
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
                return resultCode.bladeInUse;

            // Now, go ahead and spin up a new thread to handle this update, and start it.
            newState.onBootFinish = onCompletion;
            newState.onBootFailure = handleReadOrWriteBIOSError;
            newState.rebootThread = new Thread(ltspBootThread)
            {
                Name = "Booting " + bladeIP + " to LTSP"
            };
            newState.rebootThread.Start(newState);

            return resultCode.pending;
        }

        public resultCode checkBIOSOperationProgress(string nodeIp)
        {
            biosThreadState newState;

            if (_currentlyDeployingNodes.TryGetValue(nodeIp, out newState) == false || newState == null)
                return resultCode.bladeNotFound;

            if (!newState.isFinished)
                return resultCode.pending;

            return newState.result;
        }

        private static void handleReadOrWriteBIOSError(biosThreadState state)
        {
            try
            {
                state.result = resultCode.genericFail;
                state.isFinished = true;

                state.blade.spec.currentlyHavingBIOSDeployed = false;
            }
            finally
            {
                state.blade.Dispose();
            }
        }

        private void getBIOS(biosThreadState state)
        {
            try
            {
                _GetBIOS(state);
            }
            catch (Exception e)
            {
                _hostManager.addLogEvent(string.Format("Reading BIOS from {0} resulted in exception {1}", state.nodeIP, e));
                state.result = resultCode.genericFail;
                state.isFinished = true;
                //_hostManager.markBIOSOperationFailure(state.nodeIP);
            }
            finally
            {
                state.blade.Dispose();
            }
        }

        private void _GetBIOS(biosThreadState state)
        {
            copyDeploymentFilesToBlade(state.nodeIP, null);

            using (hypervisor hyp = _hostManager.makeHypervisorForBlade_LTSP(state.nodeIP))
            {
                executionResult res = hyp.startExecutable("bash", "~/getBIOS.sh");
                if (res.resultCode != 0)
                {
                    _hostManager.addLogEvent(string.Format("Reading BIOS on {0} resulted in error code {1}", state.nodeIP, res.resultCode));
                    Debug.WriteLine(DateTime.Now + "Faied bios deploy, error code " + res.resultCode);
                    Debug.WriteLine(DateTime.Now + "stdout " + res.stdout);
                    Debug.WriteLine(DateTime.Now + "stderr " + res.stderr);
                    state.result = resultCode.genericFail;
                }
                else
                {
                    _hostManager.addLogEvent(string.Format("Deployed BIOS successfully to {0}", state.nodeIP));
                    state.result = resultCode.success;
                }

                // Retrieve the output
                state.biosxml = hyp.getFileFromGuest("currentbios.xml");

                // All done, now we can power off and return.
                hyp.powerOff();
            }

            _hostManager.markLastKnownBIOS(state.nodeIP, state.biosxml);
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
                _hostManager.addLogEvent(string.Format("Deploying BIOS on {0} resulted in exception {1}", state.nodeIP, e));
                state.result = resultCode.genericFail;
                state.isFinished = true;
                //_hostManager.markBIOSOperationFailure(state.nodeIP);
            }
            finally
            {
                state.blade.Dispose();
            }
        }

        private void _SetBIOS(biosThreadState state)
        {
            // Okay, now the box is up :)
            // SCP some needed files to it.
            copyDeploymentFilesToBlade(state.nodeIP, state.biosxml);

            // And execute the command to deploy the BIOS via SSH.
            using (hypervisor hyp = _hostManager.makeHypervisorForBlade_LTSP(state.nodeIP))
            {
                executionResult res = hyp.startExecutable("bash", "~/applyBIOS.sh");
                if (res.resultCode != 0)
                {
                    _hostManager.addLogEvent(string.Format("Deploying BIOS on {0} resulted in error code {1}", state.nodeIP, res.resultCode));
                    Debug.WriteLine(DateTime.Now + "Faied bios deploy, error code " + res.resultCode);
                    Debug.WriteLine(DateTime.Now + "stdout " + res.stdout);
                    Debug.WriteLine(DateTime.Now + "stderr " + res.stderr);
                    state.result = resultCode.genericFail;
                }
                else
                {
                    _hostManager.addLogEvent(string.Format("Deployed BIOS successfully to {0}", state.nodeIP));
                    _hostManager.markLastKnownBIOS(state.nodeIP, state.biosxml);
                    state.result = resultCode.success;
                }

                // All done, now we can power off and return.
                hyp.powerOff();
            }

            state.isFinished = true;
        }

        private void copyDeploymentFilesToBlade(string nodeSpec, string biosConfigFile)
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
                        TimeSpan.FromSeconds(10), TimeSpan.FromMinutes(3));
                }
                // And copy this file specifically as binary.
                hypervisor.doWithRetryOnSomeExceptions(() =>
                {
                    hyp.copyToGuestFromBuffer("conrep", Resources.conrep);
                }, TimeSpan.FromSeconds(10), TimeSpan.FromMinutes(3));
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
            // We explicitly allow VMs to be migrated to this blade during BIOS flashing.
            param.blade = _hostManager.db.getBladeByIP(param.nodeIP, bladeLockType.lockVMCreation);
            // Power cycle it
            _hostManager.startBladePowerOff(param.blade);
            _hostManager.startBladePowerOn(param.blade);

            // Wait for it to boot.  Note that we don't ping the client repeatedly here - since the Ping class can cause 
            // a BSoD.. ;_; Instead, we wait for port 22 (SSH) to be open.
            param.connectDeadline = DateTime.Now + TimeSpan.FromMinutes(5);
            _hostManager.setCallbackOnTCPPortOpen(22, param.onBootFinish, param.onBootFailure, param.connectDeadline, param);
        }

        public void cancelOperationsForBlade(string bladeIP)
        {
            resultCode res = checkBIOSOperationProgress(bladeIP);

            if (res != resultCode.pending)
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

                while (!toCancel.isFinished)
                {
                    _hostManager.addLogEvent("Waiting for BIOS operation on " + bladeIP + " to cancel");
                    Thread.Sleep(TimeSpan.FromSeconds(10));
                }
            }
            biosThreadState foo;
            _currentlyDeployingNodes.TryRemove(bladeIP, out foo);
        }
    }
}