using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Threading;
using hypervisors;

namespace bladeDirectorClient
{
    public class VMWareHypervisorPool
    {
        private readonly Object hypervisorSpecLock = new Object();
        private ConcurrentDictionary<hypSpec_vmware, bool> hypervisorSpecs = null;

        public hypervisor_vmware createHypervisorForCurrentTest(string snapshotName = "clean", clientExecutionMethod execType = clientExecutionMethod.smb)
        {
            while (true)
            {
                lock (hypervisorSpecLock)
                {
                    if (hypervisorSpecs == null)
                    {
                        try
                        {
                            // time to initialise the collection, marking all as unused.
                            hypervisorSpecs = new ConcurrentDictionary<hypSpec_vmware, bool>();

                            int kernelVMCount = Properties.Settings.Default.kernelVMCount;
                            string vmNameBase = Properties.Settings.Default.VMWareVMName;
                            int kernelVMPortBase = Properties.Settings.Default.VMWareVMPortBase;
                            string kernelVMServer = Properties.Settings.Default.VMWareVMServer;
                            string kernelVMServerUsername = Properties.Settings.Default.VMWareVMServerUsername;
                            string kernelVMServerPassword = Properties.Settings.Default.VMWareVMServerPassword;
                            string kernelVMUsername = Properties.Settings.Default.VMWareVMUsername;
                            string kernelVMPassword = Properties.Settings.Default.VMWareVMPassword;
                            string kernelVMDebugKey = Properties.Settings.Default.VMWareVMDebugKey;

                            if (kernelVMCount == 0 || vmNameBase == "" || kernelVMPortBase == 0 || kernelVMServer == "" ||
                                kernelVMServerUsername == "" || kernelVMServerPassword == "" || kernelVMDebugKey == "")
                            {
                                throw new Exception("BladeDirectorClient not configured properly");
                            }

                            hypSpec_vmware[] hyps = new hypSpec_vmware[kernelVMCount];

                            for (int i = 1; i < kernelVMCount + 1; i++)
                            {
                                string vmname = String.Format("{0}-{1}", vmNameBase, i);
                                ushort vmPort = (ushort)(kernelVMPortBase + i);

                                hyps[i - 1] = new hypSpec_vmware(
                                    vmname, kernelVMServer,
                                    kernelVMServerUsername, kernelVMServerPassword,
                                    kernelVMUsername, kernelVMPassword,
                                    snapshotName, null, vmPort, kernelVMDebugKey, vmname);

                                hypervisorSpecs[hyps[i - 1]] = false;

                                // Check the relevant port isn't already in use
                                iLoHypervisorPool.ensurePortIsFree(vmPort);
                            }
                        }
                        catch (Exception)
                        {
                            hypervisorSpecs = null;
                            throw;
                        }
                    }

                    // Atomically find an unused hypervisor, and mark it as in-use
                    KeyValuePair<hypSpec_vmware, bool> hypKVP;
                    lock (hypervisorSpecLock)
                    {
                        KeyValuePair<hypSpec_vmware, bool>[] selectedHyp = hypervisorSpecs.Where(x => x.Value == false).Take(1).ToArray();
                        if (selectedHyp.Length == 0)
                        {
                            // Allocation failed!
                            Thread.Sleep(TimeSpan.FromSeconds(5));
                            continue;
                        }
                        hypKVP = selectedHyp[0];
                        hypervisorSpecs[hypKVP.Key] = true;
                    }

                    try
                    {
                        hypervisor_vmware toRet = new hypervisor_vmware(hypKVP.Key, execType);
                        toRet.setDisposalCallback(onDestruction);
                        return toRet;
                    }
                    catch (Exception)
                    {
                        lock (hypervisorSpecLock)
                        {
                            hypervisorSpecs[hypKVP.Key] = false;
                            throw;
                        }
                    }
                }
            }
        }

        private void onDestruction(hypSpec_vmware beingDestroyed)
        {
            lock (hypervisorSpecLock)
            {
                // Make a new hypervisor object, just so we can make sure the old blade is powered off.
                hypervisorWithSpec<hypSpec_vmware> VMBeingDestroyed = new hypervisor_vmware(beingDestroyed);
                {
                    VMBeingDestroyed.powerOff();
                }
                hypervisorSpecs[beingDestroyed] = false;
            }
        }
    }
}