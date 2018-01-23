using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Text.RegularExpressions;
using System.Threading;
using bladeDirectorWCF;
using hypervisors;

namespace bladeDirectorClient
{
    public enum VMSource
    {
        configuredServer,
        XDLClusterBlades
    }

    public class VMWareHypervisorPool
    {
        private readonly Object hypervisorSpecLock = new Object();
        private ConcurrentDictionary<hypSpec_vmware, bool> hypervisorSpecs = null;

        public hypervisor_vmware createHypervisorForNextFreeVMOrWait(string snapshotName = "clean", clientExecutionMethod execType = clientExecutionMethod.smbWithWMI, string bladeID = null)
        {
            while (true)
            {
                hypervisor_vmware toRet = createHypervisorForNextFreeVMOrNull(snapshotName, execType, bladeID: bladeID);
                if (toRet != null)
                    return toRet;

                Thread.Sleep(TimeSpan.FromSeconds(5));
            }
        }

        public hypervisorCollection<hypSpec_vmware> createAsManyVMSAsPossible(string snapshotName = "clean", clientExecutionMethod execType = clientExecutionMethod.smbWithWMI, VMSource src = VMSource.configuredServer, string bladeID = null)
        {
            hypervisorCollection<hypSpec_vmware> hyps = new hypervisorCollection<hypSpec_vmware>();

            while (true)
            {
                hypervisor_vmware thisHyp = machinePools.vmware.createHypervisorForNextFreeVMOrNull(snapshotName, execType, src, bladeID);
                if (thisHyp == null)
                    break;
                hyps.TryAdd(thisHyp.getConnectionSpec().kernelDebugIPOrHostname, thisHyp);
            }

            return hyps;
        }

        public hypervisor_vmware createHypervisorForNextFreeVMOrNull(string snapshotName = "clean", clientExecutionMethod execType = clientExecutionMethod.smbWithWMI, VMSource src = VMSource.configuredServer, string bladeID = null)
        {
            lock (hypervisorSpecLock)
            {
                populateSpecsIfNeeded(snapshotName, src);

                // Atomically find an unused VM, and mark it as in-use
                KeyValuePair<hypSpec_vmware, bool> hypKVP;
                lock (hypervisorSpecLock)
                {
                    KeyValuePair<hypSpec_vmware, bool>[] selectedHyp = hypervisorSpecs.Where(x => x.Value == false).ToArray();
                    if (selectedHyp.Length == 0)
                    {
                        // Allocation failed
                        return null;
                    }
                    if (bladeID != null)
                    {
                        // We actually throw if a user filter returned nothing, for now.
                        selectedHyp = selectedHyp.Where(x => x.Key.kernelVMServer.ToLower().Contains(bladeID.ToLower())).ToArray();
                        if (selectedHyp.Length == 0)
                            throw new Exception("XDL blade not found");
                    }

                    hypKVP = selectedHyp.First();
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

        private void populateSpecsIfNeeded(string snapshotName, VMSource src)
        {
            switch (src)
            {
                case VMSource.configuredServer:
                    populateSpecsIfNeeded_VMServer(snapshotName);
                    break;
                case VMSource.XDLClusterBlades:
                    populateSpecsIfNeeded_XDLCluster(snapshotName);
                    break;
                default:
                    throw new ArgumentOutOfRangeException("src", src, null);
            }
        }

        private void populateSpecsIfNeeded_VMServer(string snapshotName)
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

                    int VMServerIndex = 0;
                    for (int i = 1; i < kernelVMCount + 1; i++)
                    {
                        string vmname = String.Format("{0}-{1}", vmNameBase, i);
                        ushort vmPort = (ushort)(kernelVMPortBase + i - 1);

                        hyps[i - 1] = new hypSpec_vmware(
                            vmname, kernelVMServer,
                            kernelVMServerUsername, kernelVMServerPassword,
                            kernelVMUsername, kernelVMPassword,
                            snapshotName, null, vmPort, kernelVMDebugKey, vmname);

                        hypervisorSpecs[hyps[i - 1]] = false;

                        // Check the relevant port isn't already in use
                        ipUtils.ensurePortIsFree(vmPort);
                    }
                }
                catch (Exception)
                {
                    hypervisorSpecs = null;
                    throw;
                }
            }
        }

        private void populateSpecsIfNeeded_XDLCluster(string snapshotName)
        {
            if (hypervisorSpecs == null)
            {
                try
                {
                    hypervisorSpecs = new ConcurrentDictionary<hypSpec_vmware, bool>();

                    int kernelVMCount = Properties.Settings.Default.XDLClusterVMsPerBlade;

                    string kernelVMServerUsername = Properties.Settings.Default.XDLVMServerUsername;
                    string kernelVMServerPassword = Properties.Settings.Default.XDLVMServerPassword;
                    string kernelVMUsername = Properties.Settings.Default.VMWareVMUsername;
                    string kernelVMPassword = Properties.Settings.Default.VMWareVMPassword;
                    string kernelVMDebugKey = Properties.Settings.Default.VMWareVMDebugKey;

                    if (kernelVMServerUsername == "" || kernelVMServerPassword == "" || kernelVMDebugKey == "")
                        throw new Exception("BladeDirectorClient not configured properly");

                    List<hypSpec_vmware> hyps = new List<hypSpec_vmware>();

                    // Construct the hostnames of the blades we will be using
                    string[] bladeIndexesAsString = Properties.Settings.Default.XDLClusterBladeList.Split(',');
                    int[] bladeIndexes = bladeIndexesAsString.Select(x => Int32.Parse(x)).ToArray();
                    string[] bladeHostnames = bladeIndexes.Select(x => String.Format("blade{0:D2}.fuzz.xd.lan", x)).ToArray();

                    // Iterate over each blade, constructing VMs.
                    for (int index = 0; index < bladeHostnames.Length; index++)
                    {
                        string bladeHostname = bladeHostnames[index];
                        int bladeID = bladeIndexes[index];

                        // Get VMs on this blade
                        string[] vmnames = hypervisor_vmware.getVMNames(bladeHostname, kernelVMServerUsername, kernelVMServerPassword).OrderBy(x => x).ToArray();

                        // Now construct them
                        for (int vmIndex = 0; vmIndex < kernelVMCount; vmIndex++)
                        {
                            string vmname = vmnames[vmIndex];
                            // VMWare ports are generated according to the blade and VM IDs.
                            // VM 02 on blade 12 would get '51202'.
                            ushort vmPort = (ushort) (50000 + (bladeID*100) + (vmIndex) + 1);

                            var newHyp = new hypSpec_vmware(vmname, bladeHostname, kernelVMServerUsername, kernelVMServerPassword, kernelVMUsername, kernelVMPassword, snapshotName, null, vmPort, kernelVMDebugKey, vmname);

                            hyps.Add(newHyp);
                            hypervisorSpecs[newHyp] = false;

                            // Check the relevant port isn't already in use
                            ipUtils.ensurePortIsFree(vmPort);
                        }
                    }
                }
                catch (Exception)
                {
                    hypervisorSpecs = null;
                    throw;
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