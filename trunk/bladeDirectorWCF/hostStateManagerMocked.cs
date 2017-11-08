using System;
using System.Collections.Generic;
using System.Threading;
using bladeDirectorWCF.Properties;
using hypervisors;

namespace bladeDirectorWCF
{
    public class hostStateManagerMocked : hostStateManager_core
    {
        public delegate executionResult mockedExecutionDelegate(hypervisor sender, string command, string args, string workingDir, cancellableDateTime deadline = null);

        public delegate bool mockedConnectionAttempt(string nodeIp, int nodePort, Action<biosThreadState> onBootFinish, Action<biosThreadState> onError, DateTime deadline, biosThreadState biosThreadState);
        public mockedConnectionAttempt onTCPConnectionAttempt;

        private mockedNAS nas;
        private mockedExecutionHandler handler;

        public hostStateManagerMocked(string basePath)
            : base(basePath, new vmServerControl_mocked(), new biosReadWrite_mocked())
        {
            init();
        }

        public hostStateManagerMocked()
            : base(new vmServerControl_mocked(), new biosReadWrite_mocked())
        {
            init();
        }

        private void init()
        {
            setExecutionResults(mockedExecutionResponses.successful);
            setMockedNASFaultInjectionPolicy(NASFaultInjectionPolicy.retunSuccessful);
        }

        public void setMockedNASFaultInjectionPolicy(NASFaultInjectionPolicy newPolicy)
        {
            switch (newPolicy)
            {
                case NASFaultInjectionPolicy.retunSuccessful:
                    nas = new mockedNAS();
                    break;
                case NASFaultInjectionPolicy.failSnapshotDeletionOnFirstSnapshot:
                    nas = new mockedNASWithFailure();
                    // TODO: add this from the test
                    nas.addISCSITarget(new iscsiTarget() { id = 0x1337b33f, targetAlias = "dunno", targetName = "172.17.158.1-1.1.1.1-vm" });
                    break;
                default:
                    throw new ArgumentOutOfRangeException("newPolicy", newPolicy, null);
            }

            // Taken from real FreeNAS system, these are the snapshots required to use Discord.
            volume newVol = new volume
            {
                id = 1,
                avail = "1.8 TiB",
                compression = "inherit (lz4)",
                compressratio = "1.47x",
                mountpoint = "/mnt/SSDs/jails",
                name = "jails",
                path = "SSDs/jails",
                isreadonly = "inherit (off)",
                status = "-",
                volType = "dataset",
                used = "31.4 GiB (1%)",
                used_pct = "1"
            };
            nas.addVolume(newVol);

            snapshot baseSnapshot = new snapshot
            {
                filesystem = "SSDs/bladeBaseStable-esxi",
                fullname = "SSDs/bladeBaseStable-esxi@bladeBaseStable-esxi",
                id = "SSDs/bladeBaseStable-esxi@bladeBaseStable-esxi",
                mostrecent = "true",
                name = "bladeBaseStable-esxi",
                parent_type = "volume",
                refer = "20.0 GiB",
                replication = "null",
                used = "17.4 MiB"
            };
            nas.addSnapshot(baseSnapshot);

            targetGroup tgtGrp = new targetGroup
            {
                id = "13",
                iscsi_target = 13,
                iscsi_target_authgroup = "null",
                iscsi_target_authtype = "None",
                iscsi_target_initialdigest = "Auto",
                iscsi_target_initiatorgroup = "null",
                iscsi_target_portalgroup = "1"
            };
            nas.addTargetGroup(tgtGrp, null);
        }

        public List<mockedCall> getNASEvents()
        {
            return nas.events;
        }

        public override hypervisor makeHypervisorForVM(lockableVMSpec VM, lockableBladeSpec parentBladeSpec)
        {
            return new hypervisor_mocked_vmware(VM.spec, parentBladeSpec.spec, callMockedExecutionHandler);
        }

        public override hypervisor makeHypervisorForBlade_LTSP(lockableBladeSpec newBladeSpec)
        {
            return new hypervisor_mocked_ilo(newBladeSpec.spec, callMockedExecutionHandler);
        }

        public override hypervisor makeHypervisorForBlade_windows(lockableBladeSpec newBladeSpec)
        {
            return new hypervisor_mocked_ilo(newBladeSpec.spec, callMockedExecutionHandler);
        }

        public override hypervisor makeHypervisorForBlade_ESXi(lockableBladeSpec newBladeSpec)
        {
            return new hypervisor_mocked_ilo(newBladeSpec.spec, callMockedExecutionHandler);
        }

        protected override void waitForESXiBootToComplete(hypervisor hyp, cancellableDateTime deadline)
        {
        }

        public override void startBladePowerOff(lockableBladeSpec nodeSpec, cancellableDateTime deadline)
        {
            using (hypervisor hyp = new hypervisor_mocked_ilo(nodeSpec.spec, callMockedExecutionHandler))
            {
                hyp.powerOff(deadline);
            }
        }

        public override void startBladePowerOn(lockableBladeSpec nodeSpec, cancellableDateTime deadline)
        {
            using (hypervisor hyp = new hypervisor_mocked_ilo(nodeSpec.spec, callMockedExecutionHandler))
            {
                hyp.powerOn(deadline);
            }
        }

        public override void setCallbackOnTCPPortOpen(int nodePort, ManualResetEvent onCompletion, ManualResetEvent onError, cancellableDateTime deadline, biosThreadState biosThreadState)
        {
            //if (onTCPConnectionAttempt.Invoke(biosThreadState.nodeIP, nodePort, onBootFinish, onError, deadline, biosThreadState))
            onCompletion.Set();
            //else
            //    onError.Set();
        }

        protected override NASAccess getNasForDevice(bladeSpec vmServer)
        {
            return nas;
        }

        protected override NASParams getNASParams()
        {
            return new NASParams();
        }

        public void setExecutionResults(mockedExecutionResponses respType)
        {
            switch (respType)
            {
                case mockedExecutionResponses.successful:
                    handler = new mockedExecutionHandler_successful();
                    break;
                case mockedExecutionResponses.successfulButSlow:
                    handler = new mockedExecutionHandler_successfulButSlow();
                    break;
                default:
                    throw new ArgumentException();
            }
        }

        private executionResult callMockedExecutionHandler(hypervisor sender, string command, string args, string workingdir, cancellableDateTime deadline)
        {
            return handler.callMockedExecutionHandler(sender, command, args, workingdir, deadline);
        }
    }

    public class mockedNASWithFailure : mockedNAS
    {
        private int? idToFail = null;

        public override void deleteISCSITarget(iscsiTarget tgt)
        {
            if (!idToFail.HasValue)
                idToFail = tgt.id;

            if (tgt.id == idToFail)
            {
                throw new Exception("injected failure from rollbackSnapshot");
            }

            base.deleteISCSITarget(tgt);
        }
    }

    public enum NASFaultInjectionPolicy
    {
        retunSuccessful,
        failSnapshotDeletionOnFirstSnapshot
    }

    public class hypervisor_mocked_vmware : hypervisor_mocked_base<hypSpec_vmware>
    {
        public hypervisor_mocked_vmware(vmSpec spec, bladeSpec parentSpec, hostStateManagerMocked.mockedExecutionDelegate onMockedExecution)
            : base(null, onMockedExecution)
        {
            _spec = new hypSpec_vmware(spec.friendlyName, parentSpec.bladeIP, Settings.Default.esxiUsername, Settings.Default.esxiPassword, Settings.Default.vmUsername, Settings.Default.vmPassword, spec.currentSnapshot, null, spec.kernelDebugPort, spec.kernelDebugKey, spec.VMIP);
        }
    }

    public class hypervisor_mocked_ilo : hypervisor_mocked_base<hypSpec_iLo>
    {
        public hypervisor_mocked_ilo(bladeSpec spec, hostStateManagerMocked.mockedExecutionDelegate onMockedExecution)
            : base(null, onMockedExecution)
        {
            _spec = new hypSpec_iLo(spec.bladeIP, Settings.Default.vmUsername, Settings.Default.vmPassword, spec.iLOIP, Settings.Default.iloUsername, Settings.Default.iloPassword, null, null, null, spec.currentSnapshot, null, spec.kernelDebugPort, null);
        }
    }
}