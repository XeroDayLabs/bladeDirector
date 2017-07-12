using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using bladeDirector.Properties;
using hypervisors;

namespace bladeDirector
{
    public class hostStateManagerMocked : hostStateManager_core
    {
        public delegate executionResult mockedExecutionDelegate(hypervisor sender, string command, string args, string workingDir, DateTime deadline = default(DateTime));
        public mockedExecutionDelegate onMockedExecution;

        public delegate bool mockedConnectionAttempt(string nodeIp, int nodePort, Action<biosThreadState> onBootFinish, Action<biosThreadState> onError, DateTime deadline, biosThreadState biosThreadState);
        public mockedConnectionAttempt onTCPConnectionAttempt;

        private mockedNAS nas;

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
            nas = new mockedNAS();

            // Taken from real FreeNAS system.
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

        protected override hypervisor makeHypervisorForVM(lockableVMSpec VM, lockableBladeSpec parentBladeSpec)
        {
            return new hypervisor_mocked_vmware(VM.spec, parentBladeSpec.spec, onMockedExecution);
        }

        protected override hypervisor makeHypervisorForBlade_LTSP(lockableBladeSpec newBladeSpec)
        {
            return new hypervisor_mocked_ilo(newBladeSpec.spec, onMockedExecution);
        }

        protected override hypervisor makeHypervisorForBlade_windows(lockableBladeSpec newBladeSpec)
        {
            return new hypervisor_mocked_ilo(newBladeSpec.spec, onMockedExecution);
        }

        protected override hypervisor makeHypervisorForBlade_ESXi(lockableBladeSpec newBladeSpec)
        {
            return new hypervisor_mocked_ilo(newBladeSpec.spec, onMockedExecution);
        }

        protected override void waitForESXiBootToComplete(hypervisor hyp)
        {

        }

        public override void startBladePowerOff(lockableBladeSpec nodeSpec)
        {
            using (hypervisor hyp = new hypervisor_mocked_ilo(nodeSpec.spec, onMockedExecution))
            {
                hyp.powerOff();
            }
        }

        public override void startBladePowerOn(lockableBladeSpec nodeSpec)
        {
            using (hypervisor hyp = new hypervisor_mocked_ilo(nodeSpec.spec, onMockedExecution))
            {
                hyp.powerOn();
            }
        }

        public override void setCallbackOnTCPPortOpen(int nodePort, Action<biosThreadState> onBootFinish, Action<biosThreadState> onError, DateTime deadline, biosThreadState biosThreadState)
        {
            if (onTCPConnectionAttempt.Invoke(biosThreadState.nodeIP, nodePort, onBootFinish, onError, deadline, biosThreadState))
                onBootFinish.Invoke(biosThreadState);
            else
                onError.Invoke(biosThreadState);
        }

        protected override NASAccess getNasForDevice(bladeSpec vmServer)
        {
            return nas;
        }
    }

    public class hypervisor_mocked_vmware : hypervisor_mocked_base<hypSpec_vmware>
    {
        public hypervisor_mocked_vmware(vmSpec spec, bladeSpec parentSpec,
            hostStateManagerMocked.mockedExecutionDelegate onMockedExecution) : base(null, onMockedExecution)
        {
            _spec = new hypSpec_vmware(spec.displayName, parentSpec.bladeIP,
                Settings.Default.esxiUsername, Settings.Default.esxiPassword, Settings.Default.vmUsername,
                Settings.Default.vmPassword, spec.currentSnapshot, null,
                spec.kernelDebugPort, spec.kernelDebugKey, spec.VMIP);
        }
    }

    public class hypervisor_mocked_ilo : hypervisor_mocked_base<hypSpec_iLo>
    {
        public hypervisor_mocked_ilo(bladeSpec spec, 
            hostStateManagerMocked.mockedExecutionDelegate onMockedExecution) : base(null, onMockedExecution)
        {
            _spec = new hypSpec_iLo(spec.bladeIP, Settings.Default.vmUsername, Settings.Default.vmPassword, 
                spec.iLOIP, Settings.Default.iloUsername, Settings.Default.iloPassword,
                null, null, null,
                spec.currentSnapshot, null,
                spec.iLOPort, null);
        }
    }

    public class hypervisor_mocked_base<T> : hypervisorWithSpec<T>
    {
        protected T _spec;
        private readonly hostStateManagerMocked.mockedExecutionDelegate _onMockedExecution;

        public List<mockedCall> events = new List<mockedCall>(); 

        /// <summary>
        /// What files are have been saved to disk
        /// </summary>
        public Dictionary<string, string> files = new Dictionary<string, string>();

        private bool powerState;

        public hypervisor_mocked_base(T spec, hostStateManagerMocked.mockedExecutionDelegate onMockedExecution)
        {
            _spec = spec;
            _onMockedExecution = onMockedExecution;
        }

        public override void restoreSnapshot()
        {
            events.Add(new mockedCall("restoreSnapshot", null));
        }

        public override void connect()
        {
            events.Add(new mockedCall("connect", null));
        }

        public override void powerOn(DateTime deadline = default(DateTime))
        {
            events.Add(new mockedCall("powerOn", "deadline: " + deadline.ToString("T")));

            powerState = true;
        }

        public override void powerOff(DateTime deadline = default(DateTime))
        {
            events.Add(new mockedCall("powerOff", "deadline: " + deadline.ToString("T")));

            powerState = false;
        }

        public override void copyToGuest(string dstpath, string srcpath)
        {
            events.Add(new mockedCall("copyToGuest", "source: '" +  srcpath + "' dest: '" + dstpath + "'"));
            lock (files)
            {
                files.Add(dstpath, File.ReadAllText(srcpath));
            }
        }

        public void copyDataToGuest(string dstpath, string fileContents)
        {
            lock (files)
            {
                files.Add(dstpath, fileContents);
            }
        }

        public void copyDataToGuest(string dstpath, Byte[] fileContents)
        {
            lock (files)
            {
                files.Add(dstpath, Encoding.ASCII.GetString(fileContents));
            }
        }

        public override string getFileFromGuest(string srcpath, TimeSpan timeout = new TimeSpan())
        {
            events.Add(new mockedCall("getFileFromGuest", "timeout: " + timeout.ToString("T")));
            lock (files)
            {
                return files[srcpath];
            }
        }

        public override executionResult startExecutable(string toExecute, string args, string workingdir = null, DateTime deadline = default(DateTime))
        {
            events.Add(new mockedCall("startExecutable", "toExecute: '" +  toExecute + "' args: '" + args + "'" + " working dir: '" + (workingdir ?? "<null>") + "'"));

            return _onMockedExecution.Invoke(this, toExecute, args, workingdir, deadline);
        }

        public override IAsyncExecutionResult startExecutableAsync(string toExecute, string args, string workingDir = null)
        {
            events.Add(new mockedCall("startExecutableAsync", "toExecute: '" +  toExecute + "' args: '" + args + "'" + " working dir: '" + (workingDir ?? "<null>") + "'"));

            executionResult res = _onMockedExecution.Invoke(this, toExecute, args, workingDir);
            return new asycExcecutionResult_mocked(res);
        }

        public override IAsyncExecutionResult startExecutableAsyncWithRetry(string toExecute, string args, string workingDir = null)
        {
            events.Add(new mockedCall("startExecutableAsyncWithRetry", "toExecute: '" +  toExecute + "' args: '" + args + "'" + " working dir: '" + (workingDir ?? "<null>") + "'"));

            executionResult res = _onMockedExecution.Invoke(this, toExecute, args, workingDir);
            return new asycExcecutionResult_mocked(res);
        }

        public override void mkdir(string newDir)
        {
            events.Add(new mockedCall("mkdir", "newDir: '" +  newDir + "'"));
        }

        public override T getConnectionSpec()
        {
            return _spec;
        }

        public override bool getPowerStatus()
        {
            return powerState;
        }
    }

    public class asycExcecutionResult_mocked : IAsyncExecutionResult
    {
        private executionResult _res;

        public asycExcecutionResult_mocked(executionResult res)
        {
            _res = res;
        }

        public executionResult getResultIfComplete()
        {
            return _res;
        }

        public void Dispose()
        {

        }
    }
}