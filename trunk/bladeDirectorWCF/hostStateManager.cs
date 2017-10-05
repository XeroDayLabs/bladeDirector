using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using bladeDirectorWCF.Properties;
using hypervisors;
using Microsoft.SqlServer.Server;

namespace bladeDirectorWCF
{
    public class hostStateManager : hostStateManager_core
    {
        private class hostStateDBInProgressTCPConnect
        {
            public Socket biosUpdateSocket;
            public ManualResetEvent biosUpdateConnectionEvent;
            public ManualResetEvent biosUpdateTimeoutEvent;
            public DateTime biosUpdateDeadline;
            public biosThreadState biosCurrentThreadState;
            public IPEndPoint biosUpdateEndpoint;
        }

        private ConcurrentDictionary<string, hostStateDBInProgressTCPConnect> inProgressTCPConnects = new ConcurrentDictionary<string, hostStateDBInProgressTCPConnect>();

        public hostStateManager(string basePath)
            : base(basePath, new vmServerControl_ESXi(), new biosReadWrite_LTSP_iLo())
        {

        }

        public hostStateManager()
            : base(new vmServerControl_ESXi(), new biosReadWrite_LTSP_iLo())
        {
        }

        public override hypervisor makeHypervisorForVM(lockableVMSpec vm, lockableBladeSpec parentBladeSpec)
        {
            hypSpec_vmware spec = new hypSpec_vmware(vm.spec.displayName, parentBladeSpec.spec.bladeIP,
                Settings.Default.esxiUsername, Settings.Default.esxiPassword,
                Settings.Default.vmUsername, Settings.Default.vmPassword, null, null,
                vm.spec.kernelDebugPort, vm.spec.kernelDebugKey, vm.spec.VMIP);

            return new hypervisor_vmware(spec, clientExecutionMethod.smb);
        }

        public override hypervisor makeHypervisorForBlade_LTSP(lockableBladeSpec bladeSpec)
        {
            hypSpec_iLo iloSpec = new hypSpec_iLo(
                bladeSpec.spec.bladeIP, Settings.Default.ltspUsername, Settings.Default.ltspPassword,
                bladeSpec.spec.iLOIP, Settings.Default.iloUsername, Settings.Default.iloPassword,
                bladeSpec.spec.iscsiIP, null, null,
                bladeSpec.spec.currentSnapshot, null,
                bladeSpec.spec.iLOPort, null);
            return new hypervisor_iLo(iloSpec, clientExecutionMethod.SSHToBASH);
        }

        public override hypervisor makeHypervisorForBlade_ESXi(lockableBladeSpec bladeSpec)
        {
            hypSpec_iLo iloSpec = new hypSpec_iLo(
                bladeSpec.spec.bladeIP, Settings.Default.esxiUsername, Settings.Default.esxiPassword,
                bladeSpec.spec.iLOIP, Settings.Default.iloUsername, Settings.Default.iloPassword,
                null, null, null,
                null, null,
                0, null);

            return new hypervisor_iLo(iloSpec, clientExecutionMethod.SSHToBASH);
        }

        public override hypervisor makeHypervisorForBlade_windows(lockableBladeSpec bladeSpec)
        {
            hypSpec_iLo iloSpec = new hypSpec_iLo(
                bladeSpec.spec.bladeIP, Settings.Default.vmUsername, Settings.Default.vmPassword,
                bladeSpec.spec.iLOIP, Settings.Default.iloUsername, Settings.Default.iloPassword,
                bladeSpec.spec.iscsiIP, null, null,
                bladeSpec.spec.currentSnapshot, null,
                bladeSpec.spec.iLOPort, null);

            return new hypervisor_iLo(iloSpec, clientExecutionMethod.smb);
        }

        public override void startBladePowerOff(lockableBladeSpec nodeSpec)
        {
            DateTime deadline = DateTime.Now + TimeSpan.FromMinutes(3);

            using (hypervisor_iLo_HTTP hyp = new hypervisor_iLo_HTTP(nodeSpec.spec.iLOIP, Settings.Default.iloUsername, Settings.Default.iloPassword))
            {
                hyp.connect();
                while (true)
                {
                    hyp.powerOff();
                    if (hyp.getPowerStatus() == false)
                        break;
                    if (DateTime.Now > deadline)
                        throw new TimeoutException();
                    Thread.Sleep(TimeSpan.FromSeconds(5));
                }
            }
        }

        public override void startBladePowerOn(lockableBladeSpec nodeSpec)
        {
            DateTime deadline = DateTime.Now + TimeSpan.FromMinutes(3); // This should be high enough to allow for an in-progress reset

            using (hypervisor_iLo_HTTP hyp = new hypervisor_iLo_HTTP(nodeSpec.spec.iLOIP, Settings.Default.iloUsername, Settings.Default.iloPassword))
            {
                hyp.connect();
                while (true)
                {
                    hyp.powerOn();
                    if (hyp.getPowerStatus())
                        break;
                    if (DateTime.Now > deadline)
                        throw new TimeoutException();
                    Thread.Sleep(TimeSpan.FromSeconds(5));
                }
            }
        }

        public override void setCallbackOnTCPPortOpen(int port, ManualResetEvent onCompletion, ManualResetEvent onError, DateTime deadline, biosThreadState state)
        {
            lock (inProgressTCPConnects)
            {
                if (inProgressTCPConnects.ContainsKey(state.nodeIP))
                    throw new Exception("operation already in progress");

                hostStateDBInProgressTCPConnect newInProg = new hostStateDBInProgressTCPConnect
                {
                    biosUpdateEndpoint = new IPEndPoint(IPAddress.Parse(state.nodeIP), port), biosUpdateConnectionEvent = onCompletion, biosUpdateDeadline = deadline, biosUpdateTimeoutEvent = onError, biosCurrentThreadState = state, biosUpdateSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
                };

                inProgressTCPConnects.GetOrAdd(state.nodeIP, newInProg);

                newInProg.biosUpdateSocket.BeginConnect(newInProg.biosUpdateEndpoint, TCPCallback, newInProg);
            }
        }

        protected override NASAccess getNasForDevice(bladeSpec vmServer)
        {
            return new FreeNAS(Settings.Default.iscsiServerIP, Settings.Default.iscsiServerUsername, Settings.Default.iscsiServerPassword);
        }

        private void TCPCallback(IAsyncResult ar)
        {
            hostStateDBInProgressTCPConnect inProgressConnect = (hostStateDBInProgressTCPConnect) ar.AsyncState;
            try
            {
                inProgressConnect.biosUpdateSocket.EndConnect(ar);
                if (inProgressConnect.biosUpdateSocket.Connected)
                {
                    // Yay, the connection is open! We can tell the caller now.
                    inProgressConnect.biosUpdateSocket = null;
                    inProgressConnect.biosUpdateConnectionEvent.Set();

                    hostStateDBInProgressTCPConnect val;
                    inProgressTCPConnects.TryRemove(inProgressConnect.biosCurrentThreadState.nodeIP, out val);

                    return;
                }
            }
            catch (SocketException)
            {
            }

            // We failed to connect, either because .EndConnect threw, or because the socket was not connected. Report failure 
            // (if our timeout has expired), or start another connection attempt if it has not.
            if (DateTime.Now > inProgressConnect.biosUpdateDeadline)
            {
                inProgressConnect.biosUpdateSocket = null;
                inProgressConnect.biosUpdateTimeoutEvent.Set();
            }

            // Otherwise, queue up another connect attempt to just keep retrying.
            inProgressConnect.biosUpdateSocket.BeginConnect(inProgressConnect.biosUpdateEndpoint, TCPCallback, inProgressConnect);
        }

        protected override void waitForESXiBootToComplete(hypervisor hyp)
        {
            while (true)
            {
                executionResult res = hypervisor.doWithRetryOnSomeExceptions(() => hyp.startExecutable("/etc/init.d/vpxa", "status"), TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(10));

                if (res.resultCode != 0)
                {
                    Thread.Sleep(TimeSpan.FromSeconds(4));
                    continue;
                }

                if (res.stdout.Contains("vpxa is running"))
                    return;
            }
        }
    }

    
    public class VMThreadState
    {
        public DateTime deployDeadline;
        public resultAndBladeName currentProgress;
        public string vmServerIP;
        public string childVMIP;
    }

    public class biosThreadState
    {
        public string nodeIP;
        public string biosxml;
        public bool isFinished;
        public bool isStarted;
        public result result;
        public DateTime connectDeadline;
        public Thread rebootThread;
        public lockableBladeSpec blade;

        public ManualResetEvent onBootFinishEvent = new ManualResetEvent(false);
        public ManualResetEvent onBootFailureEvent = new ManualResetEvent(false);

        public Action<biosThreadState> onBootFinish;
        public Action<biosThreadState> onBootFailure;

        public biosThreadState(string newNodeIP, string newBIOSXML, string bladeIP)
        {
            nodeIP = newNodeIP;
            biosxml = newBIOSXML;
            isFinished = false;
            nodeIP = bladeIP;
        }
    }

    public enum GetBladeStatusResult
    {
        bladeNotFound,
        unused,
        yours,
        releasePending,
        notYours
    }

    public enum bladeStatus
    {
        unused,
        releaseRequested,
        inUseByDirector,
        inUse
    }

    public class resultAndWaitToken
    {
        public result result;
        public waitToken waitToken;

        public resultAndWaitToken(resultCode newResult, waitToken newWaitToken, string msg)
        {
            result = new result(newResult, msg);
            waitToken = newWaitToken;
        }

        public resultAndWaitToken(result newResult, waitToken newWaitToken)
        {
            result = newResult;
            waitToken = newWaitToken;
        }

        public resultAndWaitToken()
        {
            // for xml de/ser
        }

        public resultAndWaitToken(resultCode resultCode, waitToken newWaitToken)
        {
            result = new result(resultCode);
            waitToken = newWaitToken;
        }

        public resultAndWaitToken(resultCode resultCode)
        {
            result = new result(resultCode, "none provided @ " + Environment.StackTrace);
            waitToken = null;
        }

        public resultAndWaitToken(resultCode resultCode, string msg)
        {
            result = new result(resultCode, msg);
            waitToken = null;
        }
    }

    public class resultAndBladeName : resultAndWaitToken
    {
        public string bladeName;

        public resultAndBladeName()
            : base()
        {
            // For XML de/ser
        }

        public resultAndBladeName(resultCode result, waitToken newWaitToken, string msg)
            : base(result, newWaitToken, msg)
        {
        }

        public resultAndBladeName(result result, waitToken newWaitToken)
            : base(result, newWaitToken)
        {
        }

        public resultAndBladeName(result result)
            : base(result, null)
        {
        }
    }
    
    public class resultAndBIOSConfig : resultAndWaitToken
    {
        public string BIOSConfig;

        // For XML de/ser
        // ReSharper disable once UnusedMember.Global
        public resultAndBIOSConfig()
        {
        }

        public resultAndBIOSConfig(result newState, waitToken newWaitToken)
            : base(newState, newWaitToken)
        {
            BIOSConfig = null;
        }

        public resultAndBIOSConfig(result newState, waitToken newWaitToken, string newBIOS)
            : base(newState, newWaitToken)
        {
            BIOSConfig = newBIOS;
        }
    }

    public class result
    {
        public resultCode code;
        public string errMsg;

        public result()
        {
            // For XML de/ser
        }

        public result(resultCode newCode, string newMsg = null)
        {
            code = newCode;
            errMsg = newMsg;
        }

        public override string ToString()
        {
            return "result code is " + code + "; msg '" + errMsg + "'";
        }
    }

    public enum resultCode
    {
        success,
        bladeNotFound,
        bladeInUse,
        bladeQueueFull,
        pending,
        alreadyInProgress,
        cancelled,
        genericFail,
        noNeedLah,
        unknown
    }
}