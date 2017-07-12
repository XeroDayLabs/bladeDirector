using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using bladeDirector.Properties;
using hypervisors;

namespace bladeDirector
{
    public class hostStateManager : hostStateManager_core
    {
        private class hostStateDBInProgressTCPConnect
        {
            public Socket biosUpdateSocket;
            public Action<biosThreadState> biosUpdateConnectionCallback;
            public Action<biosThreadState> biosUpdateTimeoutCallback;
            public DateTime biosUpdateDeadline;
            public biosThreadState biosCurrentThreadState;
            public IPEndPoint biosUpdateEndpoint;
        }

        private ConcurrentDictionary<string, hostStateDBInProgressTCPConnect> inProgressTCPConnects = new ConcurrentDictionary<string, hostStateDBInProgressTCPConnect>();

        public hostStateManager(string basePath) : base(basePath, new vmServerControl_ESXi(), new biosReadWrite_LTSP_iLo())
        {
            
        }

        public hostStateManager() : base(new vmServerControl_ESXi(), new biosReadWrite_LTSP_iLo())
        {

        }

        protected override hypervisor makeHypervisorForVM(lockableVMSpec vm, lockableBladeSpec parentBladeSpec)
        {
            hypSpec_vmware spec = new hypSpec_vmware(vm.spec.displayName, parentBladeSpec.spec.bladeIP,
                Settings.Default.esxiUsername, Settings.Default.esxiPassword, 
                Settings.Default.vmUsername, Settings.Default.vmPassword, null, null,
                vm.spec.kernelDebugPort, vm.spec.kernelDebugKey, vm.spec.VMIP);

            return new hypervisor_vmware(spec, clientExecutionMethod.smb);
        }

        protected override hypervisor makeHypervisorForBlade_LTSP(lockableBladeSpec bladeSpec)
        {
            hypSpec_iLo iloSpec = new hypSpec_iLo(
                        bladeSpec.spec.bladeIP, Settings.Default.ltspUsername, Settings.Default.ltspPassword,
                        bladeSpec.spec.iLOIP, Settings.Default.iloUsername, Settings.Default.iloPassword,
                        bladeSpec.spec.iscsiIP, null, null,
                        bladeSpec.spec.currentSnapshot, null,
                        bladeSpec.spec.iLOPort, null);
            return new hypervisor_iLo(iloSpec, clientExecutionMethod.SSHToBASH);
        }

        protected override hypervisor makeHypervisorForBlade_ESXi(lockableBladeSpec bladeSpec)
        {
            hypSpec_iLo iloSpec = new hypSpec_iLo(
                        bladeSpec.spec.bladeIP, Settings.Default.esxiUsername, Settings.Default.esxiPassword,
                        bladeSpec.spec.iLOIP, Settings.Default.iloUsername, Settings.Default.iloPassword, 
                        null, null, null, 
                        null, null,
                        0, null);

            return new hypervisor_iLo(iloSpec, clientExecutionMethod.SSHToBASH);
        }

        protected override hypervisor makeHypervisorForBlade_windows(lockableBladeSpec bladeSpec)
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

        public override void setCallbackOnTCPPortOpen(int port, Action<biosThreadState> onConnect, Action<biosThreadState> onError, DateTime deadline, biosThreadState state)
        {
            lock (inProgressTCPConnects)
            {
                if (inProgressTCPConnects.ContainsKey(state.nodeIP))
                    throw new Exception("operation already in progress");

                hostStateDBInProgressTCPConnect newInProg = new hostStateDBInProgressTCPConnect
                {
                    biosUpdateEndpoint = new IPEndPoint(IPAddress.Parse(state.nodeIP), port), biosUpdateConnectionCallback = onConnect, biosUpdateDeadline = deadline, biosUpdateTimeoutCallback = onError, biosCurrentThreadState = state, biosUpdateSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
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
                    // Yay, the connection is open! We can invoke the caller's callback now.
                    inProgressConnect.biosUpdateSocket = null;
                    inProgressConnect.biosUpdateConnectionCallback.Invoke(inProgressConnect.biosCurrentThreadState);

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
                if (inProgressConnect.biosUpdateTimeoutCallback != null)
                {
                    if (inProgressConnect.biosUpdateTimeoutCallback != null)
                        inProgressConnect.biosUpdateTimeoutCallback.Invoke(inProgressConnect.biosCurrentThreadState);
                }
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

        public string[] getBladesByAllocatedServer(string requestorIP)
        {
            checkKeepAlives(requestorIP);
            return db.getBladesByAllocatedServer(requestorIP);
        }

        public GetBladeStatusResult getBladeStatus(string nodeIp, string requestorIp)
        {
            checkKeepAlives(requestorIp);
            return db.getBladeStatus(nodeIp, requestorIp);
        }
    }
    
    public class VMThreadState
    {
        public DateTime deployDeadline;
        public resultAndBladeName currentProgress;
        public lockableVMSpec childVM;
        public string vmServerIP;
    }

    public class biosThreadState
    {
        public string nodeIP;
        public string biosxml;
        public bool isFinished;
        public resultCode result;
        public DateTime connectDeadline;
        public Thread rebootThread;
        public lockableBladeSpec blade;

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

    public class resultAndBladeName
    {
        public result result;
        public string bladeName;
        public waitTokenType waitToken;

        public resultAndBladeName(resultCode resultCode, string msg)
        {
            result = new result(resultCode, msg);
        }

        public resultAndBladeName(resultCode resultCode, waitTokenType newWaitToken)
        {
            waitToken = newWaitToken;
            result = new result(resultCode, null);
        }

        public resultAndBladeName(resultCode resultCode, string newMsg, waitTokenType newWaitToken)
        {
            waitToken = newWaitToken;
            result = new result(resultCode, newMsg);
        }

        public resultAndBladeName(resultCode resultCode)
        {
            result = new result(resultCode, null);
        }
    }

    public class resultCodeAndBladeName
    {
        public resultCode code;
        public string bladeName;
        public string waitToken;
    }

    public class resultCodeAndBIOSConfig
    {
        public resultCode code;
        public string BIOSConfig;

        // For XML de/ser
        // ReSharper disable once UnusedMember.Global
        public resultCodeAndBIOSConfig()
        {
        }

        public resultCodeAndBIOSConfig(resultCode newState)
        {
            code = newState;
            BIOSConfig = null;
        }

        public resultCodeAndBIOSConfig(resultCode newState, string newBIOS)
        {
            code = newState;
            BIOSConfig = newBIOS;
        }
    }

    public class result
    {
        public resultCode code;
        public string errMsg;

        public result(resultCode newCode, string newMsg)
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