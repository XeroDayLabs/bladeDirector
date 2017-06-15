using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Web;
using System.Web.Services.Description;
using System.Web.UI.WebControls;
using bladeDirector.Properties;
using hypervisors;
using Renci.SshNet;

namespace bladeDirector
{
    public class hostStateDB : hostStateDB_core
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

        public hostStateDB(string basePath) : base(basePath, new vmServerControl_ESXi())
        {
            
        }

        public hostStateDB() : base(new vmServerControl_ESXi())
        {

        }

        protected override hypervisor makeHypervisorForVM(vmSpec VM, bladeSpec parentBladeSpec)
        {
            hypSpec_vmware spec = new hypSpec_vmware(VM.displayName, parentBladeSpec.bladeIP,
                Settings.Default.esxiUsername, Properties.Settings.Default.esxiPassword, Settings.Default.vmUsername,
                Settings.Default.vmPassword, null, null,
                0, "", VM.VMIP);

            return new hypervisor_vmware(spec, clientExecutionMethod.smb);
        }

        protected override hypervisor makeHypervisorForBlade_LTSP(bladeSpec bladeSpec)
        {
            hypSpec_iLo iloSpec = new hypSpec_iLo(
                        bladeSpec.bladeIP, Settings.Default.ltspUsername, Settings.Default.ltspPassword,
                        bladeSpec.iLOIP, Settings.Default.iloUsername, Settings.Default.iloPassword,
                        bladeSpec.iscsiIP, null, null,
                        bladeSpec.currentSnapshot, null,
                        bladeSpec.iLOPort, null);
            return new hypervisor_iLo(iloSpec, clientExecutionMethod.SSHToBASH);
        }

        protected override hypervisor makeHypervisorForBlade_ESXi(bladeSpec bladeSpec)
        {
            hypSpec_iLo iloSpec = new hypSpec_iLo(
                        bladeSpec.bladeIP, Settings.Default.esxiUsername, Settings.Default.esxiPassword, 
                        bladeSpec.iLOIP, Settings.Default.iloUsername, Settings.Default.iloPassword, 
                        null, null, null, 
                        null, null,
                        0, null);

            return new hypervisor_iLo(iloSpec, clientExecutionMethod.SSHToBASH);
        }

        protected override hypervisor makeHypervisorForBlade_windows(bladeSpec bladeSpec)
        {
            hypSpec_iLo iloSpec = new hypSpec_iLo(
                        bladeSpec.bladeIP, Settings.Default.vmUsername, Settings.Default.vmPassword,
                        bladeSpec.iLOIP, Settings.Default.iloUsername, Settings.Default.iloPassword,
                        bladeSpec.iscsiIP, null, null,
                        bladeSpec.currentSnapshot, null,
                        bladeSpec.iLOPort, null);

            return new hypervisor_iLo(iloSpec, clientExecutionMethod.smb);
        }

        protected override void startBladePowerOff(bladeSpec nodeSpec, string iLoIp)
        {
            DateTime deadline = DateTime.Now + TimeSpan.FromMinutes(1);

            using (hypervisor_iLo_HTTP hyp = new hypervisor_iLo_HTTP(iLoIp, Settings.Default.iloUsername, Settings.Default.iloPassword))
            {
                hyp.connect();
                while (true)
                {
                    hyp.powerOff();
                    if (hyp.getPowerStatus() == false)
                        break;
                    if (DateTime.Now > deadline)
                        throw new TimeoutException();
                    Thread.Sleep(TimeSpan.FromSeconds(3));
                }
            }
        }

        protected override void startBladePowerOn(bladeSpec nodeSpec, string iLoIp)
        {
            DateTime deadline = DateTime.Now + TimeSpan.FromMinutes(1);

            using (hypervisor_iLo_HTTP hyp = new hypervisor_iLo_HTTP(iLoIp, Settings.Default.iloUsername, Settings.Default.iloPassword))
            {
                hyp.connect();
                while (true)
                {
                    hyp.powerOn();
                    if (hyp.getPowerStatus() == true)
                        break;
                    if (DateTime.Now > deadline)
                        throw new TimeoutException();
                    Thread.Sleep(TimeSpan.FromSeconds(3));
                }
            }
        }

        protected override void setCallbackOnTCPPortOpen(int port, Action<biosThreadState> onConnect, Action<biosThreadState> onError, DateTime deadline, biosThreadState state)
        {
            lock (inProgressTCPConnects)
            {
                if (inProgressTCPConnects.ContainsKey(state.nodeSpec.bladeIP))
                    throw new Exception("operation already in progress");

                hostStateDBInProgressTCPConnect newInProg = new hostStateDBInProgressTCPConnect
                {
                    biosUpdateEndpoint = new IPEndPoint(IPAddress.Parse(state.nodeSpec.bladeIP), port), biosUpdateConnectionCallback = onConnect, biosUpdateDeadline = deadline, biosUpdateTimeoutCallback = onError, biosCurrentThreadState = state, biosUpdateSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
                };

                inProgressTCPConnects.GetOrAdd(state.nodeSpec.bladeIP, newInProg);

                newInProg.biosUpdateSocket.BeginConnect(newInProg.biosUpdateEndpoint, TCPCallback, newInProg);
            }
        }

        protected override NASAccess getNasForDevice(bladeSpec vmServer)
        {
            return new FreeNAS(Settings.Default.iscsiServerIP, Settings.Default.iscsiServerUsername, Settings.Default.iscsiServerPassword);
        }

        protected override void copyFilesToBlade(string nodeIp, Dictionary<string, string> toWriteText, Dictionary<string, byte[]> toWriteBinary)
        {
            using (SftpClient scp = new SftpClient(nodeIp, Settings.Default.ltspUsername, Settings.Default.ltspPassword))
            {
                scp.Connect();
                foreach (KeyValuePair<string, string> kvp in toWriteText)
                    scp.WriteAllText(kvp.Key, kvp.Value);
                foreach (KeyValuePair<string, byte[]> kvp in toWriteBinary)
                    scp.WriteAllBytes(kvp.Key, kvp.Value);
            }
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
                    inProgressTCPConnects.TryRemove(inProgressConnect.biosCurrentThreadState.nodeSpec.bladeIP, out val);

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
    }

    public class bladeState
    {
        public bool isPoweredUp;
    }

    public class VMThreadState
    {
        public bladeSpec VMServer;
        public DateTime deployDeadline;
        public resultCodeAndBladeName currentProgress;
        public vmSpec childVM;
        public VMSoftwareSpec swSpec;
    }

    public class biosThreadState
    {
        public bladeSpec nodeSpec;
        public string biosxml;
        public bool isFinished;
        public resultCode result;
        public DateTime connectDeadline;
        public Thread rebootThread;

        public Action<biosThreadState> onBootFinish;
        public Action<biosThreadState> onBootFailure;

        public biosThreadState(bladeSpec newNodeSpec, string newBIOSXML)
        {
            nodeSpec = newNodeSpec;
            biosxml = newBIOSXML;
            isFinished = false;
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
            this.code = newState;
            this.BIOSConfig = null;
        }

        public resultCodeAndBIOSConfig(biosThreadState state)
        {
            this.code = state.result;
            this.BIOSConfig = state.biosxml;
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
        genericFail,
        noNeedLah,
        unknown
    }
}