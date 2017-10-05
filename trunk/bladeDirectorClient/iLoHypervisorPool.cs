using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Runtime.InteropServices;
using System.ServiceModel;
using System.ServiceModel.Channels;
using System.Threading;
using bladeDirectorClient.bladeDirectorService;
using hypervisors;

namespace bladeDirectorClient
{
    public class VMSpec
    {
        public VMHardwareSpec hw;
        public VMSoftwareSpec sw;
    }

    public class servicesWrapper : IDisposable
    {
        public ServicesClient underlying { get; private set; }

        public servicesWrapper()
            :this(machinePools.bladeDirectorURL)
        {
            
        }

        public servicesWrapper(string uri)
        {
            Binding thisBind = new WSHttpBinding();
            thisBind.OpenTimeout = TimeSpan.FromMinutes(5);
            thisBind.ReceiveTimeout = TimeSpan.FromMinutes(5);
            thisBind.SendTimeout = TimeSpan.FromMinutes(5);
            thisBind.CloseTimeout = TimeSpan.FromMinutes(5);
            EndpointAddress ep = new EndpointAddress(uri);
            underlying = new ServicesClient(thisBind, ep);
        }

        public void Dispose()
        {
            try
            {
                Debug.WriteLine("Log entries from bladeDirector:");
                foreach (string msg in underlying.getLogEvents())
                    Debug.WriteLine(msg);
            }
            catch (Exception) { }

            // FIXME: why these casts?
            try { ((IDisposable)underlying).Dispose(); } catch (CommunicationException) { }
        }
    }

    public class iLoHypervisorPool
    {
        private readonly object keepaliveThreadLock = new object();
        private Thread keepaliveThread = null;

        private bool isConnected = false;

        private object _connectionLock = new object();

        public hypervisorCollection<hypSpec_vmware> requestVMs(VMSpec[] specs)
        {
            initialiseIfNeeded();

            using(servicesWrapper director = new servicesWrapper())
            {
                resultAndBladeName[] results = new resultAndBladeName[specs.Length];
                for (int n = 0; n < specs.Length; n++)
                {
                    results[n] = director.underlying.RequestAnySingleVM(specs[n].hw, specs[n].sw);
                    if (results[n].result.code == resultCode.success ||
                        results[n].result.code == resultCode.pending)
                    {
                        continue;
                    }
                    else
                    {
                        throw new bladeAllocationException(results[n]);
                    }
                }

                hypervisorCollection<hypSpec_vmware> toRet = new hypervisorCollection<hypSpec_vmware>();
                try
                {
                    foreach (resultAndBladeName res in results)
                    {
                        resultAndBladeName progress;
                        while (true)
                        {
                            progress = (resultAndBladeName) director.underlying.getProgress(res.waitToken);

                            if (progress.result.code == resultCode.success)
                                break;
                            if (progress.result.code != resultCode.pending && 
                                progress.result.code != resultCode.unknown)
                                throw new bladeAllocationException(progress);
                        }

                        vmSpec vmSpec = director.underlying.getVMByIP_withoutLocking(progress.bladeName);
                        vmServerCredentials serverCreds = director.underlying.getCredentialsForVMServerByVMIP(progress.bladeName);
                        snapshotDetails snapshotDetails = director.underlying.getCurrentSnapshotDetails(progress.bladeName);

                        hypSpec_vmware newSpec = new hypSpec_vmware(
                            vmSpec.displayName, vmSpec.parentBladeIP, serverCreds.username, serverCreds.password,
                            vmSpec.username, vmSpec.password, snapshotDetails.friendlyName, snapshotDetails.path,
                            vmSpec.kernelDebugPort, vmSpec.kernelDebugKey, vmSpec.VMIP);

                        ensurePortIsFree(vmSpec.kernelDebugPort);

                        // FIXME: these credentials should be passed down from the bladeDirector, I think.
                        hypervisor_vmware_FreeNAS newVM = new hypervisor_vmware_FreeNAS(newSpec,
                            Properties.Settings.Default.iloISCSIIP,
                            Properties.Settings.Default.iloISCSIUsername,
                            Properties.Settings.Default.iloISCSIPassword, clientExecutionMethod.smb);

                        newVM.setDisposalCallback(onDestruction);
                        if (!toRet.TryAdd(vmSpec.VMIP, newVM))
                            throw new Exception();
                    }
                }
                catch (Exception)
                {
                    foreach (KeyValuePair<string, hypervisorWithSpec<hypSpec_vmware>> allocedBlades in toRet)
                    {
                        if (allocedBlades.Key != null)
                            director.underlying.ReleaseBladeOrVM(allocedBlades.Key);
                    }
                    throw;
                }
                return toRet;
            }
        }


        public hypervisorCollection<hypSpec_iLo> requestAsManyHypervisorsAsPossible(string snapshotName)
        {
            return requestAsManyHypervisorsAsPossible(
                Properties.Settings.Default.iloHostUsername,
                Properties.Settings.Default.iloHostPassword,
                Properties.Settings.Default.iloUsername,
                Properties.Settings.Default.iloPassword,
                Properties.Settings.Default.iloISCSIIP,
                Properties.Settings.Default.iloISCSIUsername,
                Properties.Settings.Default.iloISCSIPassword,
                Properties.Settings.Default.iloKernelKey,
                snapshotName);
        }

        public hypervisorCollection<hypSpec_iLo> requestAsManyHypervisorsAsPossible(string iloHostUsername, string iloHostPassword,
            string iloUsername, string iloPassword,
            string iloISCSIIP, string iloISCSIUsername, string iloISCSIPassword,
            string iloKernelKey, string snapshotName)
        {
            initialiseIfNeeded();
            using (servicesWrapper director = new servicesWrapper())
            {
                hypervisorCollection<hypSpec_iLo> toRet = new hypervisorCollection<hypSpec_iLo>();
                try
                {
                    while (true)
                    {
                        resultAndBladeName res = director.underlying.RequestAnySingleNode();
                        if (res.result.code == resultCode.pending)
                        {
                            resultAndWaitToken waitRes = director.underlying.getProgress(res.waitToken);
                            if (waitRes.result.code == resultCode.pending)
                            {
                                Thread.Sleep(TimeSpan.FromSeconds(3));
                                continue;
                            }
                            if (waitRes.result.code != resultCode.success)
                                throw new bladeAllocationException(res);
                        }
                        else if (res.result.code == resultCode.bladeQueueFull)
                        {
                            break;
                        }
                        else if (res.result.code != resultCode.success)
                        {
                            throw new bladeAllocationException(res);
                        }

                        bladeSpec bladeConfig = director.underlying.getBladeByIP_withoutLocking(res.bladeName);
                        resultAndWaitToken selectRes = director.underlying.selectSnapshotForBladeOrVM(res.bladeName, snapshotName);
                        if (selectRes.result.code == resultCode.pending)
                        {
                            resultAndWaitToken waitRes = director.underlying.getProgress(selectRes.waitToken);
                            if (waitRes.result.code == resultCode.pending)
                            {
                                Thread.Sleep(TimeSpan.FromSeconds(5));
                                continue;
                            }
                            selectRes.result = waitRes.result;
                        }

                        if (selectRes.result.code != resultCode.success)
                        {
                            throw new bladeAllocationException(selectRes);
                        }

                        if (selectRes.result.code != resultCode.success)
                            throw new bladeAllocationException(selectRes);

                        snapshotDetails snapshotDetails = director.underlying.getCurrentSnapshotDetails(res.bladeName);

                        hypSpec_iLo spec = new hypSpec_iLo(
                            bladeConfig.bladeIP, iloHostUsername, iloHostPassword,
                            bladeConfig.iLOIP, iloUsername, iloPassword,
                            iloISCSIIP, iloISCSIUsername, iloISCSIPassword,
                            snapshotDetails.friendlyName, snapshotDetails.path, bladeConfig.iLOPort, iloKernelKey
                            );

                        ensurePortIsFree(bladeConfig.iLOPort);

                        bladeDirectedHypervisor_iLo newHyp = new bladeDirectedHypervisor_iLo(spec);
                        newHyp.setDisposalCallback(onDestruction);

                        NASAccess nas = new FreeNAS(spec);
                        freeNASSnapshot.getSnapshotObjectsFromNAS(nas, spec.snapshotFullName);

                        if (!toRet.TryAdd(bladeConfig.bladeIP, newHyp))
                            throw new Exception();
                    }
                    return toRet;
                }
                catch (Exception)
                {
                    toRet.Dispose();
                    throw;
                }
            }
        }

        [DllImport("iphlpapi.dll", SetLastError = true)]
        private static extern int GetExtendedUdpTable(IntPtr pUdpTable, out int dwOutBufLen, bool sort, int ipVersion, int tblClass, int reserved);

        private const int UDP_TABLE_OWNER_PID = 1;

        public static void ensurePortIsFree(UInt16 port)
        {
            MIB_UDPTABLE_OWNER_PID[] tableContents = getCurrentUDPListeners();
            MIB_UDPTABLE_OWNER_PID portUse = tableContents.SingleOrDefault(x => x.localPort == port);
            if (portUse == null)
                return;

            using (Process p = Process.GetProcessById((int) portUse.ownerPID))
            {
                p.Kill();
                p.WaitForExit((int) TimeSpan.FromSeconds(5).TotalMilliseconds);
                if (!p.HasExited)
                    throw new Exception("Unable to kill PID " + portUse.ownerPID + " which is using needed port " + portUse.localPort);
            }
        }

        private static MIB_UDPTABLE_OWNER_PID[] getCurrentUDPListeners()
        {
            int buflen;
            MIB_UDPTABLE_OWNER_PID[] tableContents;
            int res = GetExtendedUdpTable(IntPtr.Zero, out buflen, false, 2, UDP_TABLE_OWNER_PID, 0);
            if (res != 122) // error_more_data
                throw new Win32Exception(res, "GetExtendedUdpTable (first) failed");
            IntPtr tableBuffer = Marshal.AllocHGlobal(buflen);
            try
            {
                res = GetExtendedUdpTable(tableBuffer, out buflen, false, 2, UDP_TABLE_OWNER_PID, 0);
                if (res != 0)
                    throw new Win32Exception(res, "GetExtendedUdpTable (second) failed");
                UDP_TABLE_CLASS table = (UDP_TABLE_CLASS) Marshal.PtrToStructure(tableBuffer, typeof (UDP_TABLE_CLASS));
                tableContents = new MIB_UDPTABLE_OWNER_PID[(buflen - Marshal.SizeOf(typeof (UDP_TABLE_CLASS)))/Marshal.SizeOf(typeof (MIB_UDPTABLE_OWNER_PID))];

                for (int rowIndex = 0; rowIndex < tableContents.Length; rowIndex++)
                {
                    IntPtr pos = tableBuffer +
                                 Marshal.SizeOf(typeof (UDP_TABLE_CLASS)) +
                                 ((Marshal.SizeOf(typeof (MIB_UDPTABLE_OWNER_PID)))*rowIndex);
                    MIB_UDPTABLE_OWNER_PID row = (MIB_UDPTABLE_OWNER_PID) Marshal.PtrToStructure(pos, typeof (MIB_UDPTABLE_OWNER_PID));

                    row.localPort = byteswap((UInt16) row.localPort);

                    tableContents[rowIndex] = row;
                }
            }
            finally
            {
                Marshal.FreeHGlobal(tableBuffer);
            }

            return tableContents;
        }

        private static UInt16 byteswap(UInt16 localPort)
        {
            UInt16 toRet = 0;

            toRet |= (UInt16) ((localPort & 0x00ff) << 8);
            toRet |= (UInt16) ((localPort & 0xff00) >> 8);

            return toRet;
        }

        public bladeDirectedHypervisor_iLo createSingleHypervisor(string snapshotName)
        {
            return createSingleHypervisor(
                Properties.Settings.Default.iloHostUsername,
                Properties.Settings.Default.iloHostPassword,
                Properties.Settings.Default.iloUsername,
                Properties.Settings.Default.iloPassword,
                Properties.Settings.Default.iloISCSIIP,
                Properties.Settings.Default.iloISCSIUsername,
                Properties.Settings.Default.iloISCSIPassword,
                Properties.Settings.Default.iloKernelKey,
                snapshotName);
        }

        public bladeDirectedHypervisor_iLo createSingleHypervisor(string iloHostUsername, string iloHostPassword,
            string iloUsername, string iloPassword,
            string iloISCSIIP, string iloISCSIUsername, string iloISCSIPassword,
            string iloKernelKey,
            string snapshotName)
        {
            initialiseIfNeeded();

            // We request a blade from the blade director, and use them for all our tests, blocking if none are available.
            using (servicesWrapper director = new servicesWrapper())
            {
                // Request a node. If all queues are full, then wait and retry until we get one.
                resultAndBladeName allocatedBladeResult;
                while (true)
                {
                    allocatedBladeResult = director.underlying.RequestAnySingleNode();
                    if (allocatedBladeResult.result.code == resultCode.success ||
                        allocatedBladeResult.result.code == resultCode.pending)
                        break;
                    if (allocatedBladeResult.result.code == resultCode.bladeQueueFull)
                    {
                        Debug.WriteLine("All blades are fully queued, waiting until one is spare");
                        Thread.Sleep(TimeSpan.FromSeconds(5));
                        continue;
                    }
                    throw new bladeAllocationException(allocatedBladeResult);
                }

                // Now, wait until our blade is available.
                try
                {
                    bool isMine = director.underlying.isBladeMine(allocatedBladeResult.bladeName);
                    while (!isMine)
                    {
                        Debug.WriteLine("Blade " + allocatedBladeResult.bladeName + " not released yet, awaiting release..");
                        Thread.Sleep(TimeSpan.FromSeconds(5));
                        isMine = director.underlying.isBladeMine(allocatedBladeResult.bladeName);
                    }

                    // Great, now we have ownership of the blade, so we can use it safely.
                    bladeSpec bladeConfig = director.underlying.getBladeByIP_withoutLocking(allocatedBladeResult.bladeName);
                    resultAndWaitToken res = director.underlying.selectSnapshotForBladeOrVM(allocatedBladeResult.bladeName, snapshotName);
                    while (res.result.code == resultCode.pending)
                    {
                        res = director.underlying.getProgress(allocatedBladeResult.waitToken);
                        Thread.Sleep(TimeSpan.FromSeconds(5));
                    }
                    if (res.result.code != resultCode.success)
                        throw new Exception("Can't find snapshot " + snapshotName);

                    snapshotDetails snapshotDetails = director.underlying.getCurrentSnapshotDetails(allocatedBladeResult.bladeName);

                    hypSpec_iLo spec = new hypSpec_iLo(
                        bladeConfig.bladeIP, iloHostUsername, iloHostPassword,
                        bladeConfig.iLOIP, iloUsername, iloPassword,
                        iloISCSIIP, iloISCSIUsername, iloISCSIPassword,
                        snapshotDetails.friendlyName, snapshotDetails.path, bladeConfig.iLOPort, iloKernelKey
                        );

                    ensurePortIsFree(bladeConfig.iLOPort);

                    NASAccess nas = new FreeNAS(spec);
                    freeNASSnapshot.getSnapshotObjectsFromNAS(nas, spec.snapshotFullName);

                    bladeDirectedHypervisor_iLo toRet = new bladeDirectedHypervisor_iLo(spec);
                    toRet.setDisposalCallback(onDestruction);
                    return toRet;
                }
                catch (Exception)
                {
                    director.underlying.ReleaseBladeOrVM(allocatedBladeResult.bladeName);
                    throw;
                }
            }
        }

        private void initialiseIfNeeded()
        {
            if (keepaliveThread == null)
            {
                lock (keepaliveThreadLock)
                {
                    if (keepaliveThread == null)
                    {
                        using (servicesWrapper director = new servicesWrapper())
                        {
                            resultAndWaitToken logInStatus = director.underlying.logIn();
                            if (logInStatus.result.code == resultCode.pending)
                            {
                                DateTime deadline = DateTime.Now + TimeSpan.FromMinutes(5);
                                while (true)
                                {
                                    resultAndWaitToken res = director.underlying.getProgress(logInStatus.waitToken);
                                    if (res.result.code != resultCode.pending &&
                                        res.result.code != resultCode.unknown)
                                    {
                                        if (res.result.code != resultCode.success)
                                            throw new bladeAllocationException(res);
                                        break;
                                    }
                                    if (DateTime.Now > deadline)
                                        throw new TimeoutException();
                                }
                            }
                            else if (logInStatus.result.code != resultCode.success)
                                throw new bladeAllocationException(logInStatus);
                        }

                        keepaliveThread = new Thread(keepaliveThreadMain);
                        keepaliveThread.Name = "Blade director keepalive thread";
                        keepaliveThread.Start();
                    }
                }
            }

            if (!isConnected)
            {
                lock (_connectionLock)
                {
                    isConnected = true;
                }
            }
        }

        private void keepaliveThreadMain()
        {
            using (servicesWrapper director = new servicesWrapper())
            {
                while (true)
                {
                    Thread.Sleep(TimeSpan.FromSeconds(3));
                    try
                    {
                        director.underlying.keepAlive();
                    }
                    catch (TimeoutException)
                    {
                    }
                }
            }
        }

        private void onDestruction(hypSpec_iLo obj)
        {
            // Notify the director that this blade is no longer in use.
            using (servicesWrapper director = new servicesWrapper())
            {
                director.underlying.ReleaseBladeOrVM(obj.kernelDebugIPOrHostname);
            }
        }

        private void onDestruction(hypSpec_vmware obj)
        {
            using (servicesWrapper director = new servicesWrapper())
            {
                director.underlying.ReleaseBladeOrVM(obj.kernelDebugIPOrHostname);
            }
        }
    }

    public class bladeAllocationException : Exception
    {
        private readonly resultCode _code;
        private readonly string _msg;

        public bladeAllocationException(resultCode code)
        {
            _code = code;
            _msg = "(none)";
        }

        public bladeAllocationException(resultAndBladeName res)
        {
            _code = res.result.code;
            _msg = res.result.errMsg;
        }

        public bladeAllocationException(resultAndWaitToken res)
        {
            _code = res.result.code;
            _msg = res.result.errMsg;
        }

        public override string ToString()
        {
            return "Blade allocation service returned code " + _code + " with message '" + _msg + "'";
        }

        public override string Message
        {
            get { return this.ToString(); }
        }
    }

    [StructLayout(LayoutKind.Sequential, Pack = 0)]
    public class UDP_TABLE_CLASS
    {
        public UInt32 numberOfEntries;
    }

    [StructLayout(LayoutKind.Sequential, Pack = 0)]
    public class MIB_UDPTABLE_OWNER_PID
    {
        public UInt32 localAddr;
        public UInt32 localPort;
        public UInt32 ownerPID;
    }

    public class bladeDirectedHypervisor_iLo : hypervisor_iLo
    {
        public bladeDirectedHypervisor_iLo(hypSpec_iLo spec)
            : base(spec)
        {
        }

        public void setBIOSConfig(string newBiosXML)
        {
            hypSpec_iLo spec = getConnectionSpec();
            using (servicesWrapper director = new servicesWrapper())
            {
                resultAndWaitToken res = director.underlying.rebootAndStartDeployingBIOSToBlade(spec.kernelDebugIPOrHostname, newBiosXML);
                if (res.result.code == resultCode.noNeedLah)
                    return;

                if (res.result.code != resultCode.pending)
                    throw new Exception("Failed to start BIOS write to " + spec.kernelDebugIPOrHostname + ", error " + res);

                waitToken waitOn = res.waitToken;
                do
                {
                    res = director.underlying.getProgress(waitOn);
                    Thread.Sleep(TimeSpan.FromSeconds(5));
                } while (res.result.code == resultCode.pending);

                if (res.result.code != resultCode.success &&
                    res.result.code != resultCode.noNeedLah)
                    throw new Exception("Failed to write bios to " + spec.kernelDebugIPOrHostname + ", error " + res);
            }
        }
    }
}