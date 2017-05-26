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
using bladeDirectorClient.bladeDirector;
using hypervisors;

namespace bladeDirectorClient
{
    public class VMSpec
    {
        public VMHardwareSpec hw;
        public VMSoftwareSpec sw;
    }

    public class iLoHypervisorPool
    {
        private readonly object keepaliveThreadLock = new object();
        private Thread keepaliveThread = null;

        public hypervisorCollection<hypSpec_vmware> requestVMs(VMSpec[] specs)
        {
            startKeepaliveThreadIfNotRunning();

            Binding thisBind = new BasicHttpBinding("servicesSoap");
            thisBind.OpenTimeout = TimeSpan.FromMinutes(5);
            thisBind.ReceiveTimeout = TimeSpan.FromMinutes(5);
            thisBind.SendTimeout = TimeSpan.FromMinutes(5);
            thisBind.CloseTimeout = TimeSpan.FromMinutes(5);
            EndpointAddress ep = new EndpointAddress(machinePools.bladeDirectorURL);
            using (servicesSoapClient director = new servicesSoapClient(thisBind, ep))
            {
                resultCodeAndBladeName[] results = new resultCodeAndBladeName[specs.Length];
                for (int n = 0; n < specs.Length; n++)
                {
                    results[n] = director.RequestAnySingleVM(specs[n].hw, specs[n].sw);
                    if (results[n].code == resultCode.success ||
                        results[n].code == resultCode.pending)
                    {
                        continue;
                    }
                    else 
                    {
                        throw new bladeAllocationException(results[n].code);
                    }
                }

                hypervisorCollection<hypSpec_vmware> toRet = new hypervisorCollection<hypSpec_vmware>();
                try
                {
                    foreach (resultCodeAndBladeName res in results)
                    {
                        resultCodeAndBladeName progress;
                        while (true)
                        {
                            progress = director.getProgressOfVMRequest(res.waitToken);

                            if (progress.code == resultCode.success)
                                break;
                            if (progress.code != resultCode.pending && progress.code != resultCode.unknown)
                                throw new bladeAllocationException(progress.code);
                        }

                        vmSpec vmSpec = director.getConfigurationOfVM(progress.bladeName);
                        bladeSpec vmServerSpec = director.getConfigurationOfBladeByID((int) vmSpec.parentBladeID);
                        hypSpec_vmware newSpec = new hypSpec_vmware(
                            vmSpec.displayName, vmServerSpec.bladeIP, vmServerSpec.ESXiUsername, vmServerSpec.ESXiPassword,
                            vmSpec.username, vmSpec.password, vmSpec.kernelDebugPort, vmSpec.kernelDebugKey, vmSpec.VMIP);
                        newSpec.snapshotName = director.getCurrentSnapshotForBlade(vmSpec.VMIP);
                        hypervisor_vmware newVM = new hypervisor_vmware(newSpec);

                        ensurePortIsFree(vmSpec.kernelDebugPort);

                        // FIXME: these credentials should be passed down from the bladeDirector, I think.
                        newVM.configureForFreeNASSnapshots(
                            Properties.Settings.Default.iloISCSIIP, Properties.Settings.Default.iloISCSIUsername, Properties.Settings.Default.iloISCSIPassword );

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
                            director.releaseBladeOrVM(allocedBlades.Key);
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
            startKeepaliveThreadIfNotRunning();
            using (servicesSoapClient director = new servicesSoapClient("servicesSoap", machinePools.bladeDirectorURL))
            {
                string[] nodes = director.ListNodes().Split(',');

                hypervisorCollection<hypSpec_iLo> toRet = new hypervisorCollection<hypSpec_iLo>();
                try
                {
                    foreach (string nodeName in nodes)
                    {
                        resultCode res = director.RequestNode(nodeName);

                        bladeSpec bladeConfig = director.getConfigurationOfBlade(nodeName);
                        res = director.selectSnapshotForBladeOrVM(nodeName, snapshotName);
                        while (res == resultCode.pending)
                        {
                            res = director.selectSnapshotForBladeOrVM_getProgress(nodeName);
                            Thread.Sleep(TimeSpan.FromSeconds(5));
                        }
                        if (res != resultCode.success)
                            throw new Exception("Can't find snapshot " + snapshotName);
                        // FIXME: oh no, we can't call .getCurrentSnapshotForBlade until our blade is successfully allocated to
                        // us, otherwise we might get a snapshot for some other host!
                        string snapshot = director.getCurrentSnapshotForBlade(nodeName);

                        hypSpec_iLo spec = new hypSpec_iLo(
                            bladeConfig.bladeIP, iloHostUsername, iloHostPassword,
                            bladeConfig.iLOIP, iloUsername, iloPassword,
                            iloISCSIIP, iloISCSIUsername, iloISCSIPassword,
                            snapshot, bladeConfig.iLOPort, iloKernelKey
                            );

                        ensurePortIsFree(bladeConfig.iLOPort);

                        bladeDirectedHypervisor_iLo newHyp = new bladeDirectedHypervisor_iLo(spec);
                        newHyp.setDisposalCallback(onDestruction);
                        newHyp.checkSnapshotSanity();
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
            MIB_UDPTABLE_OWNER_PID portUse = tableContents.SingleOrDefault(x => x.localPort == port );
            if (portUse == null)
                return;

            using(Process p = Process.GetProcessById((int) portUse.ownerPID))
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

                    row.localPort = byteswap((UInt16)row.localPort);

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

            toRet |= (UInt16)((localPort & 0x00ff) << 8);
            toRet |= (UInt16)((localPort & 0xff00) >> 8);

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
            startKeepaliveThreadIfNotRunning();

            // We request a blade from the blade director, and use them for all our tests, blocking if none are available.
            using (servicesSoapClient director = new servicesSoapClient("servicesSoap", machinePools.bladeDirectorURL))
            {
                // Request a node. If all queues are full, then wait and retry until we get one.
                resultCodeAndBladeName allocatedBladeResult;
                while (true)
                {
                    allocatedBladeResult = director.RequestAnySingleNode();
                    if (allocatedBladeResult.code == resultCode.success || allocatedBladeResult.code == resultCode.pending)
                        break;
                    if (allocatedBladeResult.code == resultCode.bladeQueueFull)
                    {
                        Debug.WriteLine("All blades are fully queued, waiting until one is spare");
                        Thread.Sleep(TimeSpan.FromSeconds(5));
                        continue;
                    }
                    throw new Exception("Blade director returned unexpected return status '" + allocatedBladeResult.code + "'");
                }

                // Now, wait until our blade is available.
                try
                {
                    bool isMine = director.isBladeMine(allocatedBladeResult.bladeName);
                    while (!isMine)
                    {
                        Debug.WriteLine("Blade " + allocatedBladeResult.bladeName + " not released yet, awaiting release..");
                        Thread.Sleep(TimeSpan.FromSeconds(5));
                        isMine = director.isBladeMine(allocatedBladeResult.bladeName);
                    }

                    // Great, now we have ownership of the blade, so we can use it safely.
                    bladeSpec bladeConfig = director.getConfigurationOfBlade(allocatedBladeResult.bladeName);
                    var res = director.selectSnapshotForBladeOrVM(allocatedBladeResult.bladeName, snapshotName);
                    while (res == resultCode.pending)
                    {
                        res = director.selectSnapshotForBladeOrVM_getProgress(allocatedBladeResult.bladeName);
                        Thread.Sleep(TimeSpan.FromSeconds(5));
                    }
                    if (res != resultCode.success)
                        throw new Exception("Can't find snapshot " + snapshotName);
                    string snapshot = director.getCurrentSnapshotForBlade(allocatedBladeResult.bladeName);

                    hypSpec_iLo spec = new hypSpec_iLo(
                        bladeConfig.bladeIP, iloHostUsername, iloHostPassword,
                        bladeConfig.iLOIP, iloUsername, iloPassword,
                        iloISCSIIP, iloISCSIUsername, iloISCSIPassword,
                        snapshot, bladeConfig.iLOPort, iloKernelKey
                        );
                    
                    ensurePortIsFree(bladeConfig.iLOPort);

                    bladeDirectedHypervisor_iLo toRet = new bladeDirectedHypervisor_iLo(spec);
                    toRet.checkSnapshotSanity();
                    toRet.setDisposalCallback(onDestruction);
                    return toRet;
                }
                catch (Exception)
                {
                    director.releaseBladeOrVM(allocatedBladeResult.bladeName);
                    throw;
                }
            }
        }

        private void startKeepaliveThreadIfNotRunning()
        {
            if (keepaliveThread == null)
            {
                lock (keepaliveThreadLock)
                {
                    if (keepaliveThread == null)
                    {
                        keepaliveThread = new Thread(keepaliveThreadMain);
                        keepaliveThread.Name = "Blade director keepalive thread";
                        keepaliveThread.Start();
                    }
                }
            }
        }

        private void keepaliveThreadMain()
        {
            using (servicesSoapClient director = new servicesSoapClient("servicesSoap", machinePools.bladeDirectorURL))
            {
                while (true)
                {
                    Thread.Sleep(TimeSpan.FromSeconds(3));
                    try
                    {
                        director.keepAlive();
                    }
                    catch (TimeoutException) { }
                }
            }
        }

        private void onDestruction(hypSpec_iLo obj)
        {
            // Notify the director that this blade is no longer in use.
            using (servicesSoapClient director = new servicesSoapClient("servicesSoap", machinePools.bladeDirectorURL))
            {
                director.releaseBladeOrVM(obj.kernelDebugIPOrHostname);
            }
        }

        private void onDestruction(hypSpec_vmware obj)
        {
            using (servicesSoapClient director = new servicesSoapClient("servicesSoap", machinePools.bladeDirectorURL))
            {
                director.releaseBladeOrVM(obj.kernelDebugIPOrHostname);
            }
        }
    }

    public class bladeAllocationException : Exception
    {
        private readonly resultCode _code;

        public bladeAllocationException(resultCode code)
        {
            _code = code;
        }

        public override string ToString()
        {
            return "Blade allocation service returned code " + _code;
        }
    }

    [StructLayout(LayoutKind.Sequential, Pack =  0)]
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
        public bladeDirectedHypervisor_iLo(hypSpec_iLo spec) : base(spec)
        {
        }

        public void setBIOSConfig(string newBiosXML)
        {
            hypSpec_iLo spec = getConnectionSpec();
            using (servicesSoapClient director = new servicesSoapClient("servicesSoap", machinePools.bladeDirectorURL))
            {
                resultCode res = director.rebootAndStartDeployingBIOSToBlade(spec.kernelDebugIPOrHostname, newBiosXML);
                if (res == resultCode.noNeedLah)
                    return;

                if (res != resultCode.pending)
                    throw new Exception("Failed to start BIOS write to " + spec.kernelDebugIPOrHostname + ", error " + res);

                do
                {
                    res = director.checkBIOSDeployProgress(spec.kernelDebugIPOrHostname);
                } while (res == resultCode.pending);

                if (res != resultCode.success && res != resultCode.noNeedLah)
                    throw new Exception("Failed to write bios to " + spec.kernelDebugIPOrHostname + ", error " + res);
            }
        }
    }
}