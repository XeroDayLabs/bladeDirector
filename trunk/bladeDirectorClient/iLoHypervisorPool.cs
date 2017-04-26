using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Runtime.InteropServices;
using System.Threading;
using bladeDirectorClient.bladeDirector;
using hypervisors;

namespace bladeDirectorClient
{
    public class iLoHypervisorPool
    {
        private readonly object keepaliveThreadLock = new object();
        private Thread keepaliveThread = null;

        public hypervisorCollection requestAsManyHypervisorsAsPossible(string snapshotName)
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

        public hypervisorCollection requestAsManyHypervisorsAsPossible(string iloHostUsername, string iloHostPassword,
            string iloUsername, string iloPassword,
            string iloISCSIIP, string iloISCSIUsername, string iloISCSIPassword,
            string iloKernelKey, string snapshotName)
        {
            startKeepaliveThreadIfNotRunning();
            using (servicesSoapClient director = new servicesSoapClient("servicesSoap", machinePools.bladeDirectorURL))
            {
                string[] nodes = director.ListNodes().Split(',');

                hypervisorCollection toRet = new hypervisorCollection();
                try
                {
                    foreach (string nodeName in nodes)
                    {
                        resultCode res = director.RequestNode(nodeName);

                        bladeSpec bladeConfig = director.getConfigurationOfBlade(nodeName);
                        if (director.selectSnapshotForBlade(nodeName, snapshotName) != resultCode.success)
                            throw new Exception("Can't find snapshot " + snapshotName);
                        string snapshot = director.getCurrentSnapshotForBlade(nodeName);

                        hypSpec_iLo spec = new hypSpec_iLo(
                            bladeConfig.bladeIP, iloHostUsername, iloHostPassword,
                            bladeConfig.iLOIP, iloUsername, iloPassword,
                            iloISCSIIP, iloISCSIUsername, iloISCSIPassword,
                            snapshot, bladeConfig.iLOPort, iloKernelKey
                            );

                        ensurePortIsFree(bladeConfig);

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
        public static extern int GetExtendedUdpTable(IntPtr pUdpTable, out int dwOutBufLen, bool sort, int ipVersion, int tblClass, int reserved);


        private const int UDP_TABLE_OWNER_PID = 1;

        public static void ensurePortIsFree(bladeSpec bladeConfig)
        {
            MIB_UDPTABLE_OWNER_PID[] tableContents = getCurrentUDPListeners();
            MIB_UDPTABLE_OWNER_PID portUse = tableContents.SingleOrDefault(x => x.localPort ==  bladeConfig.iLOPort);
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

                Debug.WriteLine("tableBuffer " + tableBuffer);
                for (int rowIndex = 0; rowIndex < tableContents.Length; rowIndex++)
                {
                    IntPtr pos = tableBuffer + 
                                 Marshal.SizeOf(typeof (UDP_TABLE_CLASS)) +
                                 ((Marshal.SizeOf(typeof (MIB_UDPTABLE_OWNER_PID)))*rowIndex);
                    MIB_UDPTABLE_OWNER_PID row = (MIB_UDPTABLE_OWNER_PID) Marshal.PtrToStructure(pos, typeof (MIB_UDPTABLE_OWNER_PID));

                    row.localPort = byteswap((UInt16)row.localPort);

                    Debug.WriteLine("Pos " + pos + " " + row.localPort + " by " + row.ownerPID);
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
                    if (director.selectSnapshotForBlade(allocatedBladeResult.bladeName, snapshotName) != resultCode.success)
                        throw new Exception("Can't find snapshot " + snapshotName);
                    string snapshot = director.getCurrentSnapshotForBlade(allocatedBladeResult.bladeName);

                    hypSpec_iLo spec = new hypSpec_iLo(
                        bladeConfig.bladeIP, iloHostUsername, iloHostPassword,
                        bladeConfig.iLOIP, iloUsername, iloPassword,
                        iloISCSIIP, iloISCSIUsername, iloISCSIPassword,
                        snapshot, bladeConfig.iLOPort, iloKernelKey
                        );
                    
                    ensurePortIsFree(bladeConfig);

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
            // Power the node down before we do anything with it.
            using (hypervisor_iLo releasedBlade = new hypervisor_iLo(obj))
            {
                releasedBlade.powerOff();
            }
            // And notify the director that this blade is no longer in use.
            using (servicesSoapClient director = new servicesSoapClient("servicesSoap", machinePools.bladeDirectorURL))
            {
                director.releaseBladeOrVM(obj.kernelDebugIPOrHostname);
            }
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