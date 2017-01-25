using System;
using System.Diagnostics;
using System.Net;
using System.Net.NetworkInformation;
using System.Threading;
using bladeDirectorClient.bladeDirector;
using hypervisors;

namespace bladeDirectorClient
{
    public class iLoHypervisorPool
    {
        private readonly object keepaliveThreadLock = new object();
        private Thread keepaliveThread = null;

        public hypervisorCollection requestAsManyHypervisorsAsPossible()
        {
            return requestAsManyHypervisorsAsPossible(
                Properties.Settings.Default.iloHostUsername,
                Properties.Settings.Default.iloHostPassword,
                Properties.Settings.Default.iloUsername,
                Properties.Settings.Default.iloPassword,
                Properties.Settings.Default.iloISCSIIP,
                Properties.Settings.Default.iloISCSIUsername,
                Properties.Settings.Default.iloISCSIPassword,
                Properties.Settings.Default.iloKernelKey);
        }

        public hypervisorCollection requestAsManyHypervisorsAsPossible(string iloHostUsername, string iloHostPassword,
            string iloUsername, string iloPassword,
            string iloISCSIIP, string iloISCSIUsername, string iloISCSIPassword,
            string iloKernelKey)
        {
            startKeepaliveThreadIfNotRunning();
            using (servicesSoapClient director = new servicesSoapClient("servicesSoap"))
            {
                string[] nodes = director.ListNodes().Split(',');

                hypervisorCollection toRet = new hypervisorCollection();
                try
                {
                    foreach (string nodeName in nodes)
                    {
                        string res = director.RequestNode(nodeName);

                        bladeSpec bladeConfig = director.getConfigurationOfBlade(nodeName);
                        string snapshot = director.getCurrentSnapshotForBlade(nodeName) + "-clean";

                        hypSpec_iLo spec = new hypSpec_iLo(
                            bladeConfig.bladeIP, iloHostUsername, iloHostPassword,
                            bladeConfig.iLOIP, iloUsername, iloPassword,
                            iloISCSIIP, iloISCSIUsername, iloISCSIPassword,
                            snapshot, bladeConfig.iLOPort, iloKernelKey
                            );

                        checkPort(bladeConfig);

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

        private static void checkPort(bladeSpec bladeConfig)
        {
            IPGlobalProperties ip = IPGlobalProperties.GetIPGlobalProperties();
            foreach (IPEndPoint conn in ip.GetActiveUdpListeners())
            {
                if (conn.Port == bladeConfig.iLOPort)
                    throw new Exception("port " + bladeConfig.iLOPort + " is already in use");
            }
        }

        public bladeDirectedHypervisor_iLo createSingleHypervisor()
        {
            return createSingleHypervisor(
                Properties.Settings.Default.iloHostUsername,
                Properties.Settings.Default.iloHostPassword,
                Properties.Settings.Default.iloUsername,
                Properties.Settings.Default.iloPassword,
                Properties.Settings.Default.iloISCSIIP,
                Properties.Settings.Default.iloISCSIUsername,
                Properties.Settings.Default.iloISCSIPassword,
                Properties.Settings.Default.iloKernelKey);
        }

        public bladeDirectedHypervisor_iLo createSingleHypervisor(string iloHostUsername, string iloHostPassword,
            string iloUsername, string iloPassword,
            string iloISCSIIP, string iloISCSIUsername, string iloISCSIPassword,
            string iloKernelKey)
        {
            startKeepaliveThreadIfNotRunning();

            // We request a blade from the blade director, and use them for all our tests, blocking if none are available.
            using (servicesSoapClient director = new servicesSoapClient("servicesSoap"))
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
                    throw new Exception("Blade director returned unexpected return status '" + allocatedBladeResult + "'");
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
                    director.selectSnapshotForBlade(allocatedBladeResult.bladeName, "clean");
                    string snapshot = director.getCurrentSnapshotForBlade(allocatedBladeResult.bladeName);

                    hypSpec_iLo spec = new hypSpec_iLo(
                        bladeConfig.bladeIP, iloHostUsername, iloHostPassword,
                        bladeConfig.iLOIP, iloUsername, iloPassword,
                        iloISCSIIP, iloISCSIUsername, iloISCSIPassword,
                        snapshot, bladeConfig.iLOPort, iloKernelKey
                        );

                    checkPort(bladeConfig);

                    bladeDirectedHypervisor_iLo toRet = new bladeDirectedHypervisor_iLo(spec);
                    toRet.checkSnapshotSanity();
                    toRet.setDisposalCallback(onDestruction);
                    return toRet;
                }
                catch (Exception)
                {
                    director.releaseBlade(allocatedBladeResult.bladeName);
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
            using (servicesSoapClient director = new servicesSoapClient("servicesSoap"))
            {
                while (true)
                {
                    Thread.Sleep(TimeSpan.FromSeconds(3));
                    director.keepAlive();
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
            using (servicesSoapClient director = new servicesSoapClient("servicesSoap"))
            {
                director.releaseBlade(obj.kernelDebugIPOrHostname);
            }
        }
    }

    public class bladeDirectedHypervisor_iLo : hypervisor_iLo
    {
        public bladeDirectedHypervisor_iLo(hypSpec_iLo spec) : base(spec)
        {
        }

        public void setBIOSConfig(string newBiosXML)
        {
            hypSpec_iLo spec = getConnectionSpec();
            using (servicesSoapClient director = new servicesSoapClient("servicesSoap"))
            {
                director.rebootAndStartDeployingBIOSToBlade(spec.kernelDebugIPOrHostname, newBiosXML);

                resultCode res;
                do
                {
                    res = director.checkBIOSDeployProgress(spec.kernelDebugIPOrHostname);
                } while (res != resultCode.pending);

                if (res != resultCode.success)
                {
                    throw new Exception("Failed to write bios to " + spec.kernelDebugIPOrHostname + ", error " + res);
                }
            }            
        }
    }
}