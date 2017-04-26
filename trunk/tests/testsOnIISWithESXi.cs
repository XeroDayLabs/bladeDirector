using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using hypervisors;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using tests.networkService;

namespace tests
{
    /// <summary>
    /// These tests will allocate real HW/VM from the cluster. Use them with care.
    /// </summary>
    [TestClass]
    public class testsOnIISWithESXi
    {
        [TestMethod]
        [TestCategory("skipOnCI")]
        public void canAllocateVM()
        {
            using (servicesSoapClient uut = new networkService.servicesSoapClient("servicesSoap"))
            {
                uut.releaseBladeDbg("172.17.129.131", "", true);
                uut.releaseBladeDbg("172.17.129.131", "", true);

                VMHardwareSpec hwSpec = new VMHardwareSpec() { memoryMB = 2344, cpuCount = 2 };
                VMSoftwareSpec swSpec = new VMSoftwareSpec() { forceRecreate =  true};
                resultCodeAndBladeName allocRes = uut.RequestAnySingleVM(hwSpec, swSpec);
                Assert.AreEqual(tests.networkService.resultCode.pending, allocRes.code);
                string waitToken = allocRes.waitToken;

                // Wait until the operation is complete
                DateTime deadline = DateTime.Now + TimeSpan.FromMinutes(15);
                while (true)
                {
                    allocRes = uut.getProgressOfVMRequest(waitToken);
                    if (allocRes.code == resultCode.pending)
                    {
                        // .. keep waiting ..
                        Thread.Sleep(TimeSpan.FromSeconds(10));
                    }
                    else if (allocRes.code == resultCode.success)
                    {
                        break;
                    }
                    else
                    {
                        Assert.Fail("unexpected state during VM provisioning: " + allocRes.code);
                    }

                    if (DateTime.Now > deadline)
                        throw new TimeoutException();
                }

                // This blade should become a VM server
                GetBladeStatusResult allocated1 = uut.GetBladeStatus("172.17.129.131");
                Assert.AreEqual(allocated1, GetBladeStatusResult.notYours);

                // And there should now be one VM allocated to us at present.
                Assert.AreEqual("172.17.159.1", allocRes.bladeName);
                vmSpec VMConfig = uut.getConfigurationOfVM(allocRes.bladeName);
                Assert.AreEqual("VM_31_01", VMConfig.displayName);
                Assert.AreEqual("172.17.159.1", VMConfig.VMIP);
                Assert.AreEqual("192.168.159.1", VMConfig.iscsiIP);
                Assert.AreEqual("00:50:56:00:31:01", VMConfig.eth0MAC);
                Assert.AreEqual("00:50:56:01:31:01", VMConfig.eth1MAC);
                Assert.AreEqual(2344, VMConfig.hwSpec.memoryMB);
                Assert.AreEqual(2, VMConfig.hwSpec.cpuCount);
                //Assert.AreEqual("::1", VMConfig.currentOwner);
            }
        }

        [TestMethod]
        [TestCategory("skipOnCI")]
        public void canAllocateVMAndProvisionWithCorrectVMNameAndKernelDebugInfo()
        {
            using (servicesSoapClient uut = new networkService.servicesSoapClient("servicesSoap"))
            {
                uut.releaseBladeDbg("172.17.129.131", "", true);
                uut.releaseBladeDbg("172.17.129.131", "", true);

                VMHardwareSpec hwSpec = new VMHardwareSpec() {memoryMB = 2048, cpuCount = 1};
                VMSoftwareSpec swSpec = new VMSoftwareSpec()
                {
                    debuggerHost = "172.16.10.91", 
                    debuggerPort = 50475, 
                    debuggerKey = "1.2.3.4",
                    forceRecreate =  true
                };
                resultCodeAndBladeName allocRes = uut.RequestAnySingleVM(hwSpec, swSpec);
                Assert.AreEqual(resultCode.pending, allocRes.code);
                allocRes = waitForVMProvisioning(allocRes, uut);

                // The blade should be busy now, but not ours. 
                GetBladeStatusResult allocated1 = uut.GetBladeStatus("172.17.129.131");
                Assert.AreEqual(allocated1, GetBladeStatusResult.notYours);

                try
                {
                    // Now we must check that the VM was set up correctly by the bladeDirector. We do this by getting the VM name
                    // and checking it is correct.
                    vmSpec vmCfg = uut.getConfigurationOfVM(allocRes.bladeName);
                    networkService.bladeSpec vmServer = uut.getConfigurationOfBladeByID((int) vmCfg.parentBladeID);
                    hypSpec_vmware spec = makeHypFromSpec(vmCfg, swSpec, vmServer);

                    using (hypervisor_vmware hyp = new hypervisor_vmware(spec, clientExecutionMethod.smb))
                    {
                        hyp.powerOn();

                        SMBExecutor exec = new SMBExecutor(allocRes.bladeName, Properties.Settings.Default.VMUsername, Properties.Settings.Default.VMPassword);
                        executionResult res = null;
                        hypervisor_iLo.doWithRetryOnSomeExceptions(() => res = exec.startExecutable("cmd.exe", "/c echo %COMPUTERNAME%"));
                        Assert.AreEqual(vmCfg.displayName, res.stdout.Trim('\r', '\n', ' '));

                        // And check the debugger was set up properly.
                        hypervisor_iLo.doWithRetryOnSomeExceptions(() => res = exec.startExecutable("bcdedit", "/dbgsettings"));
                        Assert.IsTrue(res.stdout.Contains("debugtype               NET"));
                        Assert.IsTrue(res.stdout.Contains("port                    50475"));
                        Assert.IsTrue(res.stdout.Contains("hostip                  172.16.10.91"));
                        Assert.IsTrue(res.stdout.Contains("key                     1.2.3.4"));
                    }
                }
                finally
                {
                    // And release the VM. This should result in the blade being de-allocated and powered down.
                    uut.releaseBladeOrVM(allocRes.bladeName);
                }

                allocated1 = uut.GetBladeStatus("172.17.129.131");
                Assert.AreEqual(allocated1, GetBladeStatusResult.unused);
            }
        }

        [TestMethod]
        [TestCategory("skipOnCI")]
        public void willAllocateMultipleVMOnOneBladeServer()
        {
            using (keepalive keepalive = new keepalive("http://localhost/bladeDirector/services.asmx"))
            {
                using (servicesSoapClient uut = new networkService.servicesSoapClient("servicesSoap"))
                {
                    uut.releaseBladeDbg("172.17.129.131", "", true);
                    uut.releaseBladeDbg("172.17.129.131", "", true);

                    VMHardwareSpec hwSpec1 = new VMHardwareSpec() {memoryMB = 2048, cpuCount = 1};
                    VMSoftwareSpec swSpec1 = new VMSoftwareSpec()
                    {
                        debuggerHost = "172.16.10.91", debuggerPort = 50475, debuggerKey = "1.2.3.4", forceRecreate = true
                    };
                    resultCodeAndBladeName allocRes1 = uut.RequestAnySingleVM(hwSpec1, swSpec1);
                    Assert.AreEqual(resultCode.pending, allocRes1.code);
                    allocRes1 = waitForVMProvisioning(allocRes1, uut);

                    // Request a second VM
                    VMHardwareSpec hwSpec2 = new VMHardwareSpec() {memoryMB = 3000, cpuCount = 2};
                    VMSoftwareSpec swSpec2 = new VMSoftwareSpec()
                    {
                        debuggerHost = "6.7.8.9", debuggerPort = 50555, debuggerKey = "11.22.33.44", forceRecreate = true
                    };

                    resultCodeAndBladeName allocRes2 = uut.RequestAnySingleVM(hwSpec2, swSpec2);
                    Assert.AreEqual(resultCode.pending, allocRes2.code);
                    allocRes2 = waitForVMProvisioning(allocRes2, uut);

                    // Ensure both VM are allocated to the same hardware blade.
                    vmSpec vmCfg1 = uut.getConfigurationOfVM(allocRes1.bladeName);
                    networkService.bladeSpec vmServer1 = uut.getConfigurationOfBladeByID((int) vmCfg1.parentBladeID);
                    vmSpec vmCfg2 = uut.getConfigurationOfVM(allocRes2.bladeName);
                    networkService.bladeSpec vmServer2 = uut.getConfigurationOfBladeByID((int) vmCfg2.parentBladeID);

                    Assert.AreEqual(vmServer1.bladeID, vmServer2.bladeID);

                    // And release the first VM. This should result in the blade being retained, since VM2 is still allocated on it.
                    uut.releaseBladeOrVM(allocRes1.bladeName);
                    GetBladeStatusResult allocated = uut.GetBladeStatus("172.17.129.131");
                    Assert.AreEqual(allocated, GetBladeStatusResult.notYours);

                    // This release of VM2 should result in the blade being powered down.
                    uut.releaseBladeOrVM(allocRes2.bladeName);

                    allocated = uut.GetBladeStatus("172.17.129.131");
                    Assert.AreEqual(allocated, GetBladeStatusResult.unused);
                }
            }
        }

        [TestMethod]
        [TestCategory("skipOnCI")]
        public void willAllocateANumberOfVM()
        {
            using (keepalive keepalive = new keepalive("http://localhost/bladeDirector/services.asmx"))
            {
                using (servicesSoapClient uut = new networkService.servicesSoapClient("servicesSoap"))
                {
                    uut.releaseBladeDbg("172.17.129.131", "", true);
                    uut.releaseBladeDbg("172.17.129.131", "", true);

                    // Allocate ten VMs. They should not all fit on one VM server.
                    resultCodeAndBladeName[] allocationResults = new resultCodeAndBladeName[10];
                    for (int n = 0; n < allocationResults.Length; n++)
                    {
                        allocationResults[n] = uut.RequestAnySingleVM(
                            new VMHardwareSpec() {memoryMB = 2048, cpuCount = 1},
                            new VMSoftwareSpec()
                            {
                                debuggerHost = "172.16.10.91",
                                debuggerPort = (ushort) (53000 + n),
                                debuggerKey = "1.2.3.4",
                                forceRecreate = true
                            });
                    };

                    for (int index = 0; index < allocationResults.Length; index++)
                    {
                        resultCodeAndBladeName res = allocationResults[index];
                        Assert.AreEqual(resultCode.pending, res.code);
                        allocationResults[index] = waitForVMProvisioning(allocationResults[index], uut);
                    }

                    // Group blades by their parent blade's DB ID
                    IGrouping<long, resultCodeAndBladeName>[] bladesByParent = allocationResults.GroupBy(x => uut.getConfigurationOfVM(x.bladeName).parentBladeID).ToArray();

                    // We should have two VM servers in use.
                    Assert.AreEqual(2, bladesByParent.Length);

                    // 8 should be on the first blade, and 3 on the second.
                    Assert.AreEqual(7, bladesByParent[0].Count());
                    Assert.AreEqual("172.17.129.131", uut.getConfigurationOfBladeByID((int) bladesByParent[0].Key).bladeIP);
                    Assert.AreEqual(3, bladesByParent[1].Count());
                    Assert.AreEqual("172.17.129.130", uut.getConfigurationOfBladeByID((int)bladesByParent[1].Key).bladeIP);

                    // And release them, checking hardware status after each blade is empty.
                    foreach (IGrouping<long, resultCodeAndBladeName> bladeAndParent in bladesByParent)
                    {
                        string parentBladeName = uut.getConfigurationOfBladeByID((int)bladeAndParent.Key).bladeIP;

                        foreach (resultCodeAndBladeName res in bladeAndParent)
                        {
                            Assert.AreEqual(uut.GetBladeStatus(parentBladeName), GetBladeStatusResult.notYours);
                            uut.releaseBladeOrVM(res.bladeName);
                        }

                        Assert.AreEqual(uut.GetBladeStatus(parentBladeName), GetBladeStatusResult.unused);
                    }
                }
            }
        }

        private static resultCodeAndBladeName waitForVMProvisioning(resultCodeAndBladeName allocRes, servicesSoapClient uut)
        {
            // This allocation can take quite a while if the VM server is powered down initially.
            string waitToken = allocRes.waitToken;

            DateTime deadline = DateTime.Now + TimeSpan.FromMinutes(30);
            while (true)
            {
                allocRes = uut.getProgressOfVMRequest(waitToken);
                if (allocRes.code == resultCode.success)
                    break;
                if (allocRes.code == resultCode.pending || allocRes.code == resultCode.unknown)
                    Thread.Sleep(TimeSpan.FromSeconds(10));
                else
                    Assert.Fail("unexpected state during VM provisioning: " + allocRes.code);

                if (DateTime.Now > deadline)
                    throw new TimeoutException();
            }
            return allocRes;
        }

        public static hypSpec_vmware makeHypFromSpec(vmSpec vmCfg, VMSoftwareSpec swSpec, networkService.bladeSpec vmServer)
        {
            return  new hypSpec_vmware(vmCfg.displayName, vmServer.bladeIP, 
                vmServer.ESXiUsername, vmServer.ESXiPassword, 
                vmCfg.username, vmCfg.password, 
                swSpec.debuggerPort, swSpec.debuggerKey, vmCfg.VMIP );
        }
    }
}