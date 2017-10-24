using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using hypervisors;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Win32;
using bladeDirectorClient;
using bladeDirectorClient.bladeDirectorService;
using mockedCall = bladeDirectorClient.bladeDirectorService.mockedCall;

namespace tests
{
    [TestClass]
    public class mockedVMAllocation
    {
        [TestMethod]
        public void canAllocateVMMocked()
        {
            string hostIP = "1.1.1.1";

            using (bladeDirectorDebugServices svc = new bladeDirectorDebugServices(basicBladeTests.WCFPath, new[] { "1.1.1.1", "2.2.2.2", "3.3.3.3" }))
            {
                VMHardwareSpec hwSpec = new VMHardwareSpec() { memoryMB = 2344, cpuCount = 2 };
                VMSoftwareSpec swSpec = new VMSoftwareSpec();

                resultAndBladeName[] allocRes = doVMAllocationsForTest(svc, hostIP, new[] { new vmHWAndSWSpec(hwSpec, swSpec) });
                Assert.AreEqual(1, allocRes.Length);
                resultAndBladeName allocatedBlade = allocRes[0];

                // TODO: check nas events
                mockedCall[] nasEvents = svc.svcDebug._getNASEventsIfMocked();
                foreach (mockedCall call in nasEvents)
                    Debug.WriteLine(call.functionName + " " + call.message);
                // TODO: check executions all happened okay

                // This blade should become a VM server
                GetBladeStatusResult allocated1 = svc.svcDebug._GetBladeStatus(hostIP, "172.17.129.130");
                Assert.AreEqual(allocated1, GetBladeStatusResult.notYours);

                // And there should now be one VM allocated to us at present.
                Assert.AreEqual("172.17.158.1", allocatedBlade.bladeName);
                vmSpec VMConfig = svc.svc.getVMByIP_withoutLocking(allocatedBlade.bladeName);
                Assert.AreEqual("VM_30_01", VMConfig.displayName);
                Assert.AreEqual("172.17.158.1", VMConfig.VMIP);
                Assert.AreEqual("10.0.158.1", VMConfig.iscsiIP);
                Assert.AreEqual("00:50:56:00:30:01", VMConfig.eth0MAC);
                Assert.AreEqual("00:50:56:01:30:01", VMConfig.eth1MAC);
                Assert.AreEqual(2344, VMConfig.memoryMB);
                Assert.AreEqual(2, VMConfig.cpuCount);
                Assert.AreEqual(hostIP, VMConfig.currentOwner);
            }
        }

        [TestMethod]
        public void reportsVMCreationFailureNeatly()
        {
            string hostIP = "1.1.1.1";

            using (bladeDirectorDebugServices uut = new bladeDirectorDebugServices(basicBladeTests.WCFPath, new[] { "1.1.1.1", "2.2.2.2", "3.3.3.3" }))
            {
                VMHardwareSpec hwSpec = new VMHardwareSpec() {memoryMB = 2344, cpuCount = 2};
                VMSoftwareSpec swSpec = new VMSoftwareSpec();

                uut.svcDebug.initWithBladesFromIPList(new[] {"172.17.129.131", "172.17.129.130"}, true, NASFaultInjectionPolicy.failSnapshotDeletionOnFirstSnapshot);
                uut.svcDebug._setExecutionResultsIfMocked(mockedExecutionResponses.successful);

                resultAndBladeName allocRes;

                allocRes = uut.svcDebug._requestAnySingleVM(hostIP, hwSpec, swSpec);
                Assert.AreEqual(resultCode.pending, allocRes.result.code);

                waitToken waitTok = allocRes.waitToken;
                while (true)
                {
                    Thread.Sleep(TimeSpan.FromSeconds(1));

                    resultAndBladeName progress = (resultAndBladeName)uut.svc.getProgress(waitTok);
                    waitTok = progress.waitToken;

                    if (progress.result.code == resultCode.pending)
                        continue;
                    Assert.AreEqual(resultCode.genericFail, progress.result.code);
                    Assert.AreEqual("172.17.158.1", progress.bladeName);
                    break;
                }
                // OK, our allocation failed. Try allocating a second VM - this one should succeed.

                allocRes = uut.svcDebug._requestAnySingleVM(hostIP, hwSpec, swSpec);
                Assert.AreEqual(resultCode.pending, allocRes.result.code);
                allocRes = (resultAndBladeName)testUtils.waitForSuccess(uut, allocRes, TimeSpan.FromMinutes(11));

                Assert.AreNotEqual("172.17.158.1", allocRes.bladeName);
                Assert.AreNotEqual("172.17.158.1", allocRes.bladeName);
                vmSpec VMConfig = uut.svc.getVMByIP_withoutLocking(allocRes.bladeName);
                Assert.AreNotEqual("00:50:56:00:30:01", VMConfig.eth0MAC);
                Assert.AreNotEqual("00:50:56:01:30:01", VMConfig.eth1MAC);
            }
        }

        [TestMethod]
        public void canAllocateANumberOfVMMocked()
        {
            string hostIP = "1.1.1.1";

            using (bladeDirectorDebugServices svc = new bladeDirectorDebugServices(basicBladeTests.WCFPath, new[] { "1.1.1.1", "2.2.2.2", "3.3.3.3" }))
            {
                vmHWAndSWSpec[] toAlloc = new vmHWAndSWSpec[8];
                for (int i = 0; i < toAlloc.Length; i++)
                {
                    toAlloc[i] = new vmHWAndSWSpec(
                        new VMHardwareSpec() {memoryMB = 4096, cpuCount = 1},
                        new VMSoftwareSpec());
                }

                resultAndBladeName[] allocRes = doVMAllocationsForTest(svc, hostIP, toAlloc);
                Assert.AreEqual(toAlloc.Length, allocRes.Length);

                // Group blades by their parent blade's IP
                IGrouping<string, resultAndBladeName>[] bladesByParent = allocRes.GroupBy(x => svc.svc.getVMByIP_withoutLocking(x.bladeName).parentBladeIP).ToArray();

                // We should have two VM servers in use.
                Assert.AreEqual(2, bladesByParent.Length);

                // 5 should be on the first blade, and 3 on the second.
                Assert.AreEqual(5, bladesByParent[0].Count());
                Assert.AreEqual("172.17.129.130", bladesByParent[0].Key);
                Assert.AreEqual(3, bladesByParent[1].Count());
                Assert.AreEqual("172.17.129.131", bladesByParent[1].Key);

                // And release them, checking hardware status after each blade is empty.
                foreach (IGrouping<string, resultAndBladeName> bladeAndParent in bladesByParent)
                {
                    //string parentBladeName = uut.db.getConfigurationOfBladeByID((int)bladeAndParent.Key, bladeLockType.lockNone).spec.bladeIP;

                    foreach (resultAndBladeName res in bladeAndParent)
                    {
                        // The VM server should still be allocated before release..
                        Assert.AreEqual(svc.svcDebug._GetBladeStatus(hostIP, bladeAndParent.Key), GetBladeStatusResult.notYours);
                        svc.svcDebug._ReleaseBladeOrVM(hostIP, res.bladeName, false);
                    }

                    // This VM server should now be unused.
                    Assert.AreEqual(svc.svcDebug._GetBladeStatus(hostIP, bladeAndParent.Key), GetBladeStatusResult.unused);
                }
            }
        }

        public static resultAndBladeName[] doVMAllocationsForTest(bladeDirectorDebugServices uut, string hostIP, vmHWAndSWSpec[] specs, NASFaultInjectionPolicy NASFaultInjection = NASFaultInjectionPolicy.retunSuccessful)
        {
            uut.svcDebug.initWithBladesFromIPList(new[] { "172.17.129.131", "172.17.129.130" }, true, NASFaultInjection);
            uut.svcDebug._setExecutionResultsIfMocked(mockedExecutionResponses.successful);

            return doAllocation(uut, hostIP, specs);
        }

        private static resultAndBladeName[] doAllocation(bladeDirectorDebugServices uut, string hostIP, vmHWAndSWSpec[] specs)
        {
            resultAndBladeName[] allocRes = new resultAndBladeName[specs.Length];

            for (int i = 0; i < specs.Length; i++)
            {
                allocRes[i] = uut.svcDebug._requestAnySingleVM(hostIP, specs[i].hw, specs[i].sw);
                Assert.AreEqual(resultCode.pending, allocRes[i].result.code);
            }

            // Wait until all the allocation operations are complete
            for (int i = 0; i < specs.Length; i++)
                allocRes[i] = (resultAndBladeName)testUtils.waitForSuccess(uut, allocRes[i], TimeSpan.FromMinutes(15));

            return allocRes;
        }
    }

    public class vmHWAndSWSpec
    {
        public VMSoftwareSpec sw;
        public VMHardwareSpec hw;

        public vmHWAndSWSpec(VMHardwareSpec newHWSpec, VMSoftwareSpec newSWSpec)
        {
            sw = newSWSpec;
            hw = newHWSpec;
        }
    }
}