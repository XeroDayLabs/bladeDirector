using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using bladeDirectorClient;
using bladeDirectorClient.bladeDirectorService;

namespace tests
{
    [TestClass]
    public class mockedBladeDestruction
    {
        [TestMethod]
        public void willDeallocateBlade()
        {
            using (bladeDirectorDebugServices svc = new bladeDirectorDebugServices(basicBladeTests.WCFPath))
            {
                string hostIP = "1.1.1.1";
                svc.svcDebug.initWithBladesFromIPList(new[] { "172.17.129.131" }, true, NASFaultInjectionPolicy.retunSuccessful);

                string ourBlade = testUtils.doBladeAllocationForTest(svc, hostIP);

                // It should be ours..
                GetBladeStatusResult bladeState = svc.svcDebug._GetBladeStatus(hostIP, ourBlade);
                Assert.AreEqual(bladeState, GetBladeStatusResult.yours);

                // Then free it
                resultAndWaitToken releaseRes = svc.svcDebug._ReleaseBladeOrVM(hostIP, ourBlade, false);
                testUtils.waitForSuccess(svc, releaseRes, TimeSpan.FromSeconds(10));

                // And it should be unused.
                bladeState = svc.svcDebug._GetBladeStatus(hostIP, ourBlade);
                Assert.AreEqual(bladeState, GetBladeStatusResult.unused);
            }
        }

        [TestMethod]
        public void willReallocateBladeAfterLogin()
        {
            using (bladeDirectorDebugServices svc = new bladeDirectorDebugServices(basicBladeTests.WCFPath))
            {
                string hostIP = "1.1.1.1";
                svc.svcDebug.initWithBladesFromIPList(new[] { "172.17.129.131" }, true, NASFaultInjectionPolicy.retunSuccessful);

                string ourBlade = testUtils.doBladeAllocationForTest(svc, hostIP);

                // It should be ours..
                GetBladeStatusResult bladeState = svc.svcDebug._GetBladeStatus(hostIP, ourBlade);
                Assert.AreEqual(bladeState, GetBladeStatusResult.yours);

                // Then free it 'dirtily', by logging in again.
                resultAndWaitToken res = svc.svcDebug._logIn(hostIP);
                while (res.result.code == resultCode.pending)
                {
                    res = svc.svc.getProgress(res.waitToken);
                    Thread.Sleep(TimeSpan.FromSeconds(3));
                }
                Assert.AreEqual(resultCode.success, res.result.code);

                // The blade should now be unused.
                bladeState = svc.svcDebug._GetBladeStatus(hostIP, ourBlade);
                Assert.AreEqual(bladeState, GetBladeStatusResult.unused);

                // And if we allocate again, we should get it OK.
                ourBlade = testUtils.doBladeAllocationForTest(svc, hostIP);
                bladeState = svc.svcDebug._GetBladeStatus(hostIP, ourBlade);
                Assert.AreEqual(bladeState, GetBladeStatusResult.yours);
            }
        }

        [TestMethod]
        public void willDeallocateBladeAfterVMDestruction()
        {
            using (bladeDirectorDebugServices svc = new bladeDirectorDebugServices(basicBladeTests.WCFPath, "2.2.2.2", true))
            {
                string hostIP = "1.1.1.1";

                string ourVM = testUtils.doVMAllocationForTest(svc, hostIP);

                // Our blade should become a VM server
                GetBladeStatusResult bladeState = svc.svcDebug._GetBladeStatus(hostIP, "2.2.2.2");
                Assert.AreEqual(GetBladeStatusResult.notYours, bladeState);

                // Then free the VM.
                resultAndWaitToken releaseRes = svc.svcDebug._ReleaseBladeOrVM(hostIP, ourVM, false);
                testUtils.waitForSuccess(svc, releaseRes, TimeSpan.FromSeconds(10));

                // The blade itself should become unused.
                bladeState = svc.svcDebug._GetBladeStatus(hostIP, "2.2.2.2");
                Assert.AreEqual(GetBladeStatusResult.unused, bladeState);
            }
        }

        [TestMethod]
        public void willDeallocateBladeDuringBIOSSetting()
        {
            using (bladeDirectorDebugServices svc = new bladeDirectorDebugServices(basicBladeTests.WCFPath))
            {
                string hostIP = "1.1.1.1";
                svc.svcDebug.initWithBladesFromIPList(new[] { "172.17.129.131" }, true, NASFaultInjectionPolicy.retunSuccessful);

                string ourBlade = testUtils.doBladeAllocationForTest(svc, hostIP);

                // Start a slowwww BIOS read
                svc.svcDebug._setBIOSOperationTimeIfMocked(60 * 10);
                resultAndWaitToken readRes = svc.svcDebug._rebootAndStartReadingBIOSConfiguration(hostIP, ourBlade);
                Assert.AreEqual(resultCode.pending, readRes.result.code);

                // Then free the blade. The BIOS operation should be cancelled before it soaks up all the ten minutes of time.
                resultAndWaitToken releaseRes = svc.svcDebug._ReleaseBladeOrVM(hostIP, ourBlade, false);
                testUtils.waitForSuccess(svc, releaseRes, TimeSpan.FromMinutes(1));

                // And it should be no longer getting.
                resultAndWaitToken bladeState = svc.svc.getProgress(readRes.waitToken);
                Assert.AreNotEqual(resultCode.pending, bladeState.result.code);

                // And should not be allocated
                GetBladeStatusResult ownershipRes = svc.svc.GetBladeStatus(ourBlade);
                Assert.AreEqual(GetBladeStatusResult.unused, ownershipRes);
            }
        }

        [TestMethod]
        public void willDeallocateBladeAtLoginDuringVMProvisioning()
        {
            using (bladeDirectorDebugServices svc = new bladeDirectorDebugServices(basicBladeTests.WCFPath))
            {
                string hostIP = "1.1.1.1";
                svc.svcDebug.initWithBladesFromIPList(new[] { "172.17.129.131" }, true, NASFaultInjectionPolicy.retunSuccessful);

                VMHardwareSpec hwspec = new VMHardwareSpec
                {
                    cpuCount = 1,
                    memoryMB = 1024 * 3
                };
                VMSoftwareSpec swspec = new VMSoftwareSpec();

                // Start a slow VM allocation
                svc.svcDebug._setExecutionResultsIfMocked(mockedExecutionResponses.successfulButSlow);
                resultAndWaitToken res = svc.svcDebug._requestAnySingleVM(hostIP, hwspec, swspec);
                Assert.AreEqual(resultCode.pending, res.result.code);

                // Then re-login. The VM operation should be cancelled.
                resultAndWaitToken releaseRes = svc.svcDebug._logIn(hostIP);
                testUtils.waitForSuccess(svc, releaseRes, TimeSpan.FromMinutes(1));

                // And it should be no longer provisioning the VM.
                resultAndWaitToken bladeState = svc.svc.getProgress(res.waitToken);
                Assert.AreNotEqual(resultCode.pending, bladeState.result.code);

                // And should not be allocated.
                GetBladeStatusResult ownershipRes = svc.svc.GetBladeStatus("172.17.129.131");
                Assert.AreEqual(GetBladeStatusResult.unused, ownershipRes);
            }
        }

        [TestMethod]
        public void willDeallocateOldBladesOnLogon()
        {
            using (bladeDirectorDebugServices svc = new bladeDirectorDebugServices(basicBladeTests.WCFPath))
            {
                string hostIP = "1.1.1.1";
                svc.svcDebug.initWithBladesFromIPList(new[] { "172.17.129.131" }, true, NASFaultInjectionPolicy.retunSuccessful);

                // Allocate a blade, then login again. The allocated blade should no longer be allocated.
                string ourBlade = testUtils.doBladeAllocationForTest(svc, hostIP);

                testUtils.doLogin(svc, hostIP);

                GetBladeStatusResult bladeState = svc.svcDebug._GetBladeStatus(hostIP, ourBlade);
                Assert.AreEqual(bladeState, GetBladeStatusResult.unused);
            }
        }
    }
}