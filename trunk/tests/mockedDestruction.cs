using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using tests.bladeDirectorServices;

namespace tests
{
    [TestClass]
    public class mockedBladeDestruction
    {
        [TestMethod]
        public void willDeallocateBlade()
        {
            using (services svc = new services())
            {
                string hostIP = "1.1.1.1";
                svc.uutDebug.initWithBladesFromIPList(new[] { "172.17.129.131" }, true, NASFaultInjectionPolicy.retunSuccessful);

                string ourBlade = testUtils.doBladeAllocationForTest(svc, hostIP);

                // It should be ours..
                GetBladeStatusResult bladeState = svc.uutDebug._GetBladeStatus(hostIP, ourBlade);
                Assert.AreEqual(bladeState, GetBladeStatusResult.yours);

                // Then free it
                resultAndWaitToken releaseRes = svc.uutDebug._ReleaseBladeOrVM(hostIP, ourBlade, false);
                testUtils.waitForSuccess(svc, releaseRes, TimeSpan.FromSeconds(10));

                // And it should be unused.
                bladeState = svc.uutDebug._GetBladeStatus(hostIP, ourBlade);
                Assert.AreEqual(bladeState, GetBladeStatusResult.unused);
            }
        }

        [TestMethod]
        public void willReallocateBladeAfterLogin()
        {
            using (services svc = new services())
            {
                string hostIP = "1.1.1.1";
                svc.uutDebug.initWithBladesFromIPList(new[] { "172.17.129.131" }, true, NASFaultInjectionPolicy.retunSuccessful);

                string ourBlade = testUtils.doBladeAllocationForTest(svc, hostIP);

                // It should be ours..
                GetBladeStatusResult bladeState = svc.uutDebug._GetBladeStatus(hostIP, ourBlade);
                Assert.AreEqual(bladeState, GetBladeStatusResult.yours);

                // Then free it 'dirtily', by logging in again.
                svc.uutDebug._logIn(hostIP);

                // The blade should now be unused.
                bladeState = svc.uutDebug._GetBladeStatus(hostIP, ourBlade);
                Assert.AreEqual(bladeState, GetBladeStatusResult.unused);

                // And if we allocate again, we should get it OK.
                ourBlade = testUtils.doBladeAllocationForTest(svc, hostIP);
                bladeState = svc.uutDebug._GetBladeStatus(hostIP, ourBlade);
                Assert.AreEqual(bladeState, GetBladeStatusResult.yours);
            }
        }

        [TestMethod]
        public void willDeallocateBladeAfterVMDestruction()
        {
            using (services svc = new services("2.2.2.2"))
            {
                string hostIP = "1.1.1.1";

                string ourVM = testUtils.doVMAllocationForTest(svc, hostIP);

                // Our blade should become a VM server
                GetBladeStatusResult bladeState = svc.uutDebug._GetBladeStatus(hostIP, "2.2.2.2");
                Assert.AreEqual(GetBladeStatusResult.notYours, bladeState);

                // Then free the VM.
                resultAndWaitToken releaseRes = svc.uutDebug._ReleaseBladeOrVM(hostIP, ourVM, false);
                testUtils.waitForSuccess(svc, releaseRes, TimeSpan.FromSeconds(10));

                // The blade itself should become unused.
                bladeState = svc.uutDebug._GetBladeStatus(hostIP, "2.2.2.2");
                Assert.AreEqual(GetBladeStatusResult.unused, bladeState);
            }
        }

        [TestMethod]
        public void willDeallocateBladeDuringBIOSSetting()
        {
            using (services svc = new services())
            {
                string hostIP = "1.1.1.1";
                svc.uutDebug.initWithBladesFromIPList(new[] { "172.17.129.131" }, true, NASFaultInjectionPolicy.retunSuccessful);

                string ourBlade = testUtils.doBladeAllocationForTest(svc, hostIP);

                // Start a slowwww BIOS read
                svc.uutDebug._setBIOSOperationTimeIfMocked(60 * 10);
                resultAndWaitToken readRes = svc.uutDebug._rebootAndStartReadingBIOSConfiguration(hostIP, ourBlade);
                Assert.AreEqual(resultCode.pending, readRes.result.code);

                // Then free the blade. The BIOS operation should be cancelled before it soaks up all the ten minutes of time.
                resultAndWaitToken releaseRes = svc.uutDebug._ReleaseBladeOrVM(hostIP, ourBlade, false);
                testUtils.waitForSuccess(svc, releaseRes, TimeSpan.FromMinutes(1));

                // And it should be no longer getting.
                resultAndWaitToken bladeState = svc.uut.getProgress(readRes.waitToken);
                Assert.AreNotEqual(resultCode.pending, bladeState.result.code);

                // And should not be allocated
                GetBladeStatusResult ownershipRes = svc.uut.GetBladeStatus(ourBlade);
                Assert.AreEqual(GetBladeStatusResult.unused, ownershipRes);
            }
        }

        [TestMethod]
        public void willDeallocateOldBladesOnLogon()
        {
            using (services svc = new services())
            {
                string hostIP = "1.1.1.1";
                svc.uutDebug.initWithBladesFromIPList(new[] { "172.17.129.131" }, true, NASFaultInjectionPolicy.retunSuccessful);

                // Allocate a blade, then login again. The allocated blade should no longer be allocated.
                string ourBlade = testUtils.doBladeAllocationForTest(svc, hostIP);

                testUtils.doLogin(svc, hostIP);

                GetBladeStatusResult bladeState = svc.uutDebug._GetBladeStatus(hostIP, ourBlade);
                Assert.AreEqual(bladeState, GetBladeStatusResult.unused);
            }
        }
    }
}