using System;
using System.Net;
using System.Threading;
using bladeDirectorClient;
using bladeDirectorClient.bladeDirectorService;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace tests
{
    [TestClass]
    public class testsWithRealBlade
    { 
        [TestMethod]
        public void willSetBIOS()
        {
            string testBiosXML = Properties.Resources.testBIOS;

            using (bladeDirectorDebugServices svc = new bladeDirectorDebugServices(basicBladeTests.WCFPath, basicBladeTests.WebURI))
            {
                string hostip = "1.2.3.4";

                // We will be using this blade for our tests.
                bladeSpec spec = svc.svcDebug.createBladeSpec("172.17.129.131", "192.168.129.131", "172.17.2.131", 1234, false, VMDeployStatus.notBeingDeployed, " ... ", bladeLockType.lockAll, bladeLockType.lockAll);
                svc.svcDebug.initWithBladesFromBladeSpec(new[] { spec }, false, NASFaultInjectionPolicy.retunSuccessful);

                resultAndBladeName res = svc.svcDebug._RequestAnySingleNode(hostip);
                Assert.AreEqual(resultCode.success, res.result.code);

                // Write the new BIOS. This configuration has the 'boot state' of the numlock key set to 'off', so we can
                // read that back after writing to ensure that the BIOS write succeeds.
                var resultAndReadBack = writeBIOSAndReadBack(svc, hostip, res.bladeName, testBiosXML);
                Assert.IsTrue(resultAndReadBack.BIOSConfig.Contains("<Section name=\"NumLock\">Off</Section>"));
                // Now we can modify the BIOS slightly, to set the numlock boot state to 'on', and again write/readback, and see
                // if the change was carried out.
                testBiosXML = testBiosXML.Replace("<Section name=\"NumLock\">Off</Section>", "<Section name=\"NumLock\">On</Section>");
                resultAndReadBack = writeBIOSAndReadBack(svc, hostip, res.bladeName, testBiosXML);
                Assert.IsTrue(resultAndReadBack.BIOSConfig.Contains("<Section name=\"NumLock\">On</Section>"));

                resultAndWaitToken relRes = svc.svcDebug._ReleaseBladeOrVM(hostip, res.bladeName, false);
                while (relRes.result.code == bladeDirectorClient.bladeDirectorService.resultCode.pending)
                {
                    Thread.Sleep(TimeSpan.FromSeconds(3));
                    relRes = svc.svc.getProgress(relRes.waitToken);
                }
                Assert.AreEqual(bladeDirectorClient.bladeDirectorService.resultCode.success, relRes.result.code);
            }
        }

        [TestMethod]
        public void willCancelBIOSWrite()
        {
            willCancelBIOSWrite(TimeSpan.MinValue);
        }

        [TestMethod]
        public void willCancelBIOSWriteAfterWaiting()
        {
            for (int mins = 1; mins < 10; mins++)
                willCancelBIOSWrite(TimeSpan.FromMinutes(mins));
        }

        public void willCancelBIOSWrite(TimeSpan delay)
        {
            using (bladeDirectorDebugServices svc = new bladeDirectorDebugServices(basicBladeTests.WCFPath, basicBladeTests.WebURI))
            {
                string hostIP = "1.1.1.1";
                bladeSpec spec = svc.svcDebug.createBladeSpec("172.17.129.131", "192.168.129.131", "172.17.2.131", 1234, false, VMDeployStatus.notBeingDeployed, " ... ", bladeLockType.lockAll, bladeLockType.lockAll);
                svc.svcDebug.initWithBladesFromBladeSpec(new[] { spec }, false, NASFaultInjectionPolicy.retunSuccessful);

                resultAndBladeName res = svc.svcDebug._RequestAnySingleNode(hostIP);
                Assert.AreEqual(resultCode.success, res.result.code);
                string ourBlade = ((resultAndBladeName)testUtils.waitForSuccess(svc, res, TimeSpan.FromSeconds(30))).bladeName;

                // Start a BIOS read
                resultAndWaitToken readRes = svc.svcDebug._rebootAndStartReadingBIOSConfiguration(hostIP, ourBlade);
                Assert.AreEqual(resultCode.pending, readRes.result.code);

                // Wait a while to see if we get somewhere where it is impossible to free without a hueg delay
                Thread.Sleep(delay);

                // Then free the blade. The BIOS operation should be cancelled before it soaks up all the ten minutes of time.
                resultAndWaitToken releaseRes = svc.svcDebug._ReleaseBladeOrVM(hostIP, ourBlade, false);
                testUtils.waitForSuccess(svc, releaseRes, TimeSpan.FromMinutes(1));

                // And it should be no longer getting.
                resultAndWaitToken bladeState = svc.svc.getProgress(readRes.waitToken);
                Assert.AreNotEqual(resultCode.pending, bladeState.result.code);

                // It no longer be allocated to us.
                GetBladeStatusResult ownershipRes = svc.svc.GetBladeStatus(ourBlade);
                Assert.AreEqual(GetBladeStatusResult.notYours, ownershipRes);
            }
        }


        [TestMethod]
        public void willProvisionVM()
        {
            using (bladeDirectorDebugServices svc = new bladeDirectorDebugServices(basicBladeTests.WCFPath, basicBladeTests.WebURI))
            {
                string hostip = "1.2.3.4";
                string debuggerHost = testUtils.getBestRouteTo(IPAddress.Parse("172.17.129.131")).ToString();

                // We will be using this blade for our tests.
                bladeSpec spec = svc.svcDebug.createBladeSpec("172.17.129.131", "192.168.129.131", "172.17.2.131", 1234, false, VMDeployStatus.notBeingDeployed, " ... ", bladeLockType.lockAll, bladeLockType.lockAll);
                svc.svcDebug.initWithBladesFromBladeSpec(new[] { spec }, false, NASFaultInjectionPolicy.retunSuccessful);

                VMSoftwareSpec sw = new VMSoftwareSpec() { debuggerHost = debuggerHost, debuggerKey = "a.b.c.d", debuggerPort = 10234 };
                VMHardwareSpec hw = new VMHardwareSpec() { cpuCount = 1, memoryMB = 4096 };
                resultAndBladeName res = svc.svcDebug._requestAnySingleVM(hostip, hw, sw);
                testUtils.waitForSuccess(svc, res, TimeSpan.FromMinutes(15));

                string VMName = res.bladeName;

                // Okay, we have our blade allocated now. 
                // TODO: check it, power it on, etc.
            }
        }

        private static resultAndBIOSConfig writeBIOSAndReadBack(bladeDirectorDebugServices svc, string hostIP, string bladeIP, string testBiosXML)
        {
            resultAndWaitToken result = svc.svcDebug._rebootAndStartDeployingBIOSToBlade(hostIP, bladeIP, testBiosXML);
            testUtils.waitForSuccess(svc, result, TimeSpan.FromMinutes(5));

            // Now check it wrote OK by reading it back and comparing the numlock key state.
            Assert.AreEqual(bladeDirectorClient.bladeDirectorService.resultCode.pending, svc.svcDebug._rebootAndStartReadingBIOSConfiguration(hostIP, bladeIP).result.code);
            result = testUtils.waitForSuccess(svc, result, TimeSpan.FromMinutes(5));

            // We should definitely have written the config, so we don't permit the 'no need' code here.
            Assert.AreEqual(bladeDirectorClient.bladeDirectorService.resultCode.success, result.result.code);

            return (resultAndBIOSConfig)result;
        }
    }
}