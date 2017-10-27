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

            using (bladeDirectorDebugServices svc = new bladeDirectorDebugServices(basicBladeTests.WCFPath, true))
            //using (bladeDirectorDebugServices svc = new bladeDirectorDebugServices("http://localhost/bladeDirectorDebug", "http://localhost/bladeDirector"))
            {
                string hostip = "1.2.3.4";

                // We will be using this blade for our tests.
                bladeSpec spec = svc.svcDebug.createBladeSpec("172.17.129.131", "192.168.129.131", "172.17.2.131", 1234, false, VMDeployStatus.notBeingDeployed, " ... ", bladeLockType.lockAll, bladeLockType.lockAll);
                svc.svcDebug.initWithBladesFromBladeSpec(new[] { spec }, false, NASFaultInjectionPolicy.retunSuccessful);

                resultAndBladeName res = svc.svcDebug._RequestAnySingleNode(hostip);
                Assert.AreEqual(bladeDirectorClient.bladeDirectorService.resultCode.success, res.result.code);

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
        public void willProvisionVM()
        {
            using (bladeDirectorDebugServices svc = new bladeDirectorDebugServices(basicBladeTests.WCFPath, true))
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

        [TestMethod]
        public void willProvisionVM_reportsFailure()
        {
            using (bladeDirectorDebugServices svc = new bladeDirectorDebugServices(basicBladeTests.WCFPath, true))
            {
                string hostip = "1.2.3.4";
                string debuggerHost = testUtils.getBestRouteTo(IPAddress.Parse("172.17.129.131")).ToString();

                // We will be using this blade for our tests.
                bladeSpec spec = svc.svcDebug.createBladeSpec("172.17.129.131", "192.168.129.131", "172.17.2.131", 1234, false, VMDeployStatus.notBeingDeployed, " ... ", bladeLockType.lockAll, bladeLockType.lockAll);
                svc.svcDebug.initWithBladesFromBladeSpec(new[] { spec }, false, NASFaultInjectionPolicy.failSnapshotDeletionOnFirstSnapshot);

                VMSoftwareSpec sw = new VMSoftwareSpec() { debuggerHost = debuggerHost, debuggerKey = "a.b.c.d", debuggerPort = 10234 };
                VMHardwareSpec hw = new VMHardwareSpec() { cpuCount = 1, memoryMB = 4096 };
                resultAndBladeName res = svc.svcDebug._requestAnySingleVM(hostip, hw, sw);
                resultAndWaitToken waitRes = null;
                while (true)
                {
                    waitRes =  svc.svc.getProgress(res.waitToken);
                    if (waitRes.result.code != bladeDirectorClient.bladeDirectorService.resultCode.pending)
                        break;
                }

                Assert.AreEqual(bladeDirectorClient.bladeDirectorService.resultCode.genericFail, waitRes.result.code);

                res = svc.svcDebug._requestAnySingleVM(hostip, hw, sw);
                waitRes = null;
                while (true)
                {
                    waitRes = svc.svc.getProgress(res.waitToken);
                    if (waitRes.result.code != bladeDirectorClient.bladeDirectorService.resultCode.pending)
                        break;
                }

                Assert.AreEqual(bladeDirectorClient.bladeDirectorService.resultCode.success, waitRes.result.code);

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