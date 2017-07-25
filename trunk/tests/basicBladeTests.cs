using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.ServiceModel;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using tests.bladeDirectorServices;

namespace tests
{
    [TestClass]
    public class basicBladeTests
    {
        [TestMethod]
        public void canInitWithBladesAndGetListBack()
        {
            using (services svc = new services(new[] { "1.1.1.1", "2.2.2.2", "3.3.3.3" }))
            {
                string[] foundIPs = svc.uut.getAllBladeIP();

                Assert.AreEqual(3, foundIPs.Length);
                Assert.IsTrue(foundIPs.Contains("1.1.1.1"));
                Assert.IsTrue(foundIPs.Contains("2.2.2.2"));
                Assert.IsTrue(foundIPs.Contains("3.3.3.3"));
            }
        }

        [TestMethod]
        public void canGetBladeSpec()
        {
            using (services svc = new services())
            {
                bladeSpec spec1Expected = svc.uutDebug.createBladeSpec("blade1ip", "blade1iscsiIP", "blade1ILOIP", 111, false, VMDeployStatus.needsPowerCycle, null, bladeLockType.lockAll, bladeLockType.lockAll );
                bladeSpec spec2Expected = svc.uutDebug.createBladeSpec("blade2ip", "blade2iscsiIP", "blade2ILOIP", 222, false, VMDeployStatus.needsPowerCycle, null, bladeLockType.lockAll, bladeLockType.lockAll);
                bladeSpec[] expected = new[] {spec1Expected, spec2Expected};

                svc.uutDebug.initWithBladesFromBladeSpec(expected, false, NASFaultInjectionPolicy.retunSuccessful);

                bladeSpec spec1Actual = svc.uut.getBladeByIP_withoutLocking("blade1ip");
                bladeSpec spec2Actual = svc.uut.getBladeByIP_withoutLocking("blade2ip");
                bladeSpec[] actual = new[] { spec1Actual, spec2Actual };

                for (int i = 0; i < 2; i++)
                {
                    Assert.AreEqual(expected[i].bladeIP, actual[i].bladeIP);
                    Assert.AreEqual(expected[i].ESXiPassword, actual[i].ESXiPassword);
                    Assert.AreEqual(expected[i].ESXiUsername, actual[i].ESXiUsername);
                    Assert.AreEqual(expected[i].currentlyBeingAVMServer, actual[i].currentlyBeingAVMServer);
                    Assert.AreEqual(expected[i].currentlyHavingBIOSDeployed, actual[i].currentlyHavingBIOSDeployed);
                    Assert.AreEqual(expected[i].iLOIP, actual[i].iLOIP);
                    Assert.AreEqual(expected[i].iLOPort, actual[i].iLOPort);
                    Assert.AreEqual(expected[i].iLoPassword, actual[i].iLoPassword);
                    Assert.AreEqual(expected[i].iLoUsername, actual[i].iLoUsername);
                    Assert.AreEqual(expected[i].iscsiIP, actual[i].iscsiIP);
                    Assert.AreEqual(expected[i].lastDeployedBIOS, actual[i].lastDeployedBIOS);
                    Assert.AreEqual(expected[i].ESXiPassword, actual[i].ESXiPassword);
                    Assert.AreEqual(expected[i].ESXiUsername, actual[i].ESXiUsername);
                    // thats enough for now
                }
            }
        }

        [TestMethod]
        public void canAllocateBlade()
        {
            using (services uut = new services(new[] {"1.1.1.1", "2.2.2.2", "3.3.3.3"}))
            {
                resultAndWaitToken requestStatus = uut.uutDebug._RequestAnySingleNode("192.168.1.1");
                Assert.AreEqual(resultCode.success, requestStatus.result.code);

                string[] allocated = uut.uut.getBladesByAllocatedServer("192.168.1.1");
                Assert.IsTrue(allocated.Contains("1.1.1.1"), "String '" + allocated + "' does not contain IP we allocated");
            }
        }

        [TestMethod]
        public void willReAllocateNode()
        {
            using (services uut = new services(new[] {"1.1.1.1"}))
            {
                string hostip = "192.168.1.1";

                Assert.AreEqual(resultCode.success, uut.uutDebug._RequestAnySingleNode(hostip).result.code);

                // First, the node should be ours.
                Assert.AreEqual(GetBladeStatusResult.yours, uut.uutDebug._GetBladeStatus(hostip, "1.1.1.1"));

                // Then, someoene else requests it..
                Assert.AreEqual(resultCode.pending, uut.uutDebug._RequestAnySingleNode("192.168.2.2").result.code);

                // and it should be pending.
                Assert.AreEqual(GetBladeStatusResult.releasePending, uut.uutDebug._GetBladeStatus(hostip, "1.1.1.1"));
                Assert.AreEqual(GetBladeStatusResult.releasePending, uut.uutDebug._GetBladeStatus("192.168.2.2", "1.1.1.1"));

                // Then, we release it.. 
                resultAndWaitToken res = uut.uutDebug._ReleaseBladeOrVM(hostip, "1.1.1.1", false);
                testUtils.waitForSuccess(uut, res, TimeSpan.FromSeconds(5));

                // and it should belong to the second requestor.
                Assert.AreEqual(GetBladeStatusResult.notYours, uut.uutDebug._GetBladeStatus(hostip, "1.1.1.1"));
                Assert.AreEqual(GetBladeStatusResult.yours, uut.uutDebug._GetBladeStatus("192.168.2.2", "1.1.1.1"));
            }
        }

        [TestMethod]
        public void willReAllocateNodeAfterTimeout()
        {
            using (services uut = new services(new[] {"1.1.1.1"}))
            {
                uut.uutDebug.setKeepAliveTimeout(10);

                Assert.AreEqual(resultCode.success, uut.uutDebug._RequestAnySingleNode("192.168.1.1").result.code);
                Assert.AreEqual(resultCode.pending, uut.uutDebug._RequestAnySingleNode("192.168.2.2").result.code);

                // 1.1 has it, 2.2 is queued
                Assert.IsTrue(uut.uutDebug._isBladeMine("192.168.1.1", "1.1.1.1"));
                Assert.IsFalse(uut.uutDebug._isBladeMine("192.168.2.2", "1.1.1.1"));

                // Now let 1.1 timeout
                for (int i = 0; i < 11; i++)
                {
                    uut.uutDebug._keepAlive("192.168.2.2");
                    Thread.Sleep(TimeSpan.FromSeconds(1));
                }

                // and it should belong to the second requestor.
                Assert.IsFalse(uut.uutDebug._isBladeMine("192.168.1.1", "1.1.1.1"));
                Assert.IsTrue(uut.uutDebug._isBladeMine("192.168.2.2", "1.1.1.1"));
            }
        }

        [TestMethod]
        public void willTimeoutOnNoKeepalives()
        {
            using (services uut = new services(new[] {"1.1.1.1"}))
            {
                uut.uutDebug.setKeepAliveTimeout(10);
                
                string hostip = "192.168.1.1";

                resultAndWaitToken resp = uut.uutDebug._RequestAnySingleNode(hostip);
                resultAndBladeName resWithName = (resultAndBladeName) resp;
                Assert.AreEqual(resultCode.success, resp.result.code);
                Assert.AreEqual("1.1.1.1", resWithName.bladeName);
                Assert.AreEqual(GetBladeStatusResult.yours, uut.uutDebug._GetBladeStatus(hostip, resWithName.bladeName));
                Thread.Sleep(TimeSpan.FromSeconds(11));
                Assert.AreEqual(GetBladeStatusResult.unused, uut.uutDebug._GetBladeStatus(hostip, resWithName.bladeName));
            }
        }

        [TestMethod]
        public void willNotTimeoutWhenWeSendKeepalives()
        {
            using (services uut = new services(new[] {"1.1.1.1"}))
            {
                uut.uutDebug.setKeepAliveTimeout(10);
                string hostip = "192.168.1.1";

                Assert.AreEqual(resultCode.success, uut.uutDebug._RequestAnySingleNode(hostip).result.code);
                Assert.AreEqual(GetBladeStatusResult.yours, uut.uutDebug._GetBladeStatus(hostip, "1.1.1.1"));

                for (int i = 0; i < 11; i++)
                {
                    uut.uutDebug._keepAlive("192.168.1.1");
                    Thread.Sleep(TimeSpan.FromSeconds(1));
                }
                Assert.AreEqual(GetBladeStatusResult.yours, uut.uutDebug._GetBladeStatus(hostip, "1.1.1.1"));
            }
        }
    }
}
