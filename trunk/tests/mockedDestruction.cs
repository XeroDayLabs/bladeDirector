using System;
using System.Diagnostics;
using bladeDirector;
using System.Linq;
using System.Net;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace tests
{
    [TestClass]
    public class mockedDestruction
    {
        [TestMethod]
        public void willDeallocateBlade()
        {
            string hostIP = "1.1.1.1";

            hostStateDB_mocked uut = new hostStateDB_mocked();
            string ourBlade = doBladeAllocationForTest(uut, hostIP);

            // It should be ours..
            GetBladeStatusResult bladeState = uut.getBladeStatus(ourBlade, hostIP);
            Assert.AreEqual(bladeState, GetBladeStatusResult.yours);

            // Then free it
            uut.releaseBladeOrVM(ourBlade, hostIP);

            // And it should be unused.
            bladeState = uut.getBladeStatus(ourBlade, hostIP);
            Assert.AreEqual(bladeState, GetBladeStatusResult.unused);
        }

        [TestMethod]
        public void willDeallocateBladeDuringBIOSSetting()
        {
            string hostIP = "1.1.1.1";

            hostStateDB_mocked uut = new hostStateDB_mocked();
            string ourBlade = doBladeAllocationForTest(uut, hostIP, true);

            resultCode readRes = uut.rebootAndStartReadingBIOSConfiguration(ourBlade, hostIP);
            Assert.AreEqual(readRes, resultCode.pending);

            // Then free it
            uut.releaseBladeOrVM(ourBlade, hostIP);

            // And it should be no longer getting.
            resultCodeAndBIOSConfig bladeState = uut.checkBIOSReadProgress(ourBlade);
            Assert.AreEqual(bladeState.code, resultCode.bladeNotFound);
        }

        [TestMethod]
        public void willDeallocateOldBladesOnLogon()
        {
            string hostIP = "1.1.1.1";

            // Allocate all the blades, then login again. The allocated blades should no longer be allocated.
            hostStateDB_mocked uut = new hostStateDB_mocked();
            string ourBlade = doBladeAllocationForTest(uut, hostIP, true);

            doLogin(uut, hostIP);
            
            GetBladeStatusResult bladeState = uut.getBladeStatus(ourBlade, hostIP);
            Assert.AreEqual(bladeState, GetBladeStatusResult.unused);
        }

        [TestMethod]
        public void willDeallocateOldVMsOnLogon()
        {
            string hostIP = "1.1.1.1";

            // Allocate all the blades, then login again. The allocated blades should no longer be allocated.
            hostStateDB_mocked uut = new hostStateDB_mocked();
            string ourBlade = doVMAllocationForTest(uut, hostIP);

            // Find the parent blade of the VM we got, and make sure it is now in use (by the blade director)
            vmSpec VMSpec = uut.getVMByIP(ourBlade);
            bladeSpec bladeSpec = uut.getConfigurationOfBladeByID((int)VMSpec.parentBladeID);
            GetBladeStatusResult bladeState = uut.getBladeStatus(bladeSpec.bladeIP, hostIP);
            Assert.AreEqual(bladeState, GetBladeStatusResult.notYours);

            doLogin(uut, hostIP);

            // Find the parent blade of the VM we got, and make sure it is now unused.
            bladeState = uut.getBladeStatus(bladeSpec.bladeIP, hostIP);
            Assert.AreEqual(bladeState, GetBladeStatusResult.unused);
        }

        [TestMethod]
        public void willReUseOldVMsAfterLogon()
        {
            string hostIP = "1.1.1.1";

            hostStateDB_mocked uut = new hostStateDB_mocked();

            doLogin(uut, hostIP);
            string ourVM = doVMAllocationForTest(uut, hostIP);

            doLogin(uut, hostIP);
            ourVM = doVMAllocationForTest(uut, hostIP);
        }

        [TestMethod]
        public void willReUseOldVMsAfterLogonDuringBladeBoot()
        {
            string hostIP = "1.1.1.1";

            hostStateDB_mocked uut = new hostStateDB_mocked();

            doLogin(uut, hostIP);
            string waitToken = startSlowVMAllocationForTest(uut, hostIP);

            doLogin(uut, hostIP, TimeSpan.FromMinutes(10));
            string ourVM = doVMAllocationForTest(uut, hostIP);
        }

        private static void doLogin(hostStateDB_mocked uut, string hostIP, TimeSpan permissibleDelay = default(TimeSpan))
        {
            if (permissibleDelay == default(TimeSpan))
                permissibleDelay = TimeSpan.FromSeconds(30);

            string waitToken = uut.logIn(hostIP);
            DateTime timeout = DateTime.Now + permissibleDelay;
            while (true)
            {
                resultCode res = uut.getLogInProgress(waitToken);
                if (res == resultCode.success)
                    break;
                if (res == resultCode.pending)
                {
                    if (DateTime.Now > timeout)
                        throw new TimeoutException();

                    continue;
                }
                Assert.Fail("Unexpected status during .getLogInProgress: " + res);
            }
        }

        private static string doBladeAllocationForTest(hostStateDB_mocked uut, string hostIP, bool failTCPConnectAttempts = false)
        {
            uut.initWithBlades(new[] { "172.17.129.131" });

            uut.onMockedExecution += mockedAllocation.respondToExecutionsCorrectly;
            uut.onTCPConnectionAttempt += (ip, port, finish, error, time, state) => { return !failTCPConnectAttempts; };

            resultCodeAndBladeName allocRes = uut.RequestAnySingleNode(hostIP);
            Assert.AreEqual(resultCode.success, allocRes.code);

            return allocRes.bladeName;
        }

        private static string doVMAllocationForTest(hostStateDB_mocked uut, string hostIP, bool failTCPConnectAttempts = false)
        {
            uut.initWithBlades(new[] { "172.17.129.131" });

            uut.onMockedExecution += mockedAllocation.respondToExecutionsCorrectly;
            uut.onTCPConnectionAttempt += (ip, port, finish, error, time, state) => { return !failTCPConnectAttempts; };

            VMHardwareSpec hwspec = new VMHardwareSpec(1024 * 3, 1);
            VMSoftwareSpec swspec = new VMSoftwareSpec();
            resultCodeAndBladeName allocRes = uut.RequestAnySingleVM(hostIP, hwspec, swspec);
            string token = allocRes.waitToken;
            if (allocRes.code != resultCode.pending && allocRes.code != resultCode.success)
                Assert.Fail();

            DateTime deadline = DateTime.Now + TimeSpan.FromSeconds(30);
            while (true)
            {
                allocRes = uut.getProgressOfVMRequest(token);
                if (allocRes.code == resultCode.success)
                    break;
                Assert.AreEqual(resultCode.pending, allocRes.code);

                if (DateTime.Now > deadline)
                    throw new TimeoutException();
            }

            return allocRes.bladeName;
        }

        private static string startSlowVMAllocationForTest(hostStateDB_mocked uut, string hostIP)
        {
            uut.initWithBlades(new[] { "172.17.129.131" });

            uut.onMockedExecution += mockedAllocation.respondToExecutionsCorrectlyButSlowly;
            uut.onTCPConnectionAttempt += (ip, port, finish, error, time, state) => { return true; };

            VMHardwareSpec hwspec = new VMHardwareSpec(1024 * 3, 1);
            VMSoftwareSpec swspec = new VMSoftwareSpec();
            resultCodeAndBladeName allocRes = uut.RequestAnySingleVM(hostIP, hwspec, swspec);
            if (allocRes.code != resultCode.pending)
                Assert.Fail();

            return allocRes.waitToken;
        }
    }
}