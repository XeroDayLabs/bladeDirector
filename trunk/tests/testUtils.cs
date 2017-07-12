using System;
using bladeDirector;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace tests
{
    public static class testUtils
    {
        public static void doLogin(hostStateManagerMocked uut, string hostIP, TimeSpan permissibleDelay = default(TimeSpan))
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

        public static string doBladeAllocationForTest(hostStateManagerMocked uut, string hostIP, bool failTCPConnectAttempts = false)
        {
            uut.initWithBlades(new[] { "172.17.129.131" });

            uut.onMockedExecution += mockedAllocation.respondToExecutionsCorrectly;
            uut.onTCPConnectionAttempt += (ip, port, finish, error, time, state) => { return !failTCPConnectAttempts; };

            resultCodeAndBladeName allocRes = uut.RequestAnySingleNode(hostIP);
            Assert.AreEqual(resultCode.success, allocRes.code);

            return allocRes.bladeName;
        }

        public static string doVMAllocationForTest(hostStateManagerMocked uut, string hostIP, bool failTCPConnectAttempts = false)
        {
            uut.onMockedExecution += mockedAllocation.respondToExecutionsCorrectly;
            uut.onTCPConnectionAttempt += (ip, port, finish, error, time, state) => { return !failTCPConnectAttempts; };

            VMHardwareSpec hwspec = new VMHardwareSpec(1024 * 3, 1);
            VMSoftwareSpec swspec = new VMSoftwareSpec();
            resultAndBladeName allocRes = uut.RequestAnySingleVM(hostIP, hwspec, swspec);
            waitTokenType token = allocRes.waitToken;
            if (allocRes.result.code != resultCode.pending && allocRes.result.code != resultCode.success)
                Assert.Fail(allocRes.result.ToString());

            DateTime deadline = DateTime.Now + TimeSpan.FromSeconds(30);
            while (true)
            {
                allocRes = uut.getProgressOfVMRequest(token);
                if (allocRes.result.code == resultCode.success)
                    break;
                Assert.AreEqual(resultCode.pending, allocRes.result.code);

                if (DateTime.Now > deadline)
                    throw new TimeoutException();
            }

            return allocRes.bladeName;
        }

        public static waitTokenType startSlowVMAllocationForTest(hostStateManagerMocked uut, string hostIP)
        {
            uut.initWithBlades(new[] { "172.17.129.131" });

            uut.onMockedExecution += mockedAllocation.respondToExecutionsCorrectlyButSlowly;
            uut.onTCPConnectionAttempt += (ip, port, finish, error, time, state) => { return true; };

            VMHardwareSpec hwspec = new VMHardwareSpec(1024 * 3, 1);
            VMSoftwareSpec swspec = new VMSoftwareSpec();
            resultAndBladeName allocRes = uut.RequestAnySingleVM(hostIP, hwspec, swspec);
            if (allocRes.result.code != resultCode.pending)
                Assert.Fail();

            return allocRes.waitToken;
        }
    }
}