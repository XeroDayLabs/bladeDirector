using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using tests.bladeDirectorServices;

namespace tests
{
    public static class testUtils
    {
        public static void doLogin(services uut, string hostIP, TimeSpan permissibleDelay = default(TimeSpan))
        {
            if (permissibleDelay == default(TimeSpan))
                permissibleDelay = TimeSpan.FromSeconds(30);

            resultAndWaitToken res = uut.uutDebug._logIn(hostIP);
            res = waitForSuccess(uut, res, permissibleDelay);
        }

        public static string doBladeAllocationForTest(services uut, string hostIP)
        {
            uut.uutDebug._setExecutionResultsIfMocked(mockedExecutionResponses.successful);

            resultAndWaitToken allocRes = uut.uutDebug._RequestAnySingleNode(hostIP);
            Assert.AreEqual(resultCode.success, allocRes.result.code);


            return ((resultAndBladeName)allocRes).bladeName;
        }

        public static string doVMAllocationForTest(services uut, string hostIP)
        {
            uut.uutDebug._setExecutionResultsIfMocked(mockedExecutionResponses.successful);

            VMHardwareSpec hwspec = new VMHardwareSpec
            {
                cpuCount = 1,
                memoryMB = 1024*3
            };
            VMSoftwareSpec swspec = new VMSoftwareSpec();

            resultAndWaitToken res = uut.uutDebug._requestAnySingleVM(hostIP, hwspec, swspec);
            res = waitForSuccess(uut, res, TimeSpan.FromSeconds(30));
            return ((resultAndBladeName)res).bladeName;
        }

        public static resultAndWaitToken waitForSuccess(services uut, resultAndWaitToken res, TimeSpan timeout)
        {
            DateTime deadline = DateTime.Now + timeout;
            while (res.result.code != resultCode.success)
            {
                switch (res.result.code)
                {
                    case resultCode.success:
                    case resultCode.noNeedLah:
                        break;

                    case resultCode.pending:
                        if (DateTime.Now > deadline)
                            throw new TimeoutException();
                        res = uut.uut.getProgress(res.waitToken);
                        continue;

                    default:
                        Assert.Fail("Unexpected status during .getProgress: " + res.result.code + " / " + res.result.errMsg);
                        break;
                }
                Thread.Sleep(TimeSpan.FromSeconds(1));
            }
            return res;
        }

        public static resultAndBladeName startAsyncVMAllocationForTest(services uut, string hostIP)
        {
            uut.uutDebug._setExecutionResultsIfMocked(mockedExecutionResponses.successful);

            VMHardwareSpec hwspec = new VMHardwareSpec
            {
                cpuCount = 1,
                memoryMB = 1024 * 3
            };
            VMSoftwareSpec swspec = new VMSoftwareSpec();

            resultAndBladeName allocRes = uut.uutDebug._requestAnySingleVM(hostIP, hwspec, swspec);
            if (allocRes.result.code != resultCode.pending && allocRes.result.code != resultCode.success)
                Assert.Fail("unexpected status: " + allocRes.result.code.ToString());

            return allocRes;
        }

    }
}