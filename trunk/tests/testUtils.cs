using System;
using System.ComponentModel;
using System.Net;
using System.Runtime.InteropServices;
using System.Threading;
using bladeDirectorWCF;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace tests
{
    public static class testUtils
    {
        public static void doLogin(bladeDirectorDebugServices uut, string hostIP,
            TimeSpan permissibleDelay = default(TimeSpan))
        {
            if (permissibleDelay == default(TimeSpan))
                permissibleDelay = TimeSpan.FromSeconds(30);

            resultAndWaitToken res = uut.svcDebug._logIn(hostIP);
            res = waitForSuccess(uut, res, permissibleDelay);
        }

        public static string doBladeAllocationForTest(bladeDirectorDebugServices uut, string hostIP)
        {
            uut.svcDebug._setExecutionResultsIfMocked(mockedExecutionResponses.successful);

            resultAndWaitToken allocRes = uut.svcDebug._RequestAnySingleNode(hostIP);
            Assert.AreEqual(resultCode.success, allocRes.result.code);


            return ((resultAndBladeName) allocRes).bladeName;
        }

        public static string doVMAllocationForTest(bladeDirectorDebugServices uut, string hostIP)
        {
            uut.svcDebug._setExecutionResultsIfMocked(mockedExecutionResponses.successful);

            VMHardwareSpec hwspec = new VMHardwareSpec
            {
                cpuCount = 1,
                memoryMB = 1024*3
            };
            VMSoftwareSpec swspec = new VMSoftwareSpec();

            resultAndWaitToken res = uut.svcDebug._requestAnySingleVM(hostIP, hwspec, swspec);
            res = waitForSuccess(uut, res, TimeSpan.FromSeconds(30));
            return ((resultAndBladeName) res).bladeName;
        }

        public static resultAndWaitToken waitForSuccess(bladeDirectorDebugServices uut, resultAndWaitToken res,
            TimeSpan timeout)
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
                        res = uut.svc.getProgress(res.waitToken);
                        break;
                    default:
                        Assert.Fail("Unexpected status during .getProgress: " + res.result.code + " / " +
                                    res.result.errMsg);
                        break;
                }
                Thread.Sleep(TimeSpan.FromSeconds(1));
            }
            return res;
        }

        public static resultAndBladeName startAsyncVMAllocationForTest(bladeDirectorDebugServices uut, string hostIP)
        {
            uut.svcDebug._setExecutionResultsIfMocked(mockedExecutionResponses.successful);

            VMHardwareSpec hwspec = new VMHardwareSpec
            {
                cpuCount = 1,
                memoryMB = 1024*3
            };
            VMSoftwareSpec swspec = new VMSoftwareSpec();

            resultAndBladeName allocRes = uut.svcDebug._requestAnySingleVM(hostIP, hwspec, swspec);
            if (allocRes.result.code != resultCode.pending && allocRes.result.code != resultCode.success)
                Assert.Fail("unexpected status: " + allocRes.result.code.ToString());

            return allocRes;
        }
    }
}