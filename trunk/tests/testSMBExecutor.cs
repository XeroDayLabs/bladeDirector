using System;
using System.Threading;
using bladeDirectorClient;
using hypervisors;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace tests
{
    [TestClass]
    public class testSMBExecutor
    {
        [TestMethod]
        public void testSMBExecutorReturnsStdOutErrAndRetCode()
        {
            doExecTest(clientExecutionMethod.smb);
        }

        [TestMethod]
        public void testSMBExecutorReturnsStdOutErrAndRetCodeAsync()
        {
            doExecTestAsync(clientExecutionMethod.smb);
        }

        [TestMethod]
        public void testSMBExecutorWillSetWorkingDirectoryCorrectly()
        {
            doWorkingDirTest(clientExecutionMethod.smb, "C:\\");
            doWorkingDirTest(clientExecutionMethod.smb, "C:\\Windows");
        }

        public static void doExecTestAsync(clientExecutionMethod exec)
        {
            using (hypervisor_vmware hyp = machinePools.vmware.createHypervisorForNextFreeVMOrWait(execType: exec))
            {
                prepVM(hyp);
                IAsyncExecutionResult asyncRes = null;
                while (asyncRes == null)
                    asyncRes = hyp.startExecutableAsync("cmd.exe", "/c choice /t 5 /d y >nul & echo This is a test&echo and stderr 1>&2 & exit /b 1234");

                executionResult res = null;
                while (res == null)
                {
                    res = asyncRes.getResultIfComplete();
                    Thread.Sleep(TimeSpan.FromSeconds(1));
                }

                Assert.AreEqual("This is a test\r\n", res.stdout);
                Assert.AreEqual("and stderr  \r\n", res.stderr);
                Assert.AreEqual(1234, res.resultCode);
            }
        }

        public static void doExecTest(clientExecutionMethod exec)
        {
            using (hypervisor_vmware hyp = machinePools.vmware.createHypervisorForNextFreeVMOrWait(execType: exec))
            {
                prepVM(hyp);
                executionResult res = hyp.startExecutable("cmd.exe", "/c echo This is a test&&echo and stderr 1>&2 && exit /b 1234");
                Assert.AreEqual("This is a test\r\n", res.stdout);
                Assert.AreEqual("and stderr  \r\n", res.stderr);
                Assert.AreEqual(1234, res.resultCode);
            }
        }

        public static void doWorkingDirTest(clientExecutionMethod exec, string newWorkingDir)
        {
            using (hypervisor_vmware hyp = machinePools.vmware.createHypervisorForNextFreeVMOrWait(execType: exec))
            {
                prepVM(hyp);
                executionResult res = hyp.startExecutable("cmd /c", "cd", newWorkingDir);
                Assert.AreEqual(newWorkingDir + "\r\n", res.stdout);
            }
        }

        public static void prepVM(hypervisor_vmware hypervisor)
        {
            hypervisor.connect();
            hypervisor.powerOff();
            hypervisor.restoreSnapshot();
            hypervisor.powerOn();
        }
    }
}