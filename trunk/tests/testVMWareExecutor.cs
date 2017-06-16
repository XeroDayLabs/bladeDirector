using hypervisors;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace tests
{
    [TestClass]
    public class testVMWareExecutor
    {
        [TestMethod]
        public void testVMWareExecutorReturnsStdOutErrAndRetCode()
        {
            testSMBExecutor.doExecTest(clientExecutionMethod.vmwaretools);
        }

        [TestMethod]
        public void testVMWareExecutorReturnsStdOutErrAndRetCodeAsync()
        {
            testSMBExecutor.doExecTestAsync(clientExecutionMethod.vmwaretools);
        }

        [TestMethod]
        public void testVMWareExecutorWillSetWorkingDirectoryCorrectly()
        {
            testSMBExecutor.doWorkingDirTest(clientExecutionMethod.vmwaretools, "C:\\");
            testSMBExecutor.doWorkingDirTest(clientExecutionMethod.vmwaretools, "C:\\Windows");
        }
    }
}