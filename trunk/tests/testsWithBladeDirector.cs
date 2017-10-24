using bladeDirectorClient;
using bladeDirectorClient.bladeDirectorService;
using hypervisors;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using VMHardwareSpec = bladeDirectorClient.bladeDirectorService.VMHardwareSpec;
using VMSoftwareSpec = bladeDirectorClient.bladeDirectorService.VMSoftwareSpec;

namespace tests
{
    [TestClass]
    public class testsWithBladeDirector
    {
        [TestMethod]
        [TestCategory("requiresBladeDirector")]
        public void canAllocateVM()
        {
            using (bladeDirectorDebugServices svc = new bladeDirectorDebugServices(basicBladeTests.WCFPath))
            {
                string hostip = "172.16.10.91";

                bladeSpec spec = svc.svcDebug.createBladeSpec(
                    "172.17.129.131", "192.168.129.131", "172.17.2.131", 1234,
                    false, VMDeployStatus.notBeingDeployed, " ... ", bladeLockType.lockAll, bladeLockType.lockAll);

                svc.svcDebug.initWithBladesFromBladeSpec(new[] { spec }, false, NASFaultInjectionPolicy.retunSuccessful);

                machinePools.bladeDirectorURL = svc.servicesURL;

                VMHardwareSpec hw = new VMHardwareSpec()
                {
                    cpuCount = 1, 
                    memoryMB =  3000
                };
                VMSoftwareSpec sw = new VMSoftwareSpec()
                {
                    debuggerHost = "172.16.10.91", 
                    debuggerPort = 53000, 
                    debuggerKey = "a.b.c.d" 
                };
                using (hypervisorCollection<hypSpec_vmware> vm = machinePools.ilo.requestVMs(new VMSpec[] { new VMSpec() { hw = hw, sw = sw } }))
                {
                    // TODO: test the VM is allocated okay
                }
            }
        }
    }
}