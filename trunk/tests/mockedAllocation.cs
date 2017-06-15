using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using bladeDirector;
using hypervisors;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace tests
{
    [TestClass]
    public class mockedAllocation
    {
        [TestMethod]
        public void canAllocateVMMocked()
        {
            string hostIP = "1.1.1.1";

            hostStateDB_mocked uut = new hostStateDB_mocked();

            VMHardwareSpec hwSpec = new VMHardwareSpec() { memoryMB = 2344, cpuCount = 2 };
            VMSoftwareSpec swSpec = new VMSoftwareSpec();

            resultCodeAndBladeName[] allocRes = doVMAllocationsForTest(uut, hostIP, new[] { new vmHWAndSWSpec(hwSpec, swSpec) } );
            Assert.AreEqual(1, allocRes.Length);
            resultCodeAndBladeName allocatedBlade = allocRes[0];

            // TODO: check nas events
            List<mockedCall> nasEvents = uut.getNASEvents();
            foreach (mockedCall call in nasEvents)
                Debug.WriteLine(call.functionName + " " + call.message);
            // TODO: check executions all happened okay

            // This blade should become a VM server
            GetBladeStatusResult allocated1 = uut.getBladeStatus("172.17.129.131", hostIP);
            Assert.AreEqual(allocated1, GetBladeStatusResult.notYours);

            // And there should now be one VM allocated to us at present.
            Assert.AreEqual("172.17.159.1", allocatedBlade.bladeName);
            vmSpec VMConfig = uut.getVMByIP(allocatedBlade.bladeName);
            Assert.AreEqual("VM_31_01", VMConfig.displayName);
            Assert.AreEqual("172.17.159.1", VMConfig.VMIP);
            Assert.AreEqual("192.168.159.1", VMConfig.iscsiIP);
            Assert.AreEqual("00:50:56:00:31:01", VMConfig.eth0MAC);
            Assert.AreEqual("00:50:56:01:31:01", VMConfig.eth1MAC);
            Assert.AreEqual(2344, VMConfig.hwSpec.memoryMB);
            Assert.AreEqual(2, VMConfig.hwSpec.cpuCount);
            Assert.AreEqual(hostIP, VMConfig.currentOwner);
        }

        [TestMethod]
        public void canAllocateANumberOfVMMocked()
        {
            string hostIP = "1.1.1.1";

            hostStateDB_mocked uut = new hostStateDB_mocked();

            vmHWAndSWSpec[] toAlloc = new vmHWAndSWSpec[8];
            for (int i = 0; i < toAlloc.Length; i++)
            {
                toAlloc[i] = new vmHWAndSWSpec(
                    new VMHardwareSpec() {memoryMB = 4096, cpuCount = 1},
                    new VMSoftwareSpec());
            }

            resultCodeAndBladeName[] allocRes = doVMAllocationsForTest(uut, hostIP, toAlloc);
            Assert.AreEqual(toAlloc.Length, allocRes.Length);

            // Group blades by their parent blade's DB ID
            IGrouping<long, resultCodeAndBladeName>[] bladesByParent = allocRes.GroupBy(x => uut.getVMByIP(x.bladeName).parentBladeID).ToArray();

            // We should have two VM servers in use.
            Assert.AreEqual(2, bladesByParent.Length);

            // 5 should be on the first blade, and 3 on the second.
            Assert.AreEqual(5, bladesByParent[0].Count());
            Assert.AreEqual("172.17.129.131", uut.getConfigurationOfBladeByID((int) bladesByParent[0].Key).bladeIP);
            Assert.AreEqual(3, bladesByParent[1].Count());
            Assert.AreEqual("172.17.129.130", uut.getConfigurationOfBladeByID((int) bladesByParent[1].Key).bladeIP);

            // And release them, checking hardware status after each blade is empty.
            foreach (IGrouping<long, resultCodeAndBladeName> bladeAndParent in bladesByParent)
            {
                string parentBladeName = uut.getConfigurationOfBladeByID((int) bladeAndParent.Key).bladeIP;

                foreach (resultCodeAndBladeName res in bladeAndParent)
                {
                    // The VM server should still be allocated before release..
                    Assert.AreEqual(uut.getBladeStatus(parentBladeName, hostIP), GetBladeStatusResult.notYours);
                    uut.releaseBladeOrVM(res.bladeName, hostIP);
                }

                // This VM server should now be unused.
                Assert.AreEqual(uut.getBladeStatus(parentBladeName, hostIP), GetBladeStatusResult.unused);
            }
        }

        public static resultCodeAndBladeName[] doVMAllocationsForTest(hostStateDB_mocked uut, string hostIP, vmHWAndSWSpec[] specs)
        {
            uut.initWithBlades(new[] {"172.17.129.131", "172.17.129.130"});

            uut.onMockedExecution += respondToExecutionsCorrectly;
            uut.onTCPConnectionAttempt += (ip, port, finish, error, time, state) => { return true; };

            return doAllocation(uut, hostIP, specs);
        }

        private static resultCodeAndBladeName[] doAllocation(hostStateDB_mocked uut, string hostIP, vmHWAndSWSpec[] specs )
        {
            resultCodeAndBladeName[] allocRes = new resultCodeAndBladeName[specs.Length];

            for (int i = 0; i < specs.Length; i++)
            {
                allocRes[i] = uut.RequestAnySingleVM(hostIP, specs[i].hw, specs[i].sw);
                Assert.AreEqual(resultCode.pending, allocRes[i].code);
            }

            // Wait until all the allocation operations are complete
            DateTime deadline = DateTime.Now + TimeSpan.FromMinutes(15);
            for (int i = 0; i < specs.Length; i++)
            {
                string waitToken = allocRes[i].waitToken;

                while (true)
                {
                    allocRes[i] = uut.getProgressOfVMRequest(waitToken);
                    if (allocRes[i].code == resultCode.pending)
                    {
                        // .. keep waiting ..
                        Thread.Sleep(TimeSpan.FromSeconds(10));
                    }
                    else if (allocRes[i].code == resultCode.success)
                    {
                        // Allocation has finished, yay
                        break;
                    }
                    else
                    {
                        Assert.Fail("unexpected state during VM provisioning: " + allocRes[i].code);
                    }

                    if (DateTime.Now > deadline)
                        throw new TimeoutException();
                }
            }
            return allocRes;
        }

        public static executionResult respondToExecutionsCorrectly(hypervisor sender, string command, string args, string dir, DateTime deadline)
        {
            string commandLine = command + " " + args;
            switch (commandLine)
            {
                case "bash ~/applyBIOS.sh":
                    return new executionResult("bios stuff", "", 0);
                case "esxcfg-nas -l":
                    return new executionResult("esxivms is /mnt/SSDs/esxivms from store.xd.lan mounted available", null, 0);
                case @"C:\windows\system32\cmd /c shutdown -s -f -t 01":
                    sender.powerOff();
                    return new executionResult("", "", 0);
            }

            if (commandLine.StartsWith("vim-cmd vmsvc/power.off `vim-cmd vmsvc/getallvms | grep"))
                return new executionResult("", null, 0);
            if (commandLine.StartsWith("vim-cmd vmsvc/unregister `vim-cmd vmsvc/getallvms"))
                return new executionResult("", null, 0);
            if (commandLine.StartsWith("rm  -rf /vmfs/volumes/esxivms/"))
                return new executionResult("", null, 0);
            if (commandLine.StartsWith("cp  -R /vmfs/volumes/esxivms/PXETemplate /vmfs/volumes/esxivms/"))
                return new executionResult("", null, 0);
            if (commandLine.StartsWith("sed  -e "))
                return new executionResult("", null, 0);
            if (commandLine.StartsWith("vim-cmd  solo/registervm /vmfs/volumes/esxivms/"))
                return new executionResult("", null, 0);
            if (commandLine.StartsWith("cmd.exe /c c:\\deployed.bat "))
                return new executionResult("", null, 0);

            Assert.Fail("executed unexpected command " + commandLine);
            return new executionResult("idk", "idk", -1);
        }
    }

    public class vmHWAndSWSpec
    {
        public VMSoftwareSpec sw;
        public VMHardwareSpec hw;

        public vmHWAndSWSpec(VMHardwareSpec newHWSpec, VMSoftwareSpec newSWSpec)
        {
            sw = newSWSpec;
            hw = newHWSpec;
        }
    }
}