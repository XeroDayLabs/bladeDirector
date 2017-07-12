using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using bladeDirector;
using hypervisors;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Win32;

namespace tests
{
    [TestClass]
    public class mockedAllocation
    {
        [TestMethod]
        public void canAllocateVMMocked()
        {
            string hostIP = "1.1.1.1";

            hostStateManagerMocked uut = new hostStateManagerMocked();

            VMHardwareSpec hwSpec = new VMHardwareSpec() { memoryMB = 2344, cpuCount = 2 };
            VMSoftwareSpec swSpec = new VMSoftwareSpec();

            resultAndBladeName[] allocRes = doVMAllocationsForTest(uut, hostIP, new[] { new vmHWAndSWSpec(hwSpec, swSpec) } );
            Assert.AreEqual(1, allocRes.Length);
            resultAndBladeName allocatedBlade = allocRes[0];

            // TODO: check nas events
            List<mockedCall> nasEvents = uut.getNASEvents();
            foreach (mockedCall call in nasEvents)
                Debug.WriteLine(call.functionName + " " + call.message);
            // TODO: check executions all happened okay

            // This blade should become a VM server
            GetBladeStatusResult allocated1 = uut.db.getBladeStatus("172.17.129.130", hostIP);
            Assert.AreEqual(allocated1, GetBladeStatusResult.notYours);

            // And there should now be one VM allocated to us at present.
            Assert.AreEqual("172.17.158.1", allocatedBlade.bladeName);
            using (lockableVMSpec VMConfig = uut.db.getVMByIP(allocatedBlade.bladeName))
            {
                Assert.AreEqual("VM_30_01", VMConfig.spec.displayName);
                Assert.AreEqual("172.17.158.1", VMConfig.spec.VMIP);
                Assert.AreEqual("192.168.158.1", VMConfig.spec.iscsiIP);
                Assert.AreEqual("00:50:56:00:30:01", VMConfig.spec.eth0MAC);
                Assert.AreEqual("00:50:56:01:30:01", VMConfig.spec.eth1MAC);
                Assert.AreEqual(2344, VMConfig.spec.hwSpec.memoryMB);
                Assert.AreEqual(2, VMConfig.spec.hwSpec.cpuCount);
                Assert.AreEqual(hostIP, VMConfig.spec.currentOwner);
            }
        }

        [TestMethod]
        public void canAllocateANumberOfVMMocked()
        {
            string hostIP = "1.1.1.1";

            hostStateManagerMocked uut = new hostStateManagerMocked();

            vmHWAndSWSpec[] toAlloc = new vmHWAndSWSpec[8];
            for (int i = 0; i < toAlloc.Length; i++)
            {
                toAlloc[i] = new vmHWAndSWSpec(
                    new VMHardwareSpec() {memoryMB = 4096, cpuCount = 1},
                    new VMSoftwareSpec());
            }

            resultAndBladeName[] allocRes = doVMAllocationsForTest(uut, hostIP, toAlloc);
            Assert.AreEqual(toAlloc.Length, allocRes.Length);

            // Group blades by their parent blade's IP
            IGrouping<string, resultAndBladeName>[] bladesByParent = allocRes.GroupBy(x => uut.db.getVMByIP_withoutLocking(x.bladeName).parentBladeIP).ToArray();

            // We should have two VM servers in use.
            Assert.AreEqual(2, bladesByParent.Length);

            // 5 should be on the first blade, and 3 on the second.
            Assert.AreEqual(5, bladesByParent[0].Count());
            Assert.AreEqual("172.17.129.130", bladesByParent[0].Key);
            Assert.AreEqual(3, bladesByParent[1].Count());
            Assert.AreEqual("172.17.129.131", bladesByParent[1].Key);

            // And release them, checking hardware status after each blade is empty.
            foreach (IGrouping<string, resultAndBladeName> bladeAndParent in bladesByParent)
            {
                //string parentBladeName = uut.db.getConfigurationOfBladeByID((int)bladeAndParent.Key, bladeLockType.lockNone).spec.bladeIP;

                foreach (resultAndBladeName res in bladeAndParent)
                {
                    // The VM server should still be allocated before release..
                    Assert.AreEqual(uut.db.getBladeStatus(bladeAndParent.Key, hostIP), GetBladeStatusResult.notYours);
                    uut.releaseBladeOrVM(res.bladeName, hostIP);
                }

                // This VM server should now be unused.
                Assert.AreEqual(uut.db.getBladeStatus(bladeAndParent.Key, hostIP), GetBladeStatusResult.unused);
            }
        }

        public static resultAndBladeName[] doVMAllocationsForTest(hostStateManagerMocked uut, string hostIP, vmHWAndSWSpec[] specs)
        {
            uut.initWithBlades(new[] {"172.17.129.131", "172.17.129.130"});

            uut.onMockedExecution += respondToExecutionsCorrectly;
            uut.onTCPConnectionAttempt += (ip, port, finish, error, time, state) => { return true; };

            return doAllocation(uut, hostIP, specs);
        }

        private static resultAndBladeName[] doAllocation(hostStateManagerMocked uut, string hostIP, vmHWAndSWSpec[] specs )
        {
            resultAndBladeName[] allocRes = new resultAndBladeName[specs.Length];

            for (int i = 0; i < specs.Length; i++)
            {
                allocRes[i] = uut.RequestAnySingleVM(hostIP, specs[i].hw, specs[i].sw);
                Assert.AreEqual(resultCode.pending, allocRes[i].result.code);
            }

            // Wait until all the allocation operations are complete
            DateTime deadline = DateTime.Now + TimeSpan.FromMinutes(15);
            for (int i = 0; i < specs.Length; i++)
            {
                waitTokenType waitToken = allocRes[i].waitToken;

                while (true)
                {
                    allocRes[i] = uut.getProgressOfVMRequest(waitToken);
                    if (allocRes[i].result.code == resultCode.pending)
                    {
                        // .. keep waiting ..
                        Thread.Sleep(TimeSpan.FromSeconds(10));
                    }
                    else if (allocRes[i].result.code == resultCode.success)
                    {
                        // Allocation has finished, yay
                        break;
                    }
                    else
                    {
                        Assert.Fail("unexpected state during VM provisioning: " + allocRes[i].result.code);
                    }

                    if (DateTime.Now > deadline)
                        throw new TimeoutException();
                }
            }
            return allocRes;
        }

        public static executionResult respondToExecutionsCorrectlyButSlowly(hypervisor sender, string command, string args, string dir, DateTime deadline)
        {
            Thread.Sleep(TimeSpan.FromSeconds(3));
            return respondToExecutionsCorrectly(sender, command, args, dir, deadline);
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