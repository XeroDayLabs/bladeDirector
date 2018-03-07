using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using bladeDirectorClient;
using bladeDirectorWCF;
using hypervisors;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace tests
{
    [TestClass]
    public class testsWithRealBlade
    { 
        [TestMethod]
        public void willSetBIOS()
        {
            string testBiosXML = Properties.Resources.testBIOS;

            using (bladeDirectorDebugServices svc = new bladeDirectorDebugServices(basicBladeTests.WCFPath, basicBladeTests.WebURI))
            {
                machinePools.bladeDirectorURL = svc.servicesURL;
                string hostip = "1.2.3.4";

                // We will be using this blade for our tests.
                bladeSpec spec = svc.svcDebug.createBladeSpec("172.17.129.131", "192.168.129.131", "172.17.2.131", 1234, false, VMDeployStatus.notBeingDeployed, " ... ", "idk", "box", bladeLockType.lockAll, bladeLockType.lockAll);
                svc.svcDebug.initWithBladesFromBladeSpec(new[] { spec }, false, NASFaultInjectionPolicy.retunSuccessful);

                resultAndBladeName res = svc.svcDebug._RequestAnySingleNode(hostip);
                Assert.AreEqual(resultCode.success, res.result.code);

                // Write the new BIOS. This configuration has the 'boot state' of the numlock key set to 'off', so we can
                // read that back after writing to ensure that the BIOS write succeeds.
                var resultAndReadBack = writeBIOSAndReadBack(svc, hostip, res.bladeName, testBiosXML);
                Assert.IsTrue(resultAndReadBack.BIOSConfig.Contains("<Section name=\"NumLock\">Off</Section>"));
                // Now we can modify the BIOS slightly, to set the numlock boot state to 'on', and again write/readback, and see
                // if the change was carried out.
                testBiosXML = testBiosXML.Replace("<Section name=\"NumLock\">Off</Section>", "<Section name=\"NumLock\">On</Section>");
                resultAndReadBack = writeBIOSAndReadBack(svc, hostip, res.bladeName, testBiosXML);
                Assert.IsTrue(resultAndReadBack.BIOSConfig.Contains("<Section name=\"NumLock\">On</Section>"));

                resultAndWaitToken relRes = svc.svcDebug._ReleaseBladeOrVM(hostip, res.bladeName, false);
                while (relRes.result.code == resultCode.pending)
                {
                    Thread.Sleep(TimeSpan.FromSeconds(3));
                    relRes = svc.svc.getProgress(relRes.waitToken);
                }
                Assert.AreEqual(resultCode.success, relRes.result.code);
            }
        }

        [TestMethod]
        public void willCancelBIOSWrite()
        {
            willCancelBIOSWrite(TimeSpan.FromSeconds(1));
        }

        [TestMethod]
        [TestCategory("disabledTests")]
        public void willCancelBIOSWriteAfterWaiting()
        {
            for (int mins = 1; mins < 10; mins++)
                willCancelBIOSWrite(TimeSpan.FromMinutes(mins));
        }

        public void willCancelBIOSWrite(TimeSpan delay)
        {
            using (bladeDirectorDebugServices svc = new bladeDirectorDebugServices(basicBladeTests.WCFPath, basicBladeTests.WebURI))
            {
                machinePools.bladeDirectorURL = svc.servicesURL;

                string hostIP = "1.1.1.1";
                bladeSpec spec = svc.svcDebug.createBladeSpec("172.17.129.131", "192.168.129.131", "172.17.2.131", 1234, false, VMDeployStatus.notBeingDeployed, " ... ", "idk", "box", bladeLockType.lockAll, bladeLockType.lockAll);
                svc.svcDebug.initWithBladesFromBladeSpec(new[] { spec }, false, NASFaultInjectionPolicy.retunSuccessful);

                resultAndBladeName res = svc.svcDebug._RequestAnySingleNode(hostIP);
                Assert.AreEqual(resultCode.success, res.result.code);
                string ourBlade = ((resultAndBladeName)testUtils.waitForSuccess(svc, res, TimeSpan.FromSeconds(30))).bladeName;

                // Start a BIOS read
                resultAndWaitToken readRes = svc.svcDebug._rebootAndStartReadingBIOSConfiguration(hostIP, ourBlade);
                Assert.AreEqual(resultCode.pending, readRes.result.code);

                // Wait a while to see if we get somewhere where it is impossible to free without a hueg delay
                Thread.Sleep(delay);

                // Then free the blade. The BIOS operation should be cancelled before it soaks up all the ten minutes of time.
                resultAndWaitToken releaseRes = svc.svcDebug._ReleaseBladeOrVM(hostIP, ourBlade, false);
                testUtils.waitForSuccess(svc, releaseRes, TimeSpan.FromMinutes(1));

                // And it should be no longer getting.
                resultAndWaitToken bladeState = svc.svc.getProgress(readRes.waitToken);
                Assert.AreNotEqual(resultCode.pending, bladeState.result.code);

                // It no longer be allocated to us, since we released it earlier.
                GetBladeStatusResult ownershipRes = svc.svc.GetBladeStatus(ourBlade);
                Assert.AreEqual(GetBladeStatusResult.unused, ownershipRes);
            }
        }

        [TestMethod]
        public void willProvisionVM()
        {
            using (bladeDirectorDebugServices svc = new bladeDirectorDebugServices(basicBladeTests.WCFPath, basicBladeTests.WebURI))
            {
                machinePools.bladeDirectorURL = svc.servicesURL;

                string hostip = "1.2.3.4";

                // We will be using this blade for our tests.
                bladeSpec spec = svc.svcDebug.createBladeSpecForXDLNode(31, "xdl.hacks.the.planet", bladeLockType.lockAll, bladeLockType.lockAll);
                svc.svcDebug.initWithBladesFromBladeSpec(new[] { spec }, false, NASFaultInjectionPolicy.retunSuccessful);

                string debuggerHost = ipUtils.getBestRouteTo(IPAddress.Parse(spec.bladeIP)).ToString();
                VMSoftwareSpec sw = new VMSoftwareSpec()
                {
                    debuggerHost = debuggerHost,
                    debuggerKey = "a.b.c.d",
                    debuggerPort = 60234
                };
                VMHardwareSpec hw = new VMHardwareSpec() { cpuCount = 1, memoryMB = 4096 };
                resultAndBladeName res = svc.svcDebug._requestAnySingleVM(hostip, hw, sw);
                testUtils.waitForSuccess(svc, res, TimeSpan.FromMinutes(15));

                string VMName = res.bladeName;

                // Okay, we have our new VM allocated now. 
                vmSpec createdBlade = svc.svc.getVMByIP_withoutLocking(VMName);
                bladeSpec parentBlade = svc.svc.getBladeByIP_withoutLocking(createdBlade.parentBladeIP);
                snapshotDetails snap = svc.svc.getCurrentSnapshotDetails(VMName);
                NASParams nas = svc.svc.getNASParams();
                using (hypervisor_vmware_FreeNAS hyp = utils.createHypForVM(createdBlade, parentBlade, snap, nas))
                {
                    hyp.powerOn(new cancellableDateTime(TimeSpan.FromMinutes(2)));

                    // Check that debugging has been provisioned correctly
                    executionResult bcdEditRes = hyp.startExecutable("bcdedit", "/dbgsettings");
                    try
                    {
                        Assert.AreEqual(0, bcdEditRes.resultCode);
                        Assert.IsTrue(Regex.IsMatch(bcdEditRes.stdout, "key\\s*a.b.c.d"));
                        Assert.IsTrue(Regex.IsMatch(bcdEditRes.stdout, "debugtype\\s*NET"));
                        Assert.IsTrue(Regex.IsMatch(bcdEditRes.stdout, "hostip\\s*" + hostip));
                        Assert.IsTrue(Regex.IsMatch(bcdEditRes.stdout, "port\\s*60234"));
                    }
                    catch (AssertFailedException)
                    {
                        Debug.WriteLine("bcdedit stdout " + bcdEditRes.stdout);
                        Debug.WriteLine("bcdedit stderr " + bcdEditRes.stderr);

                        throw;
                    }

                    executionResult wmicRes = hyp.startExecutable("wmic", "computersystem get totalPhysicalMemory,name,numberOfLogicalProcessors /format:value");
                    try
                    {
                        // We expect an response similar to:
                        //
                        // Name=ALIZANALYSIS
                        // NumberOfLogicalProcessors=8
                        // TotalPhysicalMemory=17119825920
                        //

                        Assert.AreEqual(0, wmicRes.resultCode);
                        string[] lines = wmicRes.stdout.Split(new[] { '\n', 'r' }, StringSplitOptions.RemoveEmptyEntries);
                        foreach (string line in lines)
                        {
                            if (line.Trim().Length == 0)
                                continue;

                            string[] parts = line.Split('=');

                            string name = parts[0].ToLower().Trim();
                            string value = parts[1].ToLower().Trim();

                            switch (name)
                            {
                                case "name":
                                    Assert.AreEqual("VM_31_01", value, "Name is incorrect");
                                    break;
                                case "NumberOfLogicalProcessors":
                                    Assert.AreEqual("1", value, "CPU count is incorrect");
                                    break;
                                case "TotalPhysicalMemory":
                                    Assert.AreEqual((4096L * 1024L * 1024L).ToString(), value, "RAM size is incorrect");
                                    break;
                                default:
                                    break;
                            }
                        }
                    }
                    catch (AssertFailedException)
                    {
                        Debug.WriteLine("WMIC reported stdout '" + wmicRes.stdout + "'");
                        Debug.WriteLine("WMIC reported stderr '" + wmicRes.stderr + "'");
                    }
                }
            }
        }

        [TestMethod]
        [TestCategory("requiresBladeDirector")]
        public void willProvisionManyVM()
        {
            using (bladeDirectorDebugServices svc = new bladeDirectorDebugServices(basicBladeTests.WCFPath, basicBladeTests.WebURI))
            {
                machinePools.bladeDirectorURL = svc.servicesURL;

                // Create four blades, and request a load of VMs from them.
                List<bladeSpec> specs = new List<bladeSpec>();
                for (int bladeID = 28; bladeID < 32; bladeID++)
                {
                    specs.Add(svc.svcDebug.createBladeSpecForXDLNode(bladeID, "xdl.hacks.the.planet", bladeLockType.lockAll, bladeLockType.lockAll));
                }
                svc.svcDebug.initWithBladesFromBladeSpec(specs.ToArray(), false, NASFaultInjectionPolicy.retunSuccessful);

                // Ask for lots of VMs. We will get back only those that 'fit' on the cluster.
                List<VMSpec> requestedVMSpecs = new List<VMSpec>();
                foreach (bladeSpec thisBlade in specs)
                {
                    string debuggerHost = ipUtils.getBestRouteTo(IPAddress.Parse(thisBlade.bladeIP)).ToString();

                    // Add fifty VMs for each blade. Don't forget, we will only get those that 
                    // 'fit' on the cluster.
                    for (int vmCount = 0; vmCount < 50; vmCount++)
                    {
                        requestedVMSpecs.Add(new VMSpec()
                        {
                            hw = new VMHardwareSpec() {cpuCount = 1, memoryMB = 2048},
                            sw = new VMSoftwareSpec()
                            {
                                debuggerHost = debuggerHost,
                                debuggerKey = "a.b.c.d",
                                debuggerPort = 0    // auto
                            }
                        });
                    }
                }

                using (hypervisorCollection<hypSpec_vmware> vms = machinePools.ilo.requestVMs(requestedVMSpecs.ToArray(), true))
                {
                    // Okay, we have our new VMs allocated now. Lets verify them all. 
                    // Make a list of the created computer names and debug ports, since they are different for each request, and 
                    // make sure that they are all different and in the expected range.
                    List<int> debugPorts = new List<int>();
                    List<string> displayNames = new List<string>();
                    // We test each VM in parallel, otherwise things are reaaaally slow.
                    List<Thread> VMTestThreads = new List<Thread>(); 
                    foreach (hypervisorWithSpec<hypSpec_vmware> hyp in vms.Values)
                    {
                        Thread vmTest = new Thread(() =>
                        {
                            hyp.powerOn(new cancellableDateTime(TimeSpan.FromMinutes(10)));

                            // Check that debugging has been provisioned correctly
                            executionResult bcdEditRes = hyp.startExecutable("bcdedit", "/dbgsettings");
                            try
                            {
                                Assert.AreEqual(0, bcdEditRes.resultCode);
                                Assert.IsTrue(Regex.IsMatch(bcdEditRes.stdout, "key\\s*a.b.c.d"));
                                Assert.IsTrue(Regex.IsMatch(bcdEditRes.stdout, "debugtype\\s*NET"));
                                Assert.IsTrue(Regex.IsMatch(bcdEditRes.stdout, "hostip\\s*127.0.0.1"));
                                // verify port assignment and extract the port 
                                Match m = Regex.Match(bcdEditRes.stdout, "port\\s*([0-9]+)", RegexOptions.IgnoreCase);
                                Assert.IsTrue(m.Success);
                                int port = Int32.Parse(m.Groups[1].Value);
                                debugPorts.Add(port);
                            }
                            catch (AssertFailedException)
                            {
                                Debug.WriteLine("bcdedit stdout " + bcdEditRes.stdout);
                                Debug.WriteLine("bcdedit stderr " + bcdEditRes.stderr);

                                throw;
                            }

                            executionResult wmicRes = hyp.startExecutable("wmic", "computersystem get totalPhysicalMemory,name,numberOfLogicalProcessors /format:value");
                            try
                            {
                                // We expect an response similar to:
                                //
                                // Name=ALIZANALYSIS
                                // NumberOfLogicalProcessors=8
                                // TotalPhysicalMemory=17119825920
                                //

                                Assert.AreEqual(0, wmicRes.resultCode);
                                string[] lines = wmicRes.stdout.Split(new[] {'\n', '\r'},
                                    StringSplitOptions.RemoveEmptyEntries);
                                foreach (string line in lines)
                                {
                                    if (line.Trim().Length == 0)
                                        continue;

                                    string[] parts = line.Split('=');

                                    string name = parts[0].ToLower().Trim();
                                    string value = parts[1].ToLower().Trim();

                                    switch (name)
                                    {
                                        case "name":
                                            displayNames.Add(value);
                                            break;
                                        case "numberoflogicalprocessors":
                                            Assert.AreEqual("1", value, "CPU count is incorrect");
                                            break;
                                        case "totalphysicalmemory":
                                            Assert.AreEqual("2144903168", value, "RAM size is incorrect");
                                            break;
                                        default:
                                            break;
                                    }
                                }
                            }
                            catch (AssertFailedException)
                            {
                                Debug.WriteLine("WMIC reported stdout '" + wmicRes.stdout + "'");
                                Debug.WriteLine("WMIC reported stderr '" + wmicRes.stderr + "'");
                            }
                        });
                        VMTestThreads.Add(vmTest);
                        vmTest.Start();
                    }

                    // Wait for them all to run
                    foreach (Thread vmtask in VMTestThreads)
                        vmtask.Join();

                    // Now we can verify the contents of the name/port arrays we made.

                    try
                    {
                        // All computers should have unique names
                        Assert.AreEqual(displayNames.Count, displayNames.Distinct().Count());
                        // And should follow this pattern
                        Assert.IsTrue(displayNames.All(x => x.StartsWith("vm_")));
                    }
                    catch (AssertFailedException)
                    {
                        foreach (string displayName in displayNames)
                            Debug.WriteLine("Machine name: '" + displayName + "'");
                        throw;
                    }

                    try
                    {
                        // All computers should have unique debug ports
                        Assert.AreEqual(debugPorts.Count, debugPorts.Distinct().Count());
                        // And should follow this pattern
                        Assert.IsTrue(debugPorts.All(x => x > 52800));
                        Assert.IsTrue(debugPorts.All(x => x < 63200));
                    }
                    catch (AssertFailedException)
                    {
                        Debug.WriteLine("Machine debug ports:");
                        foreach (string debugPort in displayNames)
                            Debug.WriteLine(debugPort);
                        throw;
                    }
                }
            }
        }

        [TestMethod]
        public void willProvisionBlade()
        {
            using (bladeDirectorDebugServices svc = new bladeDirectorDebugServices(basicBladeTests.WCFPath, basicBladeTests.WebURI))
            {
                machinePools.bladeDirectorURL = svc.servicesURL;

                string hostip = "1.2.3.4";
                //string debuggerHost = testUtils.getBestRouteTo(IPAddress.Parse("172.17.129.131")).ToString();

                // We will be using this blade for our tests.
                bladeSpec spec = svc.svcDebug.createBladeSpecForXDLNode(31, "xdl.hacks.the.planet", bladeLockType.lockAll, bladeLockType.lockAll);
                spec.friendlyName = "newBlade";
                svc.svcDebug.initWithBladesFromBladeSpec(new[] { spec }, false, NASFaultInjectionPolicy.retunSuccessful);

                resultAndBladeName res = svc.svcDebug._RequestAnySingleNode(hostip);
                testUtils.waitForSuccess(svc, res, TimeSpan.FromMinutes(15));

                string bladeName = res.bladeName;
                resultAndWaitToken res2 = svc.svcDebug._selectSnapshotForBladeOrVM(hostip, bladeName, "discord");
                testUtils.waitForSuccess(svc, res2, TimeSpan.FromMinutes(30));

                // Okay, we have our blade allocated now. 
                bladeSpec createdBlade = svc.svc.getBladeByIP_withoutLocking(bladeName);
                snapshotDetails snap = svc.svc.getCurrentSnapshotDetails(bladeName);
                NASParams nas = svc.svc.getNASParams();
                using (hypervisor foo = utils.createHypForBlade(createdBlade, snap, nas))
                {
                    foo.powerOn(new cancellableDateTime(TimeSpan.FromMinutes(5)));

                    // Check that debugging has been provisioned correctly
                    executionResult bcdEditRes = foo.startExecutable("bcdedit", "/dbgsettings");
                    try
                    {
                        Assert.AreEqual(0, bcdEditRes.resultCode);
                        Assert.IsTrue(Regex.IsMatch(bcdEditRes.stdout, "key\\s*xdl.hacks.the.planet"), "bcdedit did not match regex for debug key");
                        Assert.IsTrue(Regex.IsMatch(bcdEditRes.stdout, "debugtype\\s*NET"), "bcdedit did not match regex for debug type");
                        Assert.IsTrue(Regex.IsMatch(bcdEditRes.stdout, "hostip\\s*1.2.3.4"), "bcdedit did not match regex for debug host");
                        Assert.IsTrue(Regex.IsMatch(bcdEditRes.stdout, "port\\s*53101"), "bcdedit did not match regex for debug port");
                    }
                    catch (AssertFailedException)
                    {
                        Debug.WriteLine("return code " + bcdEditRes.resultCode);
                        Debug.WriteLine("stdout " + bcdEditRes.stdout);
                        Debug.WriteLine("stderr " + bcdEditRes.stderr);
                    }

                    executionResult getNameRes = foo.startExecutable("echo %COMPUTERNAME%", "");
                    try
                    {
                        Assert.AreEqual(0, getNameRes.resultCode);
                        Assert.AreSame(getNameRes.stdout.ToLower(), "newBlade".Trim().ToLower(), "machine name was incorrect");
                    }
                    catch (AssertFailedException)
                    {
                        Debug.WriteLine("return code " + bcdEditRes.resultCode);
                        Debug.WriteLine("stdout " + bcdEditRes.stdout);
                        Debug.WriteLine("stderr " + bcdEditRes.stderr);
                    }
                }
            }
        }

        private static resultAndBIOSConfig writeBIOSAndReadBack(bladeDirectorDebugServices svc, string hostIP, string bladeIP, string testBiosXML)
        {
            resultAndWaitToken result = svc.svcDebug._rebootAndStartDeployingBIOSToBlade(hostIP, bladeIP, testBiosXML);
            testUtils.waitForSuccess(svc, result, TimeSpan.FromMinutes(5));

            // Now check it wrote OK by reading it back and comparing the numlock key state.
            Assert.AreEqual(resultCode.pending, svc.svcDebug._rebootAndStartReadingBIOSConfiguration(hostIP, bladeIP).result.code);
            result = testUtils.waitForSuccess(svc, result, TimeSpan.FromMinutes(5));

            // We should definitely have written the config, so we don't permit the 'no need' code here.
            Assert.AreEqual(resultCode.success, result.result.code);

            return (resultAndBIOSConfig)result;
        }
    }
}
