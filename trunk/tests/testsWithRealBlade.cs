using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using bladeDirectorClient;
using bladeDirectorClient.bladeDirectorService;
using hypervisors;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NASParams = bladeDirectorClient.bladeDirectorService.NASParams;

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
                while (relRes.result.code == bladeDirectorClient.bladeDirectorService.resultCode.pending)
                {
                    Thread.Sleep(TimeSpan.FromSeconds(3));
                    relRes = svc.svc.getProgress(relRes.waitToken);
                }
                Assert.AreEqual(bladeDirectorClient.bladeDirectorService.resultCode.success, relRes.result.code);
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

                string debuggerHost = testUtils.getBestRouteTo(IPAddress.Parse(spec.bladeIP)).ToString();
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
                    string debuggerHost = testUtils.getBestRouteTo(IPAddress.Parse(thisBlade.bladeIP)).ToString();

                    // Add ten VMs for each blade.
                    for (int vmCount = 0; vmCount < 10; vmCount++)
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
                    List<Task> VMTestTasks = new List<Task>(); 
                    foreach (hypervisorWithSpec<hypSpec_vmware> hyp in vms.Values)
                    {
                        Task vmTest = new Task(() =>
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
                        VMTestTasks.Add(vmTest);
                        vmTest.Start();
                    }

                    // Wait for them all to run
                    foreach (Task vmtask in VMTestTasks)
                        vmtask.Wait();

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
                        {
                            Debug.WriteLine("Machine name: '" + displayName + "'");
                        }
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
            Assert.AreEqual(bladeDirectorClient.bladeDirectorService.resultCode.pending, svc.svcDebug._rebootAndStartReadingBIOSConfiguration(hostIP, bladeIP).result.code);
            result = testUtils.waitForSuccess(svc, result, TimeSpan.FromMinutes(5));

            // We should definitely have written the config, so we don't permit the 'no need' code here.
            Assert.AreEqual(bladeDirectorClient.bladeDirectorService.resultCode.success, result.result.code);

            return (resultAndBIOSConfig)result;
        }
    }

    [TestClass]
    public class freeNASTests
    {
/*        private string nashostname = "172.16.61.191";
        private string nasusername = "root";
        private string naspassword = "P@$$w0rd";
        private string nastempspace = "/mnt/data/";
*/
        private string nashostname = "store.xd.lan";
        private string nasusername = "root";
        private string naspassword = "4Z2VPdefjR";
        private string nastempspace = "/mnt/SSDs/";
        
        [TestInitialize]
        public void init()
        {
            clearAll();
        }

        private void clearAll()
        {
            // We assume no-one else is using this FreeNAS server right now. We delete everything on it. >:D
            // We do, however, avoid deleting anything beginning with 'blade', so its sort-of safe to run this on
            // the production FreeNAS install, if no-one else is using it, and the only iscsi info you want to keep
            // begins with this string.

            FreeNASWithCaching foo = new FreeNASWithCaching(nashostname, nasusername, naspassword);

            foreach (iscsiTarget tgt in foo.getISCSITargets().Where(x => !x.targetName.StartsWith("blade")))
                foo.deleteISCSITarget(tgt);

            foreach (iscsiExtent ext in foo.getExtents().Where(x => !x.iscsi_target_extent_name.StartsWith("blade")))
                foo.deleteISCSIExtent(ext);

            foreach (iscsiTargetToExtentMapping tte in foo.getTargetToExtents())
            {
                var tgt = foo.getISCSITargets().Where(x => x.id == tte.iscsi_target).SingleOrDefault();
                var ext = foo.getExtents().Where(x => x.id == tte.iscsi_extent).SingleOrDefault();
                if (tgt == null || ext == null)
                    foo.deleteISCSITargetToExtent(tte);
            }

            foo.waitUntilISCSIConfigFlushed();
        }

        [TestMethod]
        public void checkThatReloadsScaleOkay()
        {
            string testPrefix = Guid.NewGuid().ToString();

            Dictionary<int, TimeSpan> reloadTimesByFileCount = new Dictionary<int, TimeSpan>();

            for (int filecount = 1; filecount < 100; filecount += 10)
            {
                clearAll();
                TimeSpan reloadTime = canAddExportNewFilesQuickly(testPrefix, 10);
                reloadTimesByFileCount.Add(filecount, reloadTime);
            }

            foreach (KeyValuePair<int, TimeSpan> kvp in reloadTimesByFileCount)
                Debug.WriteLine("Adding " + kvp.Key + " files took " + kvp.Value);

            // They should not deviate from the average by more than 10%.
            double avg = reloadTimesByFileCount.Values.Average(x => x.TotalMilliseconds);
            foreach (TimeSpan val in reloadTimesByFileCount.Values)
                Assert.AreEqual(avg, val.TotalMilliseconds, avg / 10);
        }

        [TestMethod]
        public void canAddExtentsQuickly()
        {
            Stopwatch timer = new Stopwatch();
            timer.Start();
            FreeNASWithCaching foo = new FreeNASWithCaching(nashostname, nasusername, naspassword);
            timer.Stop();
            Debug.WriteLine("Instantiation took " + timer.ElapsedMilliseconds + " ms");

            string testPrefix = Guid.NewGuid().ToString();

            // Make some test files to export
            int extentcount = 50;
            using (SSHExecutor exec = new SSHExecutor(nashostname, nasusername, naspassword))
            {
                for (int i = 0; i < extentcount; i++)
                {
                    exec.startExecutable("touch", nastempspace + "testfile_" + i);
                }                
            }

            // Add some extents and watch the time taken
            timer.Restart();
            int flushesBefore = foo.flushCount;
            iscsiExtent[] extentsAdded = new iscsiExtent[extentcount];
            for (int i = 0; i < extentcount; i++)
            {
                extentsAdded[i] = foo.addISCSIExtent(new iscsiExtent()
                {
                    iscsi_target_extent_name = testPrefix + "_" + i,
                    iscsi_target_extent_type = "File",
                    iscsi_target_extent_path = nastempspace + "/testfile_" + i
                });
            }
            timer.Stop();
            int flushesAfter = foo.flushCount;
            Debug.WriteLine("Adding " + extentcount + " extents took " + timer.ElapsedMilliseconds + " ms and required " + 
                (flushesAfter - flushesBefore) + " flushes");
            Assert.IsTrue(timer.Elapsed < TimeSpan.FromSeconds(10));

            // Each should be unique by these properties
            foreach (iscsiExtent ext in extentsAdded)
            {
                Assert.AreEqual(1, extentsAdded.Count(x => x.id == ext.id));
                Assert.AreEqual(1, extentsAdded.Count(x => x.iscsi_target_extent_path == ext.iscsi_target_extent_path));
                Assert.AreEqual(1, extentsAdded.Count(x => x.iscsi_target_extent_name == ext.iscsi_target_extent_name));
            }

            timer.Restart();
            foo.waitUntilISCSIConfigFlushed();
            timer.Stop();
            Debug.WriteLine("Reloading config took " + timer.ElapsedMilliseconds + " ms");
            Assert.IsTrue(timer.Elapsed < TimeSpan.FromSeconds(3));
        }

        [TestMethod]
        public void canAddDiskBasedExtent()
        {
            FreeNASWithCaching foo = new FreeNASWithCaching(nashostname, nasusername, naspassword);

            string testPrefix = Guid.NewGuid().ToString();

            // Make some test files to export
            using (SSHExecutor exec = new SSHExecutor(nashostname, nasusername, naspassword))
            {
                    exec.startExecutable("zfs", "create SSDs/" + testPrefix);
            }

            // Add some extents and watch the time taken
            foo.addISCSIExtent(new iscsiExtent()
            {
                iscsi_target_extent_name = testPrefix,
                iscsi_target_extent_type = "Disk",
                iscsi_target_extent_path = "SSDs/" + testPrefix
            });

            foo.waitUntilISCSIConfigFlushed();
        }

        [TestMethod]
        public void canAddTargetsQuickly()
        {
            Stopwatch timer = new Stopwatch();
            timer.Start();
            FreeNASWithCaching foo = new FreeNASWithCaching(nashostname, nasusername, naspassword);
            timer.Stop();
            Debug.WriteLine("Instantiation took " + timer.ElapsedMilliseconds + " ms");

            string testPrefix = Guid.NewGuid().ToString();

            int targetCount = 50;

            // Add some targets and watch the time taken
            timer.Restart();
            int flushesBefore = foo.flushCount;
            iscsiTarget[] targetsAdded = new iscsiTarget[targetCount];
            for (int i = 0; i < targetCount; i++)
            {
                targetsAdded[i] = foo.addISCSITarget(new iscsiTarget()
                {
                    targetName =  testPrefix + "-" + i
                });
            }
            timer.Stop();
            int flushesAfter = foo.flushCount;
            Debug.WriteLine("Adding " + targetCount + " targets took " + timer.ElapsedMilliseconds + " ms and required " +
                (flushesAfter - flushesBefore) + " flushes");
            Assert.IsTrue(timer.Elapsed < TimeSpan.FromSeconds(7));

            // Each should be unique by these properties
            try
            {
                foreach (iscsiTarget tgt in targetsAdded)
                {
                    Assert.AreEqual(1, targetsAdded.Count(x => x.id == tgt.id));
                    Assert.AreEqual(1, targetsAdded.Count(x => x.targetName == tgt.targetName));
                }
            }
            catch (AssertFailedException)
            {
                foreach (var tgt in targetsAdded)
                {
                    Debug.WriteLine("Target:");
                    Debug.WriteLine(" id = " + tgt.id);
                    Debug.WriteLine(" targetName = " + tgt.targetName);
                    Debug.WriteLine(" targetAlias = " + tgt.targetAlias);
                }
                throw;
            }

            timer.Restart();
            foo.waitUntilISCSIConfigFlushed();
            timer.Stop();
            Debug.WriteLine("Reloading config took " + timer.ElapsedMilliseconds + " ms");
            Assert.IsTrue(timer.Elapsed < TimeSpan.FromSeconds(4));
        }

        [TestMethod]
        public void canAddExportNewFilesQuickly()
        {
            string testPrefix = Guid.NewGuid().ToString();
            int extentcount = 50;
            try
            {
                TimeSpan reloadTime = canAddExportNewFilesQuickly(testPrefix, extentcount);
                Debug.WriteLine("Reloading config took " + reloadTime + " ms");
                Assert.IsTrue(reloadTime < TimeSpan.FromSeconds(5));
            }
            finally
            {
                // TODO: clean up
            }
        }

        public TimeSpan canAddExportNewFilesQuickly(string testPrefix, int newExportsCount)
        {
            // FIXME: use testprefix for filenames

            Stopwatch timer = new Stopwatch();
            timer.Start();
            FreeNASWithCaching foo = new FreeNASWithCaching(nashostname, nasusername, naspassword);
            timer.Stop();
            Debug.WriteLine("Instantiation took " + timer.ElapsedMilliseconds + " ms");

            // Make some test files to export
            using (SSHExecutor exec = new SSHExecutor(nashostname, nasusername, naspassword))
            {
                for (int i = 0; i < newExportsCount; i++)
                    exec.startExecutable("touch", nastempspace + "/testfile_" + i);
            }
            // Add some targets, extents, target-to-extents, and watch the time taken.
            timer.Restart();
            int flushesBefore = foo.flushCount;
            for (int i = 0; i < newExportsCount; i++)
            {
                iscsiTarget tgt = foo.addISCSITarget(new iscsiTarget() { targetName = testPrefix + "-" + i });
                iscsiExtent ext = foo.addISCSIExtent(new iscsiExtent()
                {
                    iscsi_target_extent_name = testPrefix + "_" + i,
                    iscsi_target_extent_type = "File",
                    iscsi_target_extent_path = nastempspace + "/testfile_" + i
                });

                foo.addISCSITargetToExtent(tgt.id, ext);
            }
            timer.Stop();
            int flushesAfter = foo.flushCount;
            Debug.WriteLine("Adding " + newExportsCount + " target, extents, and target-to-extent records took " + timer.ElapsedMilliseconds + " ms and required " +
                (flushesAfter - flushesBefore) + " flushes");
            Assert.IsTrue(timer.Elapsed < TimeSpan.FromSeconds(15));

            timer.Restart();
            foo.waitUntilISCSIConfigFlushed();
            timer.Stop();

            // Check that ctld has exported the files as we asked
            using (SSHExecutor exec = new SSHExecutor(nashostname, nasusername, naspassword))
            {
                executionResult res = exec.startExecutable("ctladm", "portlist");
                Assert.AreEqual(0, res.resultCode);
                // Each of our testfiles should appear exactly once in this list.
                string[] lines = res.stdout.Split('\n');
                for (int i = 0; i < newExportsCount; i++)
                {
                    try
                    {
                        Assert.AreEqual(1, lines.Count(x => x.Contains(testPrefix + "-" + i + ",")));
                    }
                    catch (AssertFailedException)
                    {
                        Debug.Write(res.stdout);
                        Debug.Write(res.stderr);
                        throw;
                    }  
                }
            }

            return timer.Elapsed;
        }

        [TestMethod]
        public void extentDeletionImpliesTTEDeletion()
        {
            string testPrefix = Guid.NewGuid().ToString();

            FreeNASWithCaching foo = new FreeNASWithCaching(nashostname, nasusername, naspassword);

            int origExtentCount = foo.getExtents().Count;
            int origTargetCount = foo.getISCSITargets().Count;
            int origTTECount = foo.getTargetToExtents().Count;

            // Make a test file to export
            string filePath = nastempspace + "/" + testPrefix;
            using (SSHExecutor exec = new SSHExecutor(nashostname, nasusername, naspassword))
            {
                exec.startExecutable("touch", filePath);
            }

            iscsiTarget tgt1 = foo.addISCSITarget(new iscsiTarget() { targetName = testPrefix });
            iscsiExtent ext1 = foo.addISCSIExtent(new iscsiExtent()
            {
                iscsi_target_extent_name = testPrefix,
                iscsi_target_extent_type = "File",
                iscsi_target_extent_path = filePath
            });

            iscsiTargetToExtentMapping tte1 = foo.addISCSITargetToExtent(tgt1.id, ext1);

            foo.waitUntilISCSIConfigFlushed();

            foo.deleteISCSIExtent(ext1);

            Assert.AreEqual(origTTECount, foo.getTargetToExtents().Count());
            Assert.AreEqual(origTargetCount + 1, foo.getISCSITargets().Count());
            Assert.AreEqual(origExtentCount, foo.getExtents().Count());
        }

    }
}
