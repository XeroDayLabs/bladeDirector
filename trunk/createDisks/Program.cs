using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.ServiceModel;
using System.Threading;
using System.Threading.Tasks;
using createDisks.bladeDirector;
using CommandLine;
using CommandLine.Text;
using hypervisors;
using Newtonsoft.Json;
using VMware.Vim;
using Action = System.Action;
using FileInfo = System.IO.FileInfo;
using Task = System.Threading.Tasks.Task;

namespace createDisks
{
    /// <summary>
    /// This class holds enough information to make a clone, extent, and target, when combined with a template snapshot.
    /// </summary>
    public class itemToAdd
    {
        public string serverIP;
        public string bladeIP;
        public string cloneName;
        public string nodeiLoIP;
        public string computerName;
        public uint kernelDebugPort;
        public string snapshotName;
        public string kernelDebugKey;
    }

    public class createDisksArgs
    {
        [Option('d', "delete", Required = false, DefaultValue = false, HelpText = "delete clones instead of creating them")]
        public bool deleteClones { get; set; }

        [Option('t', "tag", Required = false, DefaultValue = "clean", HelpText = "suffix for new clones")]
        public string tagName { get; set; }

        [Option('s', "script", Required = false, DefaultValue = null, HelpText = "Additional script to execute on blade after cloning")]
        public string additionalScript { get; set; }

        [Option('r', "repair", Required = false, DefaultValue = false, HelpText = "Do not do a full clone - only make missing iscsi configuration items")]
        public bool repair { get; set; }

        [OptionArray('c', "copy", Required = false, HelpText = "Additional directory to copy onto blade after cloning")]
        public string[] additionalDeploymentItems { get; set; }

        [Option('u', "directorURL", Required = false, DefaultValue = "alizbuild.xd.lan/bladedirector", HelpText = "URL to the blade director instance to use")]
        public string bladeDirectorURL { get; set; }

        [Option('b', "baseSnapshot", Required = false, DefaultValue = "bladeBaseStablesnapshot", HelpText = "Snapshot to base clones on")]
        public string baseSnapshot { get; set; }
    }

    public class Program
    {
        public delegate hypervisorWithSpec<T> hypCreateDelegate<T>(itemToAdd hyp, FreeNAS hostingNAS);

        static void Main(string[] args)
        {
            Parser parser = new CommandLine.Parser();
            createDisksArgs parsedArgs = new createDisksArgs();
            if (parser.ParseArgumentsStrict(args, parsedArgs, () => { Debug.Write(HelpText.AutoBuild(parsedArgs).ToString()); }))
                _Main(parsedArgs);
        }

        public static void _Main(createDisksArgs args)
        {
            List<itemToAdd> itemsToAdd = new List<itemToAdd>();

            string[] serverIPs = Properties.Settings.Default.debugServers.Split(',');
            string[] bladeIPs = Properties.Settings.Default.nodes.Split(',');
            foreach (string serverIP in serverIPs)
            {
                foreach (string bladeIP in bladeIPs)
                {
                    string nodeIP = "172.17.129." + (100 + int.Parse(bladeIP));
                    string nodeiLoIP = "172.17.2." + (100 + int.Parse(bladeIP));
                    string cloneName = String.Format("{0}-{1}-{2}", nodeIP, serverIP, args.tagName);
                    string computerName = String.Format("blade{0}", uint.Parse(bladeIP).ToString("D2"));
                    string snapshotName = cloneName;

                    itemToAdd itemToAdd = new itemToAdd
                    {
                        serverIP = serverIP,
                        bladeIP = nodeIP,
                        cloneName = cloneName,
                        nodeiLoIP = nodeiLoIP,
                        computerName = computerName,
                        kernelDebugPort = (59900 + uint.Parse(bladeIP)),
                        kernelDebugKey = Properties.Settings.Default.kernelDebugKey,
                        snapshotName = snapshotName
                    };

                    itemsToAdd.Add(itemToAdd);
                }
            }

            if (args.deleteClones)
            {
                deleteBlades(itemsToAdd.ToArray());
            }
            else if (args.repair)
            {
                repairBladeDeviceNodes(itemsToAdd.ToArray());
            }
            else
            {
                addBlades<hypSpec_iLo>(itemsToAdd.ToArray(), args.tagName, args.bladeDirectorURL, args.baseSnapshot, args.additionalScript, args.additionalDeploymentItems, makeILOHyp);
            }
        }

        private static hypervisorWithSpec<hypSpec_iLo> makeILOHyp(itemToAdd hyp, FreeNAS hostingnas)
        {
            return new hypervisor_iLo(new hypSpec_iLo(
                hyp.bladeIP,
                Properties.Settings.Default.iloHostUsername, Properties.Settings.Default.iloHostPassword,
                hyp.nodeiLoIP, Properties.Settings.Default.iloUsername, Properties.Settings.Default.iloPassword,
                Properties.Settings.Default.iscsiServerIP, Properties.Settings.Default.iscsiServerUsername, Properties.Settings.Default.iscsiServerPassword,
                "", (ushort)hyp.kernelDebugPort, hyp.kernelDebugKey));
        }

        public static bool doesConfigExist(itemToAdd blade, string tagName)
        {
            string nasIP = Properties.Settings.Default.iscsiServerIP;
            string nasUsername = Properties.Settings.Default.iscsiServerUsername;
            string nasPassword = Properties.Settings.Default.iscsiServerPassword;

            FreeNAS nas = new FreeNAS(nasIP, nasUsername, nasPassword);

            // Get some data from the NAS, so we don't have to keep querying it..
            List<snapshot> snapshots = nas.getSnapshots();
            List<iscsiTarget> iscsiTargets = nas.getISCSITargets();
            List<iscsiTargetToExtentMapping> tgtToExts = nas.getTargetToExtents();
            List<volume> volumes = nas.getVolumes();

            blade.cloneName = String.Format("{0}-{1}", blade.bladeIP, tagName);

            iscsiTarget tgt = iscsiTargets.SingleOrDefault(x => x.targetName == blade.cloneName);
            if (tgt == null)
                return false;
            iscsiTargetToExtentMapping tgtToExt = tgtToExts.SingleOrDefault(x => x.iscsi_target == tgt.id);
            if (tgtToExt == null)
                return false;

            snapshot theSnapshot = snapshots.SingleOrDefault(x => x.name.Equals(blade.cloneName, StringComparison.CurrentCultureIgnoreCase));
            if (theSnapshot == null)
                return false;

            volume vol = nas.findVolumeByName(volumes, blade.cloneName);
            if (vol == null)
                return false;

            return true;
        }

        public static void deleteBlades(itemToAdd[] blades)
        {
            string nasIP = Properties.Settings.Default.iscsiServerIP;
            string nasUsername = Properties.Settings.Default.iscsiServerUsername;
            string nasPassword = Properties.Settings.Default.iscsiServerPassword;
            Debug.WriteLine("Connecting to NAS at " + nasIP);
            FreeNAS nas = new FreeNAS(nasIP, nasUsername, nasPassword);

            // Get some data from the NAS, so we don't have to keep querying it..
            List<snapshot> snapshots = nas.getSnapshots();
            List<iscsiTarget> iscsiTargets = nas.getISCSITargets();
            List<iscsiExtent> iscsiExtents = nas.getExtents();
            List<iscsiTargetToExtentMapping> tgtToExts = nas.getTargetToExtents();
            List<volume> volumes = nas.getVolumes();

            foreach (itemToAdd item in blades)
            {
                // Delete target-to-extent, target, and extent
                iscsiTarget tgt = iscsiTargets.SingleOrDefault(x => x.targetName == item.cloneName);
                if (tgt != null)
                    nas.deleteISCSITarget(tgt);

                iscsiExtent ext = iscsiExtents.SingleOrDefault(x => x.iscsi_target_extent_name == item.cloneName);
                if (ext != null)
                    nas.deleteISCSIExtent(ext);
                /*
                if (tgt != null || ext != null)
                {
                    iscsiTargetToExtentMapping tgtToExt = tgtToExts.SingleOrDefault(x => 
                        ( tgt != null && (x.iscsi_target == tgt.id) || 
                        ( ext != null && (x.iscsi_extent == ext.id   ))));
                    if (tgtToExt != null)
                    {
                        nas.deleteISCSITargetToExtent(tgtToExt);
                    }
                }*/

                // Now delete the snapshot.
                snapshot toDelete = snapshots.SingleOrDefault(x => x.filesystem.ToLower().EndsWith("/" + item.cloneName));
                if (toDelete != null)
                    nas.deleteSnapshot(toDelete);

                // And the volume. Use a retry here since freenas will return before the iscsi deletion is complete,
                    volume vol = nas.findVolumeByName(volumes, item.cloneName);
                if (vol != null)
                {
                    DateTime deadline = DateTime.Now + TimeSpan.FromMinutes(4);
                    while (true)
                    {
                        try
                        {
                            nas.deleteZVol(vol);
                            break;
                        }
                        catch (Exception)
                        {
                            if (DateTime.Now > deadline)
                                throw;
                            Thread.Sleep(TimeSpan.FromSeconds(15));
                        }
                    }
                }
            }
        }

        public static void repairBladeDeviceNodes(itemToAdd[] itemsToAdd, DateTime deadline = default(DateTime))
        {
            if (deadline == default(DateTime))
                deadline = DateTime.MaxValue;

            FreeNAS nas = new FreeNAS(
                Properties.Settings.Default.iscsiServerIP, 
                Properties.Settings.Default.iscsiServerUsername, 
                Properties.Settings.Default.iscsiServerPassword);

            if (DateTime.Now > deadline) throw new TimeoutException();

            exportClonesViaiSCSI(nas, itemsToAdd);

            if (DateTime.Now > deadline) throw new TimeoutException();
        }

        public static void addBlades<T>(itemToAdd[] itemsToAdd, string tagName, string directorURL, string baseSnapshot,
            string additionalScript, string[] additionalDeploymentItem, hypCreateDelegate<T> createHyp, DateTime deadline = default(DateTime))
        {
            if (deadline == default(DateTime))
                deadline = DateTime.MaxValue;

            string nasIP = Properties.Settings.Default.iscsiServerIP;
            string nasUsername = Properties.Settings.Default.iscsiServerUsername;
            string nasPassword = Properties.Settings.Default.iscsiServerPassword;
            Debug.WriteLine("Connecting to NAS at " + nasIP);

            FreeNAS nas = new FreeNAS(nasIP, nasUsername, nasPassword);

            if (DateTime.Now > deadline) throw new TimeoutException();

            
            // Get the snapshot we'll be cloning
            List<snapshot> snapshots = nas.getSnapshots();
            snapshot toClone = snapshots.SingleOrDefault(x => x.name.Equals(baseSnapshot, StringComparison.CurrentCultureIgnoreCase));
            if (toClone == null)
                throw new Exception("Snapshot not found");
            string toCloneVolume = toClone.fullname.Split('/')[0];
            // and clone it
            createClones(nas, toClone, toCloneVolume, itemsToAdd);

            if (DateTime.Now > deadline) throw new TimeoutException();

            exportClonesViaiSCSI(nas, itemsToAdd);

            if (DateTime.Now > deadline) throw new TimeoutException();

            string[] serverIPs = itemsToAdd.Select(x => x.serverIP).Distinct().ToArray();

            // Ensure there are sufficient threads in the worker pool so that we can prepare everything at the same time.
            int wkrThreadCount;
            int completionPortThreads;
            ThreadPool.GetAvailableThreads(out wkrThreadCount, out completionPortThreads);

            if (wkrThreadCount < serverIPs.Length)
                ThreadPool.SetMaxThreads(wkrThreadCount + serverIPs.Length, completionPortThreads);

            // Next, we must prepare each clone. Do this in parallel, per-server.
            CancellationTokenSource tkn = new CancellationTokenSource();

            foreach (string serverIP in serverIPs)
            {
                itemToAdd[] toPrep = itemsToAdd.Where(x => x.serverIP == serverIP).ToArray();
                Thread[] toWaitOn = new Thread[toPrep.Length];
                for (int index = 0; index < toPrep.Length; index++)
                {
                    itemToAdd itemToAdd = toPrep[index];

                    toWaitOn[index] = new Thread(() =>
                    {
                        using (hypervisorWithSpec<T> hyp = createHyp(itemToAdd, nas))
                        {
                            prepareCloneImage(itemToAdd, toCloneVolume, tagName, directorURL, additionalScript, additionalDeploymentItem, hyp, nas);
                        }
                    }, 4*1024*1024);
                    toWaitOn[index].Name = "Create disks for machine " + itemToAdd.bladeIP;
                    toWaitOn[index].Start();
                }

                try
                {
                    foreach (Thread thread in toWaitOn)
                        thread.Join();
                }
                catch (AggregateException)
                {
                    tkn.Cancel(false);

                    throw;
                }
            }

        }

        private static void prepareCloneImage(itemToAdd itemToAdd, string toCloneVolume, string tagName, string directorURL, string additionalScript, string[] additionalDeploymentItem)
        {
            string nasIP = Properties.Settings.Default.iscsiServerIP;
            string nasUsername = Properties.Settings.Default.iscsiServerUsername;
            string nasPassword = Properties.Settings.Default.iscsiServerPassword;

            FreeNAS nas = new FreeNAS(nasIP, nasUsername, nasPassword);

            hypSpec_iLo spec = new hypSpec_iLo(
                itemToAdd.bladeIP,
                Properties.Settings.Default.iloHostUsername, Properties.Settings.Default.iloHostPassword,
                itemToAdd.nodeiLoIP, Properties.Settings.Default.iloUsername, Properties.Settings.Default.iloPassword,
                nasIP, nasUsername, nasPassword,
                "", (ushort) itemToAdd.kernelDebugPort, itemToAdd.kernelDebugKey);

            using (hypervisor_iLo ilo = new hypervisor_iLo(spec))
            {
                prepareCloneImage(itemToAdd, toCloneVolume, tagName, directorURL, additionalScript, additionalDeploymentItem, ilo, nas);
            }
        }

        public static void prepareCloneImage<T>(
            itemToAdd itemToAdd, string toCloneVolume, string tagName, string directorURL, 
            string additionalScript, string[] additionalDeploymentItem, hypervisorWithSpec<T> hyp, FreeNAS nas)
        {
            Debug.WriteLine("Preparing " + itemToAdd.bladeIP + " for server " + itemToAdd.serverIP);

            hyp.connect();
            hyp.powerOff();
            Debug.WriteLine(itemToAdd.bladeIP + " powered down, allocating via bladeDirector");

            // We must ensure the blade is allocated to the required blade before we power it on. This will cause it to
            // use the required iSCSI root path.
            EndpointAddress ep = new EndpointAddress(String.Format("http://{0}/services.asmx", directorURL));
            BasicHttpBinding binding = new BasicHttpBinding();
            using (bladeDirector.servicesSoapClient bladeDirectorClient = new bladeDirector.servicesSoapClient(binding, ep))
            {
                bladeDirectorClient.Open();
                resultCode res = bladeDirectorClient.forceBladeAllocation(itemToAdd.bladeIP, itemToAdd.serverIP);
                if (res != resultCode.success)
                    throw new Exception("Can't claim blade " + itemToAdd.bladeIP);
                resultCode shotResCode = bladeDirectorClient.selectSnapshotForBladeOrVM(itemToAdd.bladeIP, tagName);
                while (shotResCode == resultCode.pending)
                {
                    Thread.Sleep(TimeSpan.FromSeconds(4));
                    shotResCode = bladeDirectorClient.selectSnapshotForBladeOrVM_getProgress(itemToAdd.bladeIP);
                }
                if (shotResCode != resultCode.success)
                    throw new Exception("Can't select snapshot on blade " + itemToAdd.bladeIP);
            }
            Debug.WriteLine(itemToAdd.bladeIP + " allocated, powering up");
            hyp.powerOn();
            Debug.WriteLine(itemToAdd.bladeIP + " powered up, deploying");

            // Now deploy and execute our deployment script
            string args ;
            if (itemToAdd.serverIP == null || itemToAdd.kernelDebugPort == 0 || itemToAdd.kernelDebugKey == null)
                args = String.Format("{0}", itemToAdd.computerName);
            else
                args = String.Format("{0} {1} {2} {3}", itemToAdd.computerName, itemToAdd.serverIP, itemToAdd.kernelDebugPort, itemToAdd.kernelDebugKey);

            copyAndRunScript(args, hyp);

            // Copy any extra folders the user has requested that we deploy also, if there's an additional script we should
            // execute, then do that now
            if (additionalDeploymentItem != null)
            {
                hypervisor_iLo.doWithRetryOnSomeExceptions(() => hyp.mkdir("C:\\deployment") );
                foreach (string toCopy in additionalDeploymentItem)
                    copyRecursive(hyp, toCopy, "C:\\deployment");

                if (additionalScript != null)
                    hypervisor_iLo.doWithRetryOnSomeExceptions(() =>hyp.startExecutable("cmd.exe", string.Format("/c {0}", additionalScript), "C:\\deployment"));
            }
            else
            {
                if (additionalScript != null)
                    hypervisor_iLo.doWithRetryOnSomeExceptions(() => hyp.startExecutable("cmd.exe", string.Format("/c {0}", additionalScript), "C:\\"));
            }

            Debug.WriteLine(itemToAdd.bladeIP + " deployed, shutting down");

            // That's all we need, so shut down the system.
            hypervisor_iLo.doWithRetryOnSomeExceptions(() => hyp.startExecutableAsync("C:\\windows\\system32\\cmd", "/c shutdown -s -f -t 01"));

            // Once it has shut down totally, we can take the snapshot of it.
            hyp.WaitForStatus(false, TimeSpan.FromMinutes(1));

            Debug.WriteLine(itemToAdd.bladeIP + " turned off, creating snapshot");

            nas.createSnapshot(toCloneVolume + "/" + itemToAdd.cloneName, itemToAdd.snapshotName);
            Debug.WriteLine(itemToAdd.bladeIP + " complete");
        }

        private static void copyRecursive(hypervisor hyp, string srcFileOrDir, string destPath)
        {
            FileAttributes attr = File.GetAttributes(srcFileOrDir);
            if (attr.HasFlag(FileAttributes.Directory))
            {
                foreach (string srcDir in Directory.GetDirectories(srcFileOrDir))
                {
                    string fullPath = Path.Combine(destPath, Path.GetFileName(srcDir));
                    hypervisor_iLo.doWithRetryOnSomeExceptions(() => { hyp.mkdir(fullPath); });
                    foreach (string file in Directory.GetFiles(srcFileOrDir))
                    {
                        hypervisor_iLo.doWithRetryOnSomeExceptions(() =>
                        {
                            copyRecursive(hyp, file, fullPath);
                        });
                    }
                }

                foreach (string srcFile in Directory.GetFiles(srcFileOrDir))
                {
                    try
                    {
                        hypervisor_iLo.doWithRetryOnSomeExceptions(() =>
                        {
                            hyp.copyToGuest(srcFile, Path.Combine(destPath, Path.GetFileName(srcFile)));
                        });
                    }
                    catch (IOException)
                    {
                        // The file probably already exists.
                    }
                }
            }
            else
            {
                try
                {
                    hypervisor_iLo.doWithRetryOnSomeExceptions(() =>
                    {
                        hyp.copyToGuest(srcFileOrDir, Path.Combine(destPath, Path.GetFileName(srcFileOrDir)));
                    });
                }
                catch (IOException)
                {
                    // The file probably already exists.
                }
            }
        }

        private static void copyAndRunScript(string scriptArgs, hypervisor hyp)
        {
            string deployFileName = Path.GetTempFileName();
            try
            {
                File.WriteAllText(deployFileName, Properties.Resources.deployToBlade);
                hypervisor_iLo.doWithRetryOnSomeExceptions(() =>
                {
                    hyp.copyToGuest(deployFileName, "C:\\deployed.bat");
                });
                string args = String.Format("/c c:\\deployed.bat {0}", scriptArgs);
                hyp.mkdir("c:\\deployment");
                executionResult res = hyp.startExecutable ("cmd.exe", args, "c:\\deployment");
                //Debug.WriteLine(res.stdout);
                //Debug.WriteLine(res.stderr);
            }
            finally
            {
                DateTime deadline = DateTime.Now + TimeSpan.FromSeconds(10);
                while (File.Exists(deployFileName))
                {
                    try
                    {
                        File.Delete(deployFileName);
                    }
                    catch (Exception)
                    {
                        if (DateTime.Now > deadline)
                            throw;
                    }
                }
            }
        }

        private static void createClones(FreeNAS nas, snapshot toClone, string toCloneVolume, itemToAdd[] itemsToAdd)
        {
            // Now we can create snapshots
            foreach (itemToAdd itemToAdd in itemsToAdd)
            {
                Debug.WriteLine("Creating snapshot clones for server '" + itemToAdd.serverIP + "' node '" + itemToAdd.bladeIP + "'");
                string fullCloneName = String.Format("{0}/{1}", toCloneVolume, itemToAdd.cloneName);
                nas.cloneSnapshot(toClone, fullCloneName);
            }
        }

        private static void exportClonesViaiSCSI(FreeNAS nas, itemToAdd[] itemsToAdd)
        {
            // Now expose each via iSCSI.
            targetGroup tgtGrp = nas.getTargetGroups()[0];
            foreach (itemToAdd itemToAdd in itemsToAdd)
            {
                Debug.WriteLine("Creating iSCSI target/extent/link for server '" + itemToAdd.serverIP + "' node '" + itemToAdd.bladeIP + "'");

                iscsiTarget toAdd = new iscsiTarget();
                toAdd.targetAlias = itemToAdd.cloneName;
                toAdd.targetName = itemToAdd.cloneName;
                iscsiTarget newTarget = nas.getISCSITargets().SingleOrDefault(x => x.targetName == itemToAdd.cloneName);
                if (newTarget == null)
                    newTarget = nas.addISCSITarget(toAdd);

                iscsiExtent newExtent = nas.getExtents().SingleOrDefault(x => x.iscsi_target_extent_name == toAdd.targetName);

                if (newExtent == null)
                {
                    newExtent = nas.addISCSIExtent(new iscsiExtent()
                    {
                        iscsi_target_extent_name = toAdd.targetName,
                        iscsi_target_extent_path = String.Format("zvol/SSDs/{0}", toAdd.targetName)
                    });
                }

                targetGroup newTgtGroup = nas.getTargetGroups().SingleOrDefault(x => x.iscsi_target == newTarget.id);
                if (newTgtGroup == null)
                    nas.addTargetGroup(tgtGrp, newTarget);

                iscsiTargetToExtentMapping newTToE = nas.getTargetToExtents().SingleOrDefault(x => x.iscsi_target == newTarget.id);
                if (newTToE == null)
                    nas.addISCSITargetToExtent(newTarget.id, newExtent);
            }
        }
    }
}
