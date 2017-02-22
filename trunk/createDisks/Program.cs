using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
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
    class itemToAdd
    {
        public string serverIP;
        public string bladeIP;
        public string cloneName;
        public string nodeiLoIP;
        public string computerName;
        public uint kernelDebugPort;
        public string snapshotName;
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
    }

    class Program
    {
        static void Main(string[] args)
        {
            Parser parser = new CommandLine.Parser();
            createDisksArgs parsedArgs = new createDisksArgs();
            if (parser.ParseArgumentsStrict(args, parsedArgs, () => { Console.Write(HelpText.AutoBuild(parsedArgs).ToString()); }))
                _Main(parsedArgs);
        }

        static void _Main(createDisksArgs args)
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

                    var itemToAdd = new itemToAdd
                    {
                        serverIP = serverIP,
                        bladeIP = nodeIP,
                        cloneName = cloneName,
                        nodeiLoIP = nodeiLoIP,
                        computerName = computerName,
                        kernelDebugPort = (51000 + uint.Parse(bladeIP)),
                        snapshotName = snapshotName
                    };

                    itemsToAdd.Add(itemToAdd);
                }
            }

            if (args.deleteClones)
                deleteBlades(itemsToAdd.ToArray());
            else
                addBlades(itemsToAdd.ToArray(), args.tagName, args.bladeDirectorURL, args.additionalScript, args.additionalDeploymentItems, args.repair);
        }

        private static void deleteBlades(itemToAdd[] blades)
        {
            string nasIP = Properties.Settings.Default.iscsiServerIP;
            string nasUsername = Properties.Settings.Default.iscsiServerUsername;
            string nasPassword = Properties.Settings.Default.iscsiServerPassword;
            Console.WriteLine("Connecting to NAS at " + nasIP);
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
                if (tgt == null)
                {
                    Console.WriteLine("ISCSI target {0} not present, skipping", item.cloneName);
                    continue;
                }
                iscsiTargetToExtentMapping tgtToExt = tgtToExts.Single(x => x.iscsi_target == tgt.id);
                iscsiExtent ext = iscsiExtents.Single(x => x.id == tgtToExt.iscsi_extent);
                nas.deleteISCSITargetToExtent(tgtToExt);
                nas.deleteISCSITarget(tgt);
                nas.deleteISCSIExtent(ext);

                // Now delete the snapshot.
                snapshot toDelete = snapshots.SingleOrDefault(x => x.name.Equals(item.cloneName, StringComparison.CurrentCultureIgnoreCase));
                if (toDelete != null)
                    nas.deleteSnapshot(toDelete);

                // And the volume.
                volume vol = nas.findVolumeByName(volumes, item.cloneName);
                nas.deleteZVol(vol);
            }
        }

        static void addBlades(itemToAdd[] itemsToAdd, string tagName, string directorURL, string additionalScript, string[] additionalDeploymentItem, bool repairMode)
        {
            string nasIP = Properties.Settings.Default.iscsiServerIP;
            string nasUsername = Properties.Settings.Default.iscsiServerUsername;
            string nasPassword = Properties.Settings.Default.iscsiServerPassword;
            Console.WriteLine("Connecting to NAS at " + nasIP);

            FreeNAS nas = new FreeNAS(nasIP, nasUsername, nasPassword);

            // Get the snapshot we'll be cloning
            List<snapshot> snapshots = nas.getSnapshots();
            snapshot toClone = snapshots.SingleOrDefault(x => x.name.Equals(Properties.Settings.Default.iscsiBaseName, StringComparison.CurrentCultureIgnoreCase));
            if (toClone == null)
                throw new Exception("Snapshot not found");
            string toCloneVolume = toClone.fullname.Split('/')[0];

            if (!repairMode)
                createClones(nas, toClone, toCloneVolume, itemsToAdd);

            exportClonesViaiSCSI(nas, itemsToAdd);

            if (!repairMode)
            {
                // Next, we must prepare each clone. Do this in parallel, per-server.
                foreach (string serverIP in itemsToAdd.Select(x => x.serverIP).Distinct())
                {
                    itemToAdd[] toPrep = itemsToAdd.Where(x => x.serverIP == serverIP).ToArray();
                    Task[] toWaitOn = new Task[toPrep.Length];
                    for (int index = 0; index < toPrep.Length; index++)
                    {
                        itemToAdd itemToAdd = toPrep[index];

                        toWaitOn[index] = new Task(() => prepareCloneImage(itemToAdd, toCloneVolume, tagName, directorURL, additionalScript, additionalDeploymentItem));
                        toWaitOn[index].Start();
                    }

                    foreach (Task task in toWaitOn)
                        task.Wait();
                }
            }
        }

        private static void prepareCloneImage(itemToAdd itemToAdd, string toCloneVolume, string tagName, string directorURL, string additionalScript, string[] additionalDeploymentItem)
        {
            Console.WriteLine("Preparing " + itemToAdd.bladeIP + " for server " + itemToAdd.serverIP);

            string nasIP = Properties.Settings.Default.iscsiServerIP;
            string nasUsername = Properties.Settings.Default.iscsiServerUsername;
            string nasPassword = Properties.Settings.Default.iscsiServerPassword;

            FreeNAS nas = new FreeNAS(nasIP, nasUsername, nasPassword);

            hypSpec_iLo spec = new hypSpec_iLo(
                itemToAdd.bladeIP,
                Properties.Settings.Default.iloHostUsername, Properties.Settings.Default.iloHostPassword,
                itemToAdd.nodeiLoIP, Properties.Settings.Default.iloUsername, Properties.Settings.Default.iloPassword,
                nasIP, nasUsername, nasPassword,
                "", (ushort) itemToAdd.kernelDebugPort, "");
            using (hypervisor_iLo ilo = new hypervisor_iLo(spec))
            {
                ilo.connect();
                ilo.powerOff();

                // We must ensure the blade is allocated to the required blade before we power it on. This will cause it to
                // use the required iSCSI root path.
                string url = String.Format("http://{0}/services.asmx", directorURL);
                using (bladeDirector.servicesSoapClient bladeDirectorClient = new bladeDirector.servicesSoapClient("servicesSoap", url))
                {
                    string res = bladeDirectorClient.forceBladeAllocation(itemToAdd.bladeIP, itemToAdd.serverIP);
                    if (res != "success")
                        throw new Exception("Can't claim blade " + itemToAdd.bladeIP);
                    resultCode shotResCode = bladeDirectorClient.selectSnapshotForBlade(itemToAdd.bladeIP, tagName);
                    if (shotResCode != resultCode.success)
                        throw new Exception("Can't select snapshot on blade " + itemToAdd.bladeIP);
                }
                ilo.powerOn();

                // Now deploy and execute our deployment script
                string args = String.Format("{0} {1} {2}", itemToAdd.computerName, itemToAdd.serverIP, spec.kernelDebugPort);
                copyAndRunScript(args, ilo);

                // Copy any extra folders the user has requested that we deploy also, if there's an additional script we should
                // execute, then do that now
                if (additionalDeploymentItem != null)
                {
                    ilo.mkdir("C:\\deployment");
                    foreach (string toCopy in additionalDeploymentItem)
                        copyRecursive(ilo, toCopy, "C:\\deployment");
                    ilo.startExecutable("cmd.exe", string.Format("/c {0}", additionalScript), "C:\\deployment");
                }
                else
                {
                    ilo.startExecutable("cmd.exe", string.Format("/c {0}", additionalScript), "C:\\");
                }

                // That's all we need, so shut down the system.
                ilo.startExecutable("C:\\windows\\system32\\cmd", "/c shutdown -s -f -t 01");

                // Once it has shut down totally, we can take the snapshot of it.
                ilo.WaitForStatus(false, TimeSpan.FromMinutes(1));

                nas.createSnapshot(toCloneVolume + "/" + itemToAdd.cloneName, itemToAdd.snapshotName);
            }
        }

        private static void copyRecursive(hypervisor_iLo ilo, string srcFileOrDir, string destPath)
        {
            FileAttributes attr = File.GetAttributes(srcFileOrDir);
            if (attr.HasFlag(FileAttributes.Directory))
            {
                foreach (string srcDir in Directory.GetDirectories(srcFileOrDir))
                {
                    string fullPath = Path.Combine(destPath, Path.GetFileName(srcDir));
                    ilo.mkdir(fullPath);
                    foreach (string file in Directory.GetFiles(srcFileOrDir))
                        copyRecursive(ilo, file, fullPath);
                }

                foreach (string srcFile in Directory.GetFiles(srcFileOrDir))
                {
                    try
                    {
                        ilo.copyToGuest(srcFile, Path.Combine(destPath, Path.GetFileName(srcFile)));
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
                    ilo.copyToGuest(srcFileOrDir, Path.Combine(destPath, Path.GetFileName(srcFileOrDir)));
                }
                catch (IOException)
                {
                    // The file probably already exists.
                }
            }
        }

        private static void copyAndRunScript(string scriptArgs, hypervisor_iLo ilo)
        {
            string deployFileName = Path.GetTempFileName();
            try
            {
                File.WriteAllText(deployFileName, Properties.Resources.deployToBlade);
                ilo.copyToGuest(deployFileName, "C:\\deployed.bat");
                string args = String.Format("/c c:\\deployed.bat {0}", scriptArgs);
                ilo.startExecutable("cmd.exe", args);
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
                Console.WriteLine("Creating snapshot clones for server '" + itemToAdd.serverIP + "' node '" + itemToAdd.bladeIP + "'");
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
                Console.WriteLine("Creating iSCSI target/extent/link for server '" + itemToAdd.serverIP + "' node '" + itemToAdd.bladeIP + "'");

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



    public class FreeNAS
    {
        private readonly string _serverIp;
        private readonly string _username;
        private readonly string _password;
        private readonly CookieContainer cookies = new CookieContainer();

        public FreeNAS(string serverIP, string username, string password)
        {
            _serverIp = serverIP;
            _username = username;
            _password = password;
        }

        public class resp
        {
            public string text;
        }

        private resp doReq(string url, string method, HttpStatusCode expectedCode, string payload = null)
        {
            if (url.Contains("?"))
                url = url + "&limit=999999";
            else
                url = url + "?limit=999999";
            HttpWebRequest req = (HttpWebRequest)WebRequest.Create(url);
            req.Method = method;
            CredentialCache cred = new CredentialCache();
            cred.Add(new Uri(url), "Basic", new NetworkCredential(_username, _password));
            req.Credentials = cred;
            req.PreAuthenticate = true;

            if (payload != null)
            {
                req.ContentType = "application/json";
                Byte[] dataBytes = Encoding.ASCII.GetBytes(payload);
                req.ContentLength = dataBytes.Length;
                using (Stream stream = req.GetRequestStream())
                {
                    stream.Write(dataBytes, 0, dataBytes.Length);
                }
            }

            try
            {
                using (HttpWebResponse resp = (HttpWebResponse)req.GetResponse())
                {
                    using (Stream respStream = resp.GetResponseStream())
                    {
                        using (StreamReader respStreamReader = new StreamReader(respStream))
                        {
                            string contentString = respStreamReader.ReadToEnd();

                            if (resp.StatusCode != expectedCode)
                                throw new Exception("FreeNAS API call failed, status " + resp.StatusCode + ", URL " + url + " HTTP response body " + contentString);

                            return new resp() { text = contentString };
                        }
                    }
                }
            }
            catch (WebException e)
            {
                using (Stream respStream = e.Response.GetResponseStream())
                {
                    using (StreamReader respStreamReader = new StreamReader(respStream))
                    {
                        string contentString = respStreamReader.ReadToEnd();
                        throw new Exception("FreeNAS API call failed, status " + ((HttpWebResponse)e.Response).StatusCode + ", URL " + url + " HTTP response body " + contentString);
                    }
                }
            }
        }

        public List<iscsiTarget> getISCSITargets()
        {
            string HTTPResponse = doReq("http://" + _serverIp + "/api/v1.0/services/iscsi/target/", "get", HttpStatusCode.OK).text;
            return JsonConvert.DeserializeObject<List<iscsiTarget>>(HTTPResponse);
        }

        public void deleteISCSITarget(iscsiTarget target)
        {
            string url = String.Format("http://{0}/api/v1.0/services/iscsi/target/{1}", _serverIp, target.id);
            doReq(url, "DELETE", HttpStatusCode.NoContent);
        }

        public List<iscsiTargetToExtentMapping> getTargetToExtents()
        {
            string HTTPResponse = doReq("http://" + _serverIp + "/api/v1.0/services/iscsi/targettoextent/", "get", HttpStatusCode.OK).text;
            return JsonConvert.DeserializeObject<List<iscsiTargetToExtentMapping>>(HTTPResponse);
        }

        public List<iscsiExtent> getExtents()
        {
            string HTTPResponse = doReq("http://" + _serverIp + "/api/v1.0/services/iscsi/extent/", "get", HttpStatusCode.OK).text;
            return JsonConvert.DeserializeObject<List<iscsiExtent>>(HTTPResponse);
        }

        public void deleteISCSITargetToExtent(iscsiTargetToExtentMapping tgtToExtent)
        {
            string url = String.Format("http://{0}/api/v1.0/services/iscsi/targettoextent/{1}", _serverIp, tgtToExtent.id);
            doReq(url, "DELETE", HttpStatusCode.NoContent);
        }

        public void deleteZVol(volume toDelete)
        {
            // Oh no, the freenas API keeps returning HTTP 404 when I try to delete a volume! :( We ignore it and use the web UI
            // instead. ;_;
            DoNonAPIReq("", HttpStatusCode.OK);

            string url = "account/login/";
            string payloadStr = string.Format("username={0}&password={1}", _username, _password);
            DoNonAPIReq(url, HttpStatusCode.OK, payloadStr);

            // Now we can do the request to rollback the snapshot.
            string resp = DoNonAPIReq("storage/zvol/delete/" + toDelete.path + "/", HttpStatusCode.OK, "");

            if (resp.Contains("\"error\": true") || !resp.Contains("Volume successfully destroyed"))
                throw new Exception("Volume deletion failed: " + resp);
        }

        public void deleteVolume(volume toDelete)
        {
            string url = String.Format("http://{0}/api/v1.0/storage/volume/{1}/", _serverIp, toDelete.id);
            string payload = "{ \"destroy\": true, \"cascade\": true} ";
            doReq(url, "DELETE", HttpStatusCode.NoContent, payload);
        }

        public void deleteISCSIExtent(iscsiExtent extent)
        {
            string url = String.Format("http://{0}/api/v1.0/services/iscsi/extent/{1}", _serverIp, extent.id);
            doReq(url, "DELETE", HttpStatusCode.NoContent);
        }

        public List<volume> getVolumes()
        {
            string HTTPResponse = doReq("http://" + _serverIp + "/api/v1.0/storage/volume", "get", HttpStatusCode.OK).text;
            return JsonConvert.DeserializeObject<List<volume>>(HTTPResponse);
        }

        public zvol getVolume(volume parent)
        {
            string url = String.Format("http://{0}/api/v1.0/storage/volume/{1}", _serverIp, parent.id);
            string HTTPResponse = doReq(url, "get", HttpStatusCode.OK).text;
            return JsonConvert.DeserializeObject<zvol>(HTTPResponse);
        }

        public void cloneSnapshot(snapshot snapshot, string path)
        {
            string url = String.Format("http://{0}/api/v1.0/storage/snapshot/{1}/clone/", _serverIp, snapshot.fullname);
            string payload = String.Format("{{\"name\": \"{0}\" }}", path);
            doReq(url, "POST", HttpStatusCode.Accepted, payload);
        }

        public volume findVolumeByMountpoint(List<volume> vols, string mountpoint)
        {
            if (vols == null)
                return null;

            volume toRet = vols.SingleOrDefault(x => x.mountpoint == mountpoint);
            if (toRet != null)
                return toRet;

            if (vols.All(x => x.children != null && x.children.Count == 0) || vols.Count == 0)
                return null;

            foreach (volume vol in vols)
            {
                volume maybeThis = findVolumeByMountpoint(vol.children, mountpoint);
                if (maybeThis != null)
                    return maybeThis;
            }
            return null;
        }

        public volume findVolumeByName(List<volume> vols, string name)
        {
            if (vols == null)
                return null;

            volume toRet = vols.SingleOrDefault(x => x.name == name);
            if (toRet != null)
                return toRet;

            if (vols.All(x => x.children != null && x.children.Count == 0) || vols.Count == 0)
                return null;

            foreach (volume vol in vols)
            {
                volume maybeThis = findVolumeByName(vol.children, name);
                if (maybeThis != null)
                    return maybeThis;
            }

            return null;
        }

        public List<snapshot> getSnapshots()
        {
            string HTTPResponse = doReq("http://" + _serverIp + "/api/v1.0/storage/snapshot/", "get", HttpStatusCode.OK).text;
            return JsonConvert.DeserializeObject<List<snapshot>>(HTTPResponse);
        }

        public snapshot createSnapshot(string dataset, string name)
        {
            string payload = String.Format("{{\"dataset\": \"{0}\", " +
                                             "\"name\": \"{1}\" " +
                                           "}}", dataset, name);

            string HTTPResponse = doReq("http://" + _serverIp + "/api/v1.0/storage/snapshot/", "post", HttpStatusCode.Created, payload).text;
            return JsonConvert.DeserializeObject<snapshot>(HTTPResponse);
        }

        public snapshot deleteSnapshot(snapshot toDelete)
        {
            string name = toDelete.fullname;
            name = Uri.EscapeDataString(name);
            string HTTPResponse = doReq("http://" + _serverIp + "/api/v1.0/storage/snapshot/" + name, "DELETE", HttpStatusCode.NoContent).text;
            return JsonConvert.DeserializeObject<snapshot>(HTTPResponse);
        }

        public void rollbackSnapshot(snapshot shotToRestore)
        {
            // Oh no, FreeNAS doesn't export the 'rollback' command via the API! :( We need to log into the web UI and faff with 
            // that in order to rollback instead.
            //
            // First, do an initial GET to / so we can get a CSRF token and some cookies.
            DoNonAPIReq("", HttpStatusCode.OK);

            // Now we can perform the login.
            string url = "account/login/";
            string payloadStr = string.Format("username={0}&password={1}", _username, _password);
            DoNonAPIReq(url, HttpStatusCode.OK, payloadStr);

            // Now we can do the request to rollback the snapshot.
            string resp = DoNonAPIReq("storage/snapshot/rollback/" + shotToRestore.fullname + "/", HttpStatusCode.OK, "");

            if (resp.Contains("\"error\": true") || !resp.Contains("Rollback successful."))
                throw new Exception("Rollback failed: " + resp);
        }

        private string DoNonAPIReq(string urlRel, HttpStatusCode expectedCode, string postVars = null)
        {
            Uri url = new Uri(String.Format("http://{0}/{1}", _serverIp, urlRel), UriKind.Absolute);
            HttpWebRequest req = WebRequest.CreateHttp(url);
            req.UserAgent = "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:42.0) Gecko/20100101 Firefox/42.0";
            req.Accept = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";
            req.CookieContainer = cookies;

            if (postVars != null)
            {
                req.Method = "POST";
                string csrfToken = cookies.GetCookies(url)["csrftoken"].Value;
                string payload = postVars + "&csrfmiddlewaretoken=" + csrfToken;
                Byte[] payloadBytes = Encoding.ASCII.GetBytes(payload);
                req.ContentLength = payloadBytes.Length;
                req.ContentType = "application/x-www-form-urlencoded";
                req.Headers.Add("form_id", "form_str");
                using (Stream s = req.GetRequestStream())
                {
                    s.Write(payloadBytes, 0, payloadBytes.Length);
                }
            }

            HttpWebResponse resp = null;
            try
            {
                using (resp = (HttpWebResponse)req.GetResponse())
                {
                    if (resp.StatusCode != expectedCode)
                        throw new Exception("Statuss code was " + resp.StatusCode + " but expected " + expectedCode + " while requesting " + urlRel);

                    using (Stream respStream = resp.GetResponseStream())
                    {
                        using (StreamReader respStreamReader = new StreamReader(respStream))
                        {
                            return respStreamReader.ReadToEnd();
                        }
                    }
                }
            }
            catch (WebException e)
            {
                string respText;
                using (Stream respStream = e.Response.GetResponseStream())
                {
                    using (StreamReader respStreamReader = new StreamReader(respStream))
                    {
                        respText = respStreamReader.ReadToEnd();
                    }
                }

                Debug.WriteLine(e.Message);
                Debug.WriteLine(respText);

                throw;
            }
        }

        public iscsiTarget addISCSITarget(iscsiTarget toAdd)
        {
            string payload = String.Format("{{\"iscsi_target_name\": \"{0}\", " +
                                             "\"iscsi_target_alias\": \"{1}\" " +
                                           "}}", toAdd.targetName, toAdd.targetAlias);
            string HTTPResponse = doReq("http://" + _serverIp + "/api/v1.0/services/iscsi/target/", "POST", HttpStatusCode.Created, payload).text;
            iscsiTarget created = JsonConvert.DeserializeObject<iscsiTarget>(HTTPResponse);

            return created;
        }

        public iscsiTargetToExtentMapping addISCSITargetToExtent(int targetID, iscsiExtent extent)
        {
            string payload = String.Format("{{" +
                                           "\"iscsi_target\": \"{0}\", " +
                                           "\"iscsi_extent\": \"{1}\", " +
                                           "\"iscsi_lunid\": null " +
                                           "}}", targetID, extent.id);
            string HTTPResponse = doReq("http://" + _serverIp + "/api/v1.0/services/iscsi/targettoextent/", "POST", HttpStatusCode.Created, payload).text;
            iscsiTargetToExtentMapping created = JsonConvert.DeserializeObject<iscsiTargetToExtentMapping>(HTTPResponse);

            return created;
        }

        public targetGroup addTargetGroup(targetGroup toAdd, iscsiTarget target)
        {
            string payload = String.Format("{{\"iscsi_target\": \"{0}\", " +
                                             "\"iscsi_target_authgroup\": \"{1}\", " +
                                             "\"iscsi_target_authtype\": \"{2}\", " +
                                             "\"iscsi_target_portalgroup\": \"{3}\", " +
                                             "\"iscsi_target_initiatorgroup\": \"{4}\", " +
                                             "\"iscsi_target_initialdigest\": \"{5}\" " +
                                           "}}", target.id,
                                           toAdd.iscsi_target_authgroup, toAdd.iscsi_target_authtype, toAdd.iscsi_target_portalgroup,
                                           toAdd.iscsi_target_initiatorgroup, toAdd.iscsi_target_initialdigest);
            string HTTPResponse = doReq("http://" + _serverIp + "/api/v1.0/services/iscsi/targetgroup/", "POST", HttpStatusCode.Created, payload).text;
            targetGroup created = JsonConvert.DeserializeObject<targetGroup>(HTTPResponse);

            return created;
        }

        public volume findParentVolume(List<volume> vols, volume volToFind)
        {
            volume toRet = vols.SingleOrDefault(x => x.children.Count(y => y.name == volToFind.name && x.volType == "dataset") > 0);
            if (toRet != null)
                return toRet;

            if (vols.All(x => x.children.Count == 0) || vols.Count == 0)
                return null;

            foreach (volume vol in vols)
                return findParentVolume(vol.children, volToFind);

            return null;
        }

        public iscsiExtent addISCSIExtent(iscsiExtent extent)
        {
            // Chop off leading '/dev/' from path
            string extentPath = extent.iscsi_target_extent_path;
            if (extentPath.StartsWith("/dev/"))
                extentPath = extentPath.Substring(5);
            string payload = String.Format("{{" +
                                           "\"iscsi_target_extent_type\": \"{0}\", " +
                                           "\"iscsi_target_extent_name\": \"{1}\", " +
                                           //"\"iscsi_target_extent_filesize\": \"{2}\", " +
                                           "\"iscsi_target_extent_disk\": \"{3}\" " +
                                           "}}", "Disk", extent.iscsi_target_extent_name,
                extent.iscsi_target_extent_filesize, extentPath);
            string HTTPResponse = doReq("http://" + _serverIp + "/api/v1.0/services/iscsi/extent/", "POST", HttpStatusCode.Created, payload).text;
            iscsiExtent created = JsonConvert.DeserializeObject<iscsiExtent>(HTTPResponse);

            return created;

        }

        public List<iscsiPortal> getPortals()
        {
            string HTTPResponse = doReq("http://" + _serverIp + "/api/v1.0/services/iscsi/portal/", "get", HttpStatusCode.OK).text;
            return JsonConvert.DeserializeObject<List<iscsiPortal>>(HTTPResponse);
        }

        public List<targetGroup> getTargetGroups()
        {
            string HTTPResponse = doReq("http://" + _serverIp + "/api/v1.0/services/iscsi/targetgroup/", "get", HttpStatusCode.OK).text;
            return JsonConvert.DeserializeObject<List<targetGroup>>(HTTPResponse);
        }
    }

    public class zvol
    {
        [JsonProperty("name")]
        public string name { get; set; }

        [JsonProperty("volsize")]
        public int volSize { get; set; }

        [JsonProperty("id")]
        public int id { get; set; }

        [JsonProperty("children")]
        public zvol[] children { get; set; }
    }

    public class targetGroup
    {
        [JsonProperty("id")]
        public int id { get; set; }

        [JsonProperty("iscsi_target")]
        public int iscsi_target { get; set; }

        [JsonProperty("iscsi_target_authgroup")]
        public string iscsi_target_authgroup { get; set; }

        [JsonProperty("iscsi_target_authtype")]
        public string iscsi_target_authtype { get; set; }

        [JsonProperty("iscsi_target_initialdigest")]
        public string iscsi_target_initialdigest { get; set; }

        [JsonProperty("iscsi_target_initiatorgroup")]
        public string iscsi_target_initiatorgroup { get; set; }

        [JsonProperty("iscsi_target_portalgroup")]
        public string iscsi_target_portalgroup { get; set; }
    }

    public class iscsiPortal
    {
        [JsonProperty("id")]
        public string id { get; set; }

        [JsonProperty("iscsi_target_portal_comment")]
        public string iscsi_target_portal_comment { get; set; }

        [JsonProperty("iscsi_target_portal_discoveryauthgroup")]
        public string iscsi_target_portal_discoveryauthgroup { get; set; }

        [JsonProperty("iscsi_target_portal_discoveryauthmethod")]
        public string iscsi_target_portal_discoveryauthmethod { get; set; }

        [JsonProperty("iscsi_target_portal_tag")]
        public string iscsi_target_portal_tag { get; set; }

        [JsonProperty("iscsi_target_portal_ips")]
        public string[] iscsi_target_portal_ips { get; set; }
    }


    public class snapshot
    {
        [JsonProperty("filesystem")]
        public string filesystem { get; set; }

        [JsonProperty("fullname")]
        public string fullname { get; set; }

        [JsonProperty("id")]
        public string id { get; set; }

        [JsonProperty("mostrecent")]
        public string mostrecent { get; set; }

        [JsonProperty("name")]
        public string name { get; set; }

        [JsonProperty("parent_type")]
        public string parent_type { get; set; }

        [JsonProperty("refer")]
        public string refer { get; set; }

        [JsonProperty("replication")]
        public string replication { get; set; }

        [JsonProperty("used")]
        public string used { get; set; }
    }

    public class volume
    {
        [JsonProperty("id")]
        public int id { get; set; }

        [JsonProperty("avail")]
        public string avail { get; set; }

        [JsonProperty("compression")]
        public string compression { get; set; }

        [JsonProperty("compressratio")]
        public string compressratio { get; set; }

        [JsonProperty("is_decrypted")]
        public string is_decrypted { get; set; }

        [JsonProperty("is_upgraded")]
        public string is_upgraded { get; set; }

        [JsonProperty("mountpoint")]
        public string mountpoint { get; set; }

        [JsonProperty("name")]
        public string name { get; set; }

        [JsonProperty("path")]
        public string path { get; set; }

        [JsonProperty("readonly")]
        public string isreadonly { get; set; }

        [JsonProperty("used")]
        public string used { get; set; }

        [JsonProperty("used_pct")]
        public string used_pct { get; set; }

        [JsonProperty("vol_encrypt")]
        public string vol_encrypt { get; set; }

        [JsonProperty("vol_encryptkey")]
        public string vol_encryptkey { get; set; }

        [JsonProperty("vol_fstype")]
        public string vol_fstype { get; set; }

        [JsonProperty("vol_guid")]
        public string vol_guid { get; set; }

        [JsonProperty("vol_name")]
        public string vol_name { get; set; }

        [JsonProperty("status")]
        public string status { get; set; }

        [JsonProperty("type")]
        public string volType { get; set; }

        [JsonProperty("children")]
        public List<volume> children { get; set; }

    }

    public class iscsiExtent
    {
        [JsonProperty("id")]
        public int id { get; set; }

        [JsonProperty("iscsi_target_extent_avail_threshold")]
        public string iscsi_target_extent_avail_threshold { get; set; }

        [JsonProperty("iscsi_target_extent_blocksize")]
        public string iscsi_target_extent_blocksize { get; set; }

        [JsonProperty("iscsi_target_extent_comment")]
        public string iscsi_target_extent_comment { get; set; }

        [JsonProperty("iscsi_target_extent_filesize")]
        public string iscsi_target_extent_filesize { get; set; }

        [JsonProperty("iscsi_target_extent_insecure_tpc")]
        public string iscsi_target_extent_insecure_tpc { get; set; }

        [JsonProperty("iscsi_target_extent_legacy")]
        public string iscsi_target_extent_legacy { get; set; }

        [JsonProperty("iscsi_target_extent_naa")]
        public string iscsi_target_extent_naa { get; set; }

        [JsonProperty("iscsi_target_extent_name")]
        public string iscsi_target_extent_name { get; set; }

        [JsonProperty("iscsi_target_extent_path")]
        public string iscsi_target_extent_path { get; set; }

        [JsonProperty("iscsi_target_extent_pblocksize")]
        public string iscsi_target_extent_pblocksize { get; set; }

        [JsonProperty("iscsi_target_extent_ro")]
        public string iscsi_target_extent_ro { get; set; }

        [JsonProperty("iscsi_target_extent_rpm")]
        public string iscsi_target_extent_rpm { get; set; }

        [JsonProperty("iscsi_target_extent_serial")]
        public string iscsi_target_extent_serial { get; set; }

        [JsonProperty("iscsi_target_extent_type")]
        public string iscsi_target_extent_type { get; set; }

        [JsonProperty("iscsi_target_extent_xen")]
        public string iscsi_target_extent_xen { get; set; }
    }

    public class iscsiTargetToExtentMapping
    {
        [JsonProperty("iscsi_target")]
        public int iscsi_target { get; set; }

        [JsonProperty("iscsi_extent")]
        public int iscsi_extent { get; set; }

        [JsonProperty("iscsi_lunid")]
        public string iscsi_lunid { get; set; }

        [JsonProperty("id")]
        public int id { get; set; }
    }

    public class iscsiTarget
    {
        [JsonProperty("iscsi_target_name")]
        public string targetName { get; set; }

        [JsonProperty("iscsi_target_alias")]
        public string targetAlias { get; set; }

        [JsonProperty("id")]
        public int id { get; set; }
    }
}
