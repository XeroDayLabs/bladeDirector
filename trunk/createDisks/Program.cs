using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using hypervisors;
using Newtonsoft.Json;
using VMware.Vim;
using Action = System.Action;
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

    class Program
    {
        static void Main(string[] args)
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
                    string cloneName = String.Format("{0}-{1}", nodeIP, serverIP);
                    string computerName = String.Format("blade{0}", uint.Parse(bladeIP).ToString("D2"));
                    string snapshotName = string.Format("{0}-clean", cloneName);

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

            createClonesAndExportViaiSCSI(nas, toClone, toCloneVolume, itemsToAdd);

            // Next, we must prepare each clone. Do this in parallel, per-server.
            foreach (string serverIP in serverIPs)
            {
                itemToAdd[] toPrep = itemsToAdd.Where(x => x.serverIP == serverIP).ToArray();
                Task[] toWaitOn = new Task[toPrep.Length];
                for (int index = 0; index < toPrep.Length; index++)
                {
                    itemToAdd itemToAdd = toPrep[index];

                    toWaitOn[index] = new Task(() => prepareCloneImage(itemToAdd, toCloneVolume));
                    toWaitOn[index].Start();
                }

                foreach (Task task in toWaitOn)
                    task.Wait();
            }
        }

        private static void prepareCloneImage(itemToAdd itemToAdd, string toCloneVolume)
        {
            Console.WriteLine("Preparing " + itemToAdd.bladeIP + " for server " + itemToAdd.bladeIP);

            string nasIP = Properties.Settings.Default.iscsiServerIP;
            string nasUsername = Properties.Settings.Default.iscsiServerUsername;
            string nasPassword = Properties.Settings.Default.iscsiServerPassword;

            FreeNAS nas = new FreeNAS(nasIP, nasUsername, nasPassword);

            hypSpec_iLo spec = new hypSpec_iLo(
                itemToAdd.bladeIP,
                Properties.Settings.Default.iloHostUsername, Properties.Settings.Default.iloHostPassword,
                itemToAdd.nodeiLoIP, Properties.Settings.Default.iloUsername, Properties.Settings.Default.iloPassword,
                nasIP, nasUsername, nasPassword,
                "", 0, "");
            hypervisor_iLo_appdomainPayload ilo = new hypervisor_iLo_appdomainPayload(spec);
            ilo.connect();
            ilo.powerOff();

            // We must ensure the blade is allocated to the required blade before we power it on. This will cause it to
            // use the required iSCSI root path.
            string url = Properties.Settings.Default.directorURL;
            using (bladeDirector.servicesSoapClient bladeDirectorClient = new bladeDirector.servicesSoapClient("servicesSoap", url))
            {
                string res = bladeDirectorClient.forceBladeAllocation(itemToAdd.bladeIP, itemToAdd.serverIP);
            }
            ilo.powerOn();
            // and then use psexec to name it.
            ilo.startExecutable("C:\\windows\\system32\\cmd", "/c wmic computersystem where caption='%COMPUTERNAME%' call rename " + itemToAdd.computerName);
            // Set the target of the kernel debugger, and enable it.
            ilo.startExecutable("C:\\windows\\system32\\cmd", "/c bcdedit /dbgsettings net hostip:" + itemToAdd.serverIP + " port:" + itemToAdd.kernelDebugPort);
            ilo.startExecutable("C:\\windows\\system32\\cmd", "/c bcdedit /debug on");
            ilo.startExecutable("C:\\windows\\system32\\cmd", "/c shutdown -s -f -t 01");

            // Once it has shut down totally, we can take the snapshot of it.
            ilo.WaitForStatus(false, TimeSpan.FromMinutes(1));

            nas.createSnapshot(toCloneVolume + "/" + itemToAdd.cloneName, itemToAdd.snapshotName);
        }
        
        private static void createClonesAndExportViaiSCSI(FreeNAS nas, snapshot toClone, string toCloneVolume, List<itemToAdd> itemsToAdd)
        {
            // Now we can create snapshots
            foreach (itemToAdd itemToAdd in itemsToAdd)
            {
                Console.WriteLine("Creating snapshot clones for server '" + itemToAdd.serverIP + "' node '" + itemToAdd.bladeIP + "'");
                string fullCloneName = String.Format("{0}/{1}", toCloneVolume, itemToAdd.cloneName);
                nas.cloneSnapshot(toClone, fullCloneName);
            }

            // Now expose each via iSCSI.
            targetGroup tgtGrp = nas.getTargetGroups()[0];
            foreach (itemToAdd itemToAdd in itemsToAdd)
            {
                Console.WriteLine("Creating iSCSI target/extent/link for server '" + itemToAdd.serverIP + "' node '" + itemToAdd.bladeIP + "'");

                iscsiTarget toAdd = new iscsiTarget();
                toAdd.targetAlias = itemToAdd.cloneName;
                toAdd.targetName = itemToAdd.cloneName;
                iscsiTarget newTarget = nas.addISCSITarget(toAdd);

                iscsiExtent newExtent = new iscsiExtent()
                {
                    iscsi_target_extent_name = toAdd.targetName,
                    iscsi_target_extent_path = String.Format("zvol/SSDs/{0}", toAdd.targetName)
                };
                newExtent = nas.addISCSIExtent(newExtent);

                nas.addTargetGroup(tgtGrp, newTarget);

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

        private void deleteZVol(volume parent, volume toDelete)
        {
            string url = String.Format("http://{0}/api/v1.0/storage/volume/{1}/zvols/{2}", _serverIp, parent.name, toDelete.name);
            doReq(url, "DELETE", HttpStatusCode.NoContent);
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
            string HTTPResponse = doReq("http://" + _serverIp + "/api/v1.0/storage/snapshot/?format=json", "get", HttpStatusCode.OK).text;
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

        public void rollbackSnapshot(snapshot shotToRestore) //, volume parentVolume, volume clone)
        {
            // Oh no, FreeNAS doesn't export the 'rollback' command via the API! :( We need to log into the web UI and faff with 
            // that in order to rollback instead.
            //
            // First, do an initial GET to / so we can get a CSRF token and some cookies.
            DoNonAPIReq("", HttpStatusCode.OK);
            //doInitialReq();

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
            string HTTPResponse = doReq("http://" + _serverIp + "/api/v1.0/services/iscsi/portal/?format=json", "get", HttpStatusCode.OK).text;
            return JsonConvert.DeserializeObject<List<iscsiPortal>>(HTTPResponse);
        }

        public List<targetGroup> getTargetGroups()
        {
            string HTTPResponse = doReq("http://" + _serverIp + "/api/v1.0/services/iscsi/targetgroup/?format=json", "get", HttpStatusCode.OK).text;
            return JsonConvert.DeserializeObject<List<targetGroup>>(HTTPResponse);
        }
    }

    public class targetGroup
    {
        [JsonProperty("id")]
        public string id { get; set; }

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
