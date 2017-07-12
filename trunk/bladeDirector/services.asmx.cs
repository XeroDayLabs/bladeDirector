using System;
using System.ComponentModel;
using System.Web;
using System.Web.Services;

namespace bladeDirector
{
    /// <summary>
    /// Summary description for requestNode
    /// </summary>
    [WebService(Namespace = "http://tempuri.org/")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [ToolboxItem(false)]
    // To allow this Web Service to be called from script, using ASP.NET AJAX, uncomment the following line. 
    // [System.Web.Script.Services.ScriptService]
    public class services : WebService
    {
        public static hostStateManager hostStateManager;

        public static void initWithBlades(string[] bladeIPs)
        {
            hostStateManager = new hostStateManager();
            hostStateManager.initWithBlades(bladeIPs);
        }

        public static void initWithBlades(bladeSpec[] spec)
        {
            hostStateManager = new hostStateManager();
            hostStateManager.initWithBlades(spec);
        }

        [WebMethod]
        public void keepAlive()
        {
            string srcIp = sanitizeAddress(HttpContext.Current.Request.UserHostAddress);
            _keepAlive(srcIp);
        }

        [WebMethod]
        public string logIn()
        {
            string srcIp = sanitizeAddress(HttpContext.Current.Request.UserHostAddress);
            return hostStateManager.logIn(srcIp);
        }

        [WebMethod]
        public resultCode getLogInProgress(string waitToken)
        {
            return hostStateManager.getLogInProgress(waitToken);
        }

        public void _keepAlive(string srcIP)
        {
            hostStateManager.keepAlive(srcIP);
        }

        [WebMethod]
        public string ListNodes()
        {
            string[] IPAddresses = hostStateManager.db.getAllBladeIP();
            return String.Join(",", IPAddresses);
        }

        [WebMethod]
        public string getBladesByAllocatedServer(string NodeIP)
        {
            string[] IPAddresses = hostStateManager.getBladesByAllocatedServer(NodeIP);
            return String.Join(",", IPAddresses);
        }

        [WebMethod]
        public resultCodeAndBladeName RequestAnySingleNode()
        {
            return RequestAnySingleNode(HttpContext.Current.Request.UserHostAddress);
        }

        public resultCodeAndBladeName RequestAnySingleNode(string requestorIP)
        {
            string srcIp = sanitizeAddress(requestorIP);
            return hostStateManager.RequestAnySingleNode(srcIp);
        }

        [WebMethod]
        public GetBladeStatusResult GetBladeStatus(string NodeIP)
        {
            string srcIp = sanitizeAddress(HttpContext.Current.Request.UserHostAddress);
            return GetBladeStatus(NodeIP, srcIp);
        }

        public GetBladeStatusResult GetBladeStatus(string nodeIP, string requestorIP)
        {
            return hostStateManager.getBladeStatus(nodeIP, requestorIP);
        }

        [WebMethod]
        public bool isBladeMine(string NodeIP)
        {
            string srcIp = sanitizeAddress(HttpContext.Current.Request.UserHostAddress);
            return isBladeMine(NodeIP, srcIp);
        }

        public bool isBladeMine(string nodeIP, string requestorIP)
        {
            return hostStateManager.isBladeMine(nodeIP, requestorIP);
        }

        [WebMethod]
        public resultCode releaseBladeOrVM(string NodeIP)
        {
            string srcIp = sanitizeAddress(HttpContext.Current.Request.UserHostAddress);
            return hostStateManager.releaseBladeOrVM(NodeIP, srcIp, false);
        }

        [WebMethod]
        public resultCode releaseBladeDbg(string nodeIP, string requestorIP, bool force = false)
        {
            return hostStateManager.releaseBladeOrVM(nodeIP, requestorIP, force);
        }

        [WebMethod]
        public string getCurrentSnapshotForBlade(string NodeIP)
        {
            return hostStateManager.getCurrentSnapshotForBladeOrVM(NodeIP);
        }

        [WebMethod]
        public resultCode selectSnapshotForBladeOrVM(string NodeIP, string snapshotName)
        {
            using (disposingList<lockableBladeSpec> blades = hostStateManager.db.getAllBladeInfo(x => x.bladeIP == NodeIP, bladeLockType.lockAll))
            {
                if (blades.Count > 0)
                    return hostStateManager.selectSnapshotForBlade(NodeIP, snapshotName);
            }
            using (disposingList<lockableVMSpec> VMs = hostStateManager.db.getAllVMInfo(x => x.VMIP == NodeIP))
            {
                if (VMs.Count > 0)
                    return hostStateManager.selectSnapshotForVM(NodeIP, snapshotName);
            }

            return resultCode.bladeNotFound;
        }
        
        [WebMethod]
        public string getFreeNASSnapshotPath(string NodeIP)
        {
            string requestorIP = sanitizeAddress(HttpContext.Current.Request.UserHostAddress);
            return hostStateManager.getFreeNASSnapshotPath(requestorIP, NodeIP);
        }

        [WebMethod]
        public string getLastDeployedBIOSForBlade(string NodeIP)
        {
            return hostStateManager.getLastDeployedBIOSForBlade(NodeIP);
        }

        [WebMethod]
        public resultCode rebootAndStartDeployingBIOSToBlade(string NodeIP, string BIOSXML)
        {
            string requestorIP = sanitizeAddress(HttpContext.Current.Request.UserHostAddress);
            return hostStateManager.rebootAndStartDeployingBIOSToBlade(NodeIP, requestorIP, BIOSXML);
        }

        [WebMethod]
        public resultCode rebootAndStartReadingBIOSConfiguration(string NodeIP)
        {
            string requestorIP = sanitizeAddress(HttpContext.Current.Request.UserHostAddress);
            return hostStateManager.rebootAndStartReadingBIOSConfiguration(NodeIP, requestorIP);
        }

        [WebMethod]
        public resultCode checkBIOSDeployProgress(string NodeIP)
        {
            return hostStateManager.checkBIOSWriteProgress(NodeIP);
        }

        [WebMethod]
        public resultCodeAndBIOSConfig checkBIOSReadProgress(string NodeIP)
        {
            return hostStateManager.checkBIOSReadProgress(NodeIP);
        }

        [WebMethod]
        public resultAndBladeName RequestAnySingleVM(VMHardwareSpec hwSpec, VMSoftwareSpec swSpec)
        {
            string requestorIP = sanitizeAddress(HttpContext.Current.Request.UserHostAddress);
            return hostStateManager.RequestAnySingleVM(requestorIP, hwSpec, swSpec);
        }

        public resultAndBladeName RequestAnySingleVM(VMHardwareSpec hwSpec, VMSoftwareSpec swSpec, string requestorIP)
        {
            return hostStateManager.RequestAnySingleVM(requestorIP, hwSpec, swSpec);
        }

        [WebMethod]
        public resultAndBladeName getProgressOfVMRequest(waitTokenType waitToken)
        {
            return hostStateManager.getProgressOfVMRequest(waitToken);
        }
        
        // Only call me from tests, not in 'real' code, because of locking issues otherwise.
        [WebMethod]
        public vmSpec getConfigurationOfVM(string bladeName)
        {
            return hostStateManager.db.getVMByIP_withoutLocking(bladeName);
        }

        // Only call me from tests, not in 'real' code, because of locking issues otherwise.
/*        [WebMethod]
        public resultCode forceBladeAllocation(string NodeIP, string newOwner)
        {
            return hostStateDB.forceBladeAllocation(NodeIP, newOwner);
        }*/

        // Only call me from tests, not in 'real' code, because of locking issues otherwise.
        [WebMethod]
        public bladeSpec getConfigurationOfBlade(string NodeIP)
        {
            return hostStateManager.db.getBladeByIP_withoutLocking(NodeIP);
        }

        private string sanitizeAddress(string toSanitize)
        {
            // The ipv6 loopback, ::1, gets used sometimes during VM provisioning. Because of that, we escape the colons into
            // something that can be present in clone/target/extent names.
            return toSanitize.Replace(":", "-");
        }
    }
}
