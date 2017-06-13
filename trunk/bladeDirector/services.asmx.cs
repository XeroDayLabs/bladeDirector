using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel.Web;
using System.Web;
using System.Web.Services;

namespace bladeDirector
{
    /// <summary>
    /// Summary description for requestNode
    /// </summary>
    [WebService(Namespace = "http://tempuri.org/")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]
    // To allow this Web Service to be called from script, using ASP.NET AJAX, uncomment the following line. 
    // [System.Web.Script.Services.ScriptService]
    public class services : System.Web.Services.WebService
    {
        public static void initWithBlades(string[] bladeIPs)
        {
            hostStateDB.dbFilename = ":memory:";
            hostStateDB.initWithBlades(bladeIPs);
        }

        public static void initWithBlades(bladeSpec[] spec)
        {
            hostStateDB.dbFilename = ":memory:";
            hostStateDB.initWithBlades(spec);
        }

        [WebMethod]
        public void keepAlive()
        {
            string srcIp = sanitizeAddress(HttpContext.Current.Request.UserHostAddress);
            _keepAlive(srcIp);
        }

        public void _keepAlive(string srcIP)
        {
            hostStateDB.keepAlive(srcIP);
        }

        [WebMethod]
        public string ListNodes()
        {
            string[] IPAddresses = hostStateDB.getAllBladeIP();
            return String.Join(",", IPAddresses);
        }

        [WebMethod]
        public string getBladesByAllocatedServer(string NodeIP)
        {
            string[] IPAddresses = hostStateDB.getBladesByAllocatedServer(NodeIP);
            return String.Join(",", IPAddresses);
        }

        [WebMethod]
        public resultCodeAndBladeName RequestAnySingleNode()
        {
            string srcIp = sanitizeAddress(HttpContext.Current.Request.UserHostAddress);
            return hostStateDB.RequestAnySingleNode(srcIp);
        }

        [WebMethod]
        public resultCode RequestNode(string NodeIP)
        {
            string srcIp = sanitizeAddress(HttpContext.Current.Request.UserHostAddress);
            return RequestNode(NodeIP, srcIp);
        }

        public resultCode RequestNode(string NodeIP, string requestorIP)
        {
            return hostStateDB.tryRequestNode(NodeIP, requestorIP);
        }

        [WebMethod]
        public GetBladeStatusResult GetBladeStatus(string NodeIP)
        {
            string srcIp = sanitizeAddress(HttpContext.Current.Request.UserHostAddress);
            return GetBladeStatus(NodeIP, srcIp);
        }

        public GetBladeStatusResult GetBladeStatus(string nodeIP, string requestorIP)
        {
            return hostStateDB.getBladeStatus(nodeIP, requestorIP);
        }

        [WebMethod]
        public bool isBladeMine(string NodeIP)
        {
            string srcIp = sanitizeAddress(HttpContext.Current.Request.UserHostAddress);
            return isBladeMine(NodeIP, srcIp);
        }

        public bool isBladeMine(string nodeIP, string requestorIP)
        {
            return hostStateDB.isBladeMine(nodeIP, requestorIP);
        }

        [WebMethod]
        public resultCode releaseBladeOrVM(string NodeIP)
        {
            string srcIp = sanitizeAddress(HttpContext.Current.Request.UserHostAddress);
            return hostStateDB.releaseBladeOrVM(NodeIP, srcIp, false);
        }

        [WebMethod]
        public resultCode releaseBladeDbg(string nodeIP, string requestorIP, bool force = false)
        {
            return hostStateDB.releaseBladeOrVM(nodeIP, requestorIP, force);
        }

        [WebMethod]
        public resultCode forceBladeAllocation(string NodeIP, string newOwner)
        {
            return hostStateDB.forceBladeAllocation(NodeIP, newOwner);
        }

        [WebMethod]
        public bladeSpec getConfigurationOfBlade(string NodeIP)
        {
            return hostStateDB.getConfigurationOfBlade(NodeIP);
        }

        [WebMethod]
        public bladeSpec getConfigurationOfBladeByID(int NodeID)
        {
            return hostStateDB.getConfigurationOfBladeByID(NodeID);
        }
        
        [WebMethod]
        public string getCurrentSnapshotForBlade(string NodeIP)
        {
            return hostStateDB.getCurrentSnapshotForBladeOrVM(NodeIP);
        }

        [WebMethod]
        public resultCode selectSnapshotForBladeOrVM(string NodeIP, string snapshotName)
        {
            return hostStateDB.selectSnapshotForBladeOrVM(NodeIP, snapshotName);
        }

        [WebMethod]
        public resultCode selectSnapshotForBladeOrVM_getProgress(string NodeIP)
        {
            return hostStateDB.selectSnapshotForBladeOrVM_getProgress(NodeIP);
        }
        
        [WebMethod]
        public string getFreeNASSnapshotPath(string NodeIP)
        {
            string requestorIP = sanitizeAddress(HttpContext.Current.Request.UserHostAddress);
            return hostStateDB.getFreeNASSnapshotPath(requestorIP, NodeIP);
        }

        [WebMethod]
        public string getLastDeployedBIOSForBlade(string NodeIP)
        {
            return hostStateDB.getLastDeployedBIOSForBlade(NodeIP);
        }

        [WebMethod]
        public resultCode rebootAndStartDeployingBIOSToBlade(string NodeIP, string BIOSXML)
        {
            string requestorIP = sanitizeAddress(HttpContext.Current.Request.UserHostAddress);
            return hostStateDB.rebootAndStartDeployingBIOSToBlade(NodeIP, requestorIP, BIOSXML);
        }

        [WebMethod]
        public resultCode rebootAndStartReadingBIOSConfiguration(string NodeIP)
        {
            string requestorIP = sanitizeAddress(HttpContext.Current.Request.UserHostAddress);
            return hostStateDB.rebootAndStartReadingBIOSConfiguration(NodeIP, requestorIP);
        }

        [WebMethod]
        public resultCode checkBIOSDeployProgress(string NodeIP)
        {
            return hostStateDB.checkBIOSWriteProgress(NodeIP);
        }

        [WebMethod]
        public resultCodeAndBIOSConfig checkBIOSReadProgress(string NodeIP)
        {
            return hostStateDB.checkBIOSReadProgress(NodeIP);
        }

        [WebMethod]
        public resultCodeAndBladeName RequestAnySingleVM(VMHardwareSpec hwSpec, VMSoftwareSpec swSpec)
        {
            string requestorIP = sanitizeAddress(HttpContext.Current.Request.UserHostAddress);
            return hostStateDB.RequestAnySingleVM(requestorIP, hwSpec, swSpec);
        }

        public resultCodeAndBladeName RequestAnySingleVM(VMHardwareSpec hwSpec, VMSoftwareSpec swSpec, string requestorIP)
        {
            return hostStateDB.RequestAnySingleVM(requestorIP, hwSpec, swSpec);
        }

        [WebMethod]
        public resultCodeAndBladeName getProgressOfVMRequest(string waitToken)
        {
            return hostStateDB.RequestAnySingleVM_getProgress(waitToken);
        }

        [WebMethod]
        public vmSpec getConfigurationOfVM(string bladeName)
        {
            return hostStateDB.getVMByIP(bladeName);
        }

        private string sanitizeAddress(string toSanitize)
        {
            // The ipv6 loopback, ::1, gets used sometimes during VM provisioning. Because of that, we escape the colons into
            // something that can be present in clone/target/extent names.
            return toSanitize.Replace(":", "-");
        }
    }
}
