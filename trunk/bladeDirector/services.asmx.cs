using System;
using System.ComponentModel;
using System.ServiceModel;
using System.Web;
using System.Web.Services;

namespace bladeDirector
{
    public class services : IServices
    {
        public static hostStateManager_core hostStateManager;

        public void keepAlive()
        {
            keepAlive(HttpContext.Current.Request.UserHostAddress);
        }

        [WebMethod]
        public string logIn()
        {
            return hostStateManager.logIn(HttpContext.Current.Request.UserHostAddress);
        }

        [WebMethod]
        public resultCode getLogInProgress(string waitToken)
        {
            return hostStateManager.getLogInProgress(waitToken);
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

        [WebMethod]
        public GetBladeStatusResult GetBladeStatus(string NodeIP)
        {
            return GetBladeStatus(HttpContext.Current.Request.UserHostAddress, NodeIP);
        }

        [WebMethod]
        public bool isBladeMine(string NodeIP)
        {
            return isBladeMine(NodeIP, HttpContext.Current.Request.UserHostAddress);
        }

        [WebMethod]
        public resultCode releaseBladeOrVM(string NodeIP)
        {
            return ReleaseBlade(NodeIP, HttpContext.Current.Request.UserHostAddress, false);
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
            return rebootAndStartDeployingBIOSToBlade(NodeIP, HttpContext.Current.Request.UserHostAddress, BIOSXML);
        }

        [WebMethod]
        public resultCode rebootAndStartReadingBIOSConfiguration(string NodeIP)
        {
            return rebootAndStartReadingBIOSConfiguration(NodeIP, HttpContext.Current.Request.UserHostAddress);
        }

        [WebMethod]
        public resultCodeAndBIOSConfig checkBIOSOperationProgress(string NodeIP)
        {
            return checkBIOSOperationProgress(HttpContext.Current.Request.UserHostAddress, NodeIP);
        }

        [WebMethod]
        public resultAndBladeName RequestAnySingleVM(VMHardwareSpec hwSpec, VMSoftwareSpec swSpec)
        {
            return RequestAnySingleVM(HttpContext.Current.Request.UserHostAddress, hwSpec, swSpec);
        }

        [WebMethod]
        public resultAndBladeName getProgressOfVMRequest(string waitToken)
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
        [WebMethod]
        public bladeSpec getConfigurationOfBlade(string NodeIP)
        {
            return hostStateManager.db.getBladeByIP_withoutLocking(NodeIP);
        }


#region things not exposed to the client

        public static void keepAlive(string srcIP)
        {
            hostStateManager.keepAlive(sanitizeAddress(srcIP));
        }

        public static string logIn(string requestorIP)
        {
            return hostStateManager.logIn(sanitizeAddress(requestorIP));
        }

        public static resultCodeAndBladeName RequestAnySingleNode(string requestorIP)
        {
            return hostStateManager.RequestAnySingleNode(sanitizeAddress(requestorIP));
        }

        public static GetBladeStatusResult GetBladeStatus(string requestorIP, string nodeIP)
        {
            return hostStateManager.getBladeStatus(sanitizeAddress(requestorIP), sanitizeAddress(nodeIP) );
        }

        public static resultCode ReleaseBlade(string nodeIP, string requestorIP, bool force)
        {
            return hostStateManager.releaseBladeOrVM(sanitizeAddress(nodeIP), sanitizeAddress(requestorIP), force);
        }

        public static bool isBladeMine(string nodeIP, string requestorIP)
        {
            return hostStateManager.isBladeMine(sanitizeAddress(nodeIP), sanitizeAddress(requestorIP));
        }

        public static resultCode rebootAndStartDeployingBIOSToBlade(string NodeIP, string BIOSXML, string requestorIP)
        {
            return hostStateManager.rebootAndStartDeployingBIOSToBlade(sanitizeAddress(NodeIP), sanitizeAddress(requestorIP), BIOSXML );
        }

        public static resultCode rebootAndStartReadingBIOSConfiguration(string NodeIP, string requestorIP)
        {
            return hostStateManager.rebootAndStartReadingBIOSConfiguration(sanitizeAddress(NodeIP), sanitizeAddress(requestorIP));
        }

        public static resultCodeAndBIOSConfig checkBIOSOperationProgress(string requestorIP, string NodeIP)
        {
            return hostStateManager.checkBIOSOperationProgress(sanitizeAddress(requestorIP), sanitizeAddress(NodeIP));
        }

        public static resultAndBladeName RequestAnySingleVM(string requestorIP, VMHardwareSpec hwSpec, VMSoftwareSpec swSpec)
        {
            return hostStateManager.RequestAnySingleVM(sanitizeAddress(requestorIP), hwSpec, swSpec);
        }

        private static string sanitizeAddress(string toSanitize)
        {
            // The ipv6 loopback, ::1, gets used sometimes during VM provisioning. Because of that, we escape the colons into
            // something that can be present in clone/target/extent names.
            return toSanitize.Replace(":", "-");
        }
#endregion
    }
}
