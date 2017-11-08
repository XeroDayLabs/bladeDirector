using System;
using System.ServiceModel;
using System.ServiceModel.Channels;
using hypervisors;

namespace bladeDirectorWCF
{
    [ServiceBehavior(AddressFilterMode = AddressFilterMode.Any)]
    public class services : IServices
    {
        public static hostStateManager_core hostStateManager = new hostStateManager();

        #region static
        private static void _keepAlive(string srcIP)
        {
            hostStateManager.keepAlive(sanitizeAddress(srcIP));
        }

        public static resultAndWaitToken _logIn(string requestorIP)
        {
            return hostStateManager.logIn(sanitizeAddress(requestorIP));
        }

        public static void _setResourceSharingModel(fairnessChecker.fairnessType fairnessType)
        {
            hostStateManager.setResourceSharingModel(fairnessType);
        }

        public static void _setWebSvcURL(string newURL)
        {
            hostStateManager.setWebSvcURL(newURL);
        }

        public static string _getWebSvcURL(string srcIP)
        {
            return hostStateManager.getWebSvcURL(srcIP);
        }

        public static NASParams _getNASParams(string srcIP)
        {
            return hostStateManager.getNASParams(srcIP);
        }

        private static resultAndWaitToken _getProgress(waitToken waitToken)
        {
            return hostStateManager.getProgress(waitToken);
        }

        private static string[] _getAllBladeIP()
        {
            return hostStateManager.getAllBladeIP();
        }

        private static resultAndBladeName _RequestAnySingleNode(string requestorIP)
        {
            return hostStateManager.RequestAnySingleNode(sanitizeAddress(requestorIP));
        }

        public static resultAndBladeName _RequestSpecificNode(string requestorIP, string nodeIP)
        {
            return hostStateManager.RequestSpecificNode(sanitizeAddress(requestorIP), nodeIP);
        }

        private static GetBladeStatusResult _GetBladeStatus(string requestorIP, string nodeIP)
        {
            return hostStateManager.getBladeStatus(sanitizeAddress(requestorIP), sanitizeAddress(nodeIP));
        }

        private GetBladeStatusResult _GetVMStatus(string requestorIP, string nodeIP)
        {
            return hostStateManager.getVMStatus(sanitizeAddress(requestorIP), sanitizeAddress(nodeIP));
        }

        private static resultAndWaitToken _ReleaseBlade(string nodeIP, string requestorIP, bool force)
        {
            return hostStateManager.releaseBladeOrVM(sanitizeAddress(nodeIP), sanitizeAddress(requestorIP), force);
        }

        private static bool _isBladeMine(string nodeIP, string requestorIP)
        {
            return hostStateManager.isBladeMine(sanitizeAddress(nodeIP), sanitizeAddress(requestorIP));
        }

        private static resultAndWaitToken _rebootAndStartDeployingBIOSToBlade(string NodeIP, string BIOSXML, string requestorIP)
        {
            return hostStateManager.rebootAndStartDeployingBIOSToBlade(sanitizeAddress(NodeIP), sanitizeAddress(requestorIP), BIOSXML);
        }

        private static resultAndWaitToken _rebootAndStartReadingBIOSConfiguration(string NodeIP, string requestorIP)
        {
            return hostStateManager.rebootAndStartReadingBIOSConfiguration(sanitizeAddress(NodeIP), sanitizeAddress(requestorIP));
        }
        
        private static resultAndBladeName _RequestAnySingleVM(string requestorIP, VMHardwareSpec hwSpec, VMSoftwareSpec swSpec)
        {
            return hostStateManager.RequestAnySingleVM(sanitizeAddress(requestorIP), hwSpec, swSpec);
        }

        private static resultAndBladeName[] _requestAsManyVMAsPossible(string requestorIP, VMHardwareSpec hwSpec, VMSoftwareSpec swSpec)
        {
            return hostStateManager.requestAsManyVMAsPossible(sanitizeAddress(requestorIP), hwSpec, swSpec);
        }

        public static string _generateIPXEScript(string requestorIP)
        {
            return hostStateManager.generateIPXEScript(sanitizeAddress(requestorIP));
        }
        
        private static TimeSpan _getKeepAliveTimeout()
        {
            return hostStateManager.getKeepAliveTimeout();
        }

        private resultCode _addNode(string newIP, string iScsiip, string iLoIP, ushort debugPort, string debugKey, string newFriendlyName)
        {
            return hostStateManager.addNode(newIP, iScsiip, iLoIP, debugPort, debugKey, newFriendlyName);
        }

        private vmSpec _getVMByIP_withoutLocking(string VMIP)
        {
            return hostStateManager.getVMByIP_withoutLocking(VMIP);
        }

        private bladeSpec _getBladeByIP_withoutLocking(string bladeIP)
        {
            return hostStateManager.getBladeByIP_withoutLocking(bladeIP);
        }

        private string[] _getBladesByAllocatedServer(string serverIP)
        {
            return hostStateManager.getBladesByAllocatedServer(serverIP);
        }

        private vmSpec[] _getVMByVMServerIP_nolocking(string bladeIP)
        {
            return hostStateManager.getVMByVMServerIP_nolocking(bladeIP);
        }

        private string[] _getLogEvents()
        {
            return hostStateManager.getLogEvents().ToArray();
        }

        public vmServerCredentials _getCredentialsForVMServerByVMIP(string VMIP)
        {
            return hostStateManager._getCredentialsForVMServerByVMIP(VMIP);
        }

        public snapshotDetails _getCurrentSnapshotDetails(string VMIP)
        {
            return hostStateManager._getCurrentSnapshotDetails(VMIP);
        }

        public resultAndWaitToken _selectSnapshotForBladeOrVM(string requestorIP, string bladeName, string newShot)
        {
            return hostStateManager.selectSnapshotForBladeOrVM(requestorIP, bladeName, newShot);
        }
        
#endregion

        #region non-static
        public void keepAlive()
        {
            _keepAlive(getSrcIP());
        }

        public resultAndWaitToken logIn()
        {
            return _logIn(getSrcIP());
        }

        public void setResourceSharingModel(fairnessChecker.fairnessType fairnessType)
        {
            _setResourceSharingModel(fairnessType);
        }

        public string getWebSvcURL()
        {
            return _getWebSvcURL(getSrcIP());
        }

        public void setWebSvcURL(string newURL)
        {
            _setWebSvcURL(newURL);
        }

        public NASParams getNASParams()
        {
            return _getNASParams(getSrcIP());
        }

        public resultAndWaitToken getProgress(waitToken waitToken)
        {
            return _getProgress(waitToken);
        }

        public string[] getAllBladeIP()
        {
            return _getAllBladeIP();
        }

        public resultAndBladeName RequestAnySingleNode()
        {
            return _RequestAnySingleNode(getSrcIP());
        }

        public resultAndBladeName RequestSpecificNode(string nodeIP)
        {
            return _RequestSpecificNode(getSrcIP(), nodeIP);
        }

        public GetBladeStatusResult GetBladeStatus(string nodeIP)
        {
            return _GetBladeStatus(getSrcIP(), sanitizeAddress(nodeIP));
        }

        public GetBladeStatusResult GetVMStatus(string nodeIP)
        {
            return _GetVMStatus(getSrcIP(), nodeIP);
        }

        public resultAndWaitToken ReleaseBladeOrVM(string nodeIP)
        {
            return _ReleaseBlade(sanitizeAddress(nodeIP), getSrcIP(), false);
        }

        public bool isBladeMine(string nodeIP)
        {
            return _isBladeMine(sanitizeAddress(nodeIP), getSrcIP());
        }

        public resultAndWaitToken rebootAndStartDeployingBIOSToBlade(string BIOSXML, string requestorIP)
        {
            return _rebootAndStartDeployingBIOSToBlade(getSrcIP(), sanitizeAddress(requestorIP), BIOSXML);
        }

        public resultAndWaitToken rebootAndStartReadingBIOSConfiguration(string requestorIP)
        {
            return _rebootAndStartReadingBIOSConfiguration(getSrcIP(), sanitizeAddress(requestorIP));
        }
        
        public resultAndBladeName RequestAnySingleVM(VMHardwareSpec hwSpec, VMSoftwareSpec swSpec)
        {
            return _RequestAnySingleVM(getSrcIP(), hwSpec, swSpec);
        }

        public resultAndBladeName[] requestAsManyVMAsPossible(VMHardwareSpec hwSpec, VMSoftwareSpec swSpec)
        {
            return _requestAsManyVMAsPossible(getSrcIP(), hwSpec, swSpec);
        }

        public string generateIPXEScript()
        {
            return _generateIPXEScript(getSrcIP());
        }
        
        public TimeSpan getKeepAliveTimeout()
        {
            return _getKeepAliveTimeout();
        }

        public resultCode addNode(string nodeIP, string iSCSIIP, string iLoIP, ushort debugPort, string debugKey, string newFriendlyName)
        {
            return _addNode(nodeIP, iSCSIIP, iLoIP, debugPort, debugKey, newFriendlyName);
        }

        public vmSpec getVMByIP_withoutLocking(string VMIP)
        {
            return _getVMByIP_withoutLocking(VMIP);
        }

        public bladeSpec getBladeByIP_withoutLocking(string bladeIP)
        {
            return _getBladeByIP_withoutLocking(bladeIP);
        }

        public string[] getBladesByAllocatedServer(string serverIP)
        {
            return _getBladesByAllocatedServer(serverIP);
        }

        public vmSpec[] getVMByVMServerIP_nolocking(string bladeIP)
        {
            return _getVMByVMServerIP_nolocking(bladeIP);
        }

        public string[] getLogEvents()
        {
            return _getLogEvents();
        }

        public vmServerCredentials getCredentialsForVMServerByVMIP(string VMIP)
        {
            return _getCredentialsForVMServerByVMIP(VMIP);
        }

        public snapshotDetails getCurrentSnapshotDetails(string VMIP)
        {
            return _getCurrentSnapshotDetails(VMIP);
        }

        public resultAndWaitToken selectSnapshotForBladeOrVM(string bladeName, string newShot)
        {
            return _selectSnapshotForBladeOrVM(getSrcIP(), sanitizeAddress(bladeName), newShot);
        }

        public static string getSrcIP()
        {
            OperationContext ctx = OperationContext.Current;
            MessageProperties props = ctx.IncomingMessageProperties;
            RemoteEndpointMessageProperty ep = props[RemoteEndpointMessageProperty.Name] as RemoteEndpointMessageProperty;
            string IP = ep.Address;

            return sanitizeAddress(IP);
        }

        public static string sanitizeAddress(string toSanitize)
        {
            // The ipv6 loopback, ::1, gets used sometimes during VM provisioning. Because of that, we escape the colons into
            // something that can be present in clone/target/extent names.
            return toSanitize.Replace(":", "-");
        }
#endregion
    }
}