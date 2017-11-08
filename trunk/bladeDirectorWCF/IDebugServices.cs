using System.ServiceModel;
using System.Text;
using hypervisors;

namespace bladeDirectorWCF
{
    [ServiceContract]
    public interface IDebugServices
    {
        [OperationContract]
        string ping();

        [OperationContract]
        void initWithBladesFromIPList(string[] bladeIPs, bool useMockedManager, NASFaultInjectionPolicy faultInjection);

        [OperationContract]
        void initWithBladesFromBladeSpec(bladeSpec[] spec, bool useMockedManager, NASFaultInjectionPolicy faultInjection);

        [OperationContract]
        bladeSpec createBladeSpec(string newBladeIP, string newISCSIIP, string newILOIP, ushort newILOPort, 
            bool newCurrentlyHavingBIOSDeployed, VMDeployStatus newvmDeployState, string newCurrentBIOS, string newDebugKey,
            string newFriendlyName,
            bladeLockType permittedAccessRead, bladeLockType permittedAccessWrite);

        [OperationContract]
        bladeSpec createBladeSpecForXDLNode(int nodeIndex, string newDebugKey, bladeLockType permittedAccessRead, bladeLockType permittedAccessWrite);

        [OperationContract]
        resultAndWaitToken _logIn(string requestorIP);
        
        [OperationContract]
        mockedCall[] _getNASEventsIfMocked();
        
        [OperationContract]
        void _setExecutionResultsIfMocked(mockedExecutionResponses respType);
        
        [OperationContract]
        void _setBIOSOperationTimeIfMocked(int operationTimeSeconds);

        [OperationContract]
        resultAndWaitToken _selectSnapshotForBladeOrVM(string requestorIP, string nodeIP, string snapshotName);

        [OperationContract]
        resultAndWaitToken _rebootAndStartReadingBIOSConfiguration(string requestorIP, string nodeIP);
        
        [OperationContract]
        resultAndWaitToken _rebootAndStartDeployingBIOSToBlade(string requestorIP, string nodeIP, string BIOSXML);

        [OperationContract]
        resultAndBladeName _RequestAnySingleNode(string requestorIP);

        [OperationContract]
        resultAndBladeName _RequestSpecificNode(string requestorIP, string nodeIP);
        
        [OperationContract]
        GetBladeStatusResult _GetBladeStatus(string requestorIP, string nodeIP);

        [OperationContract]
        GetBladeStatusResult _GetVMStatus(string requestorIP, string VMIP);

        [OperationContract]
        bool _isBladeMine(string requestorIP, string clientIP, bool ignoreDeployments = false);
        
        [OperationContract]
        resultAndWaitToken _ReleaseBladeOrVM(string requestorIP, string nodeIP, bool force = false);
        
        [OperationContract]
        void _keepAlive(string requestorIP);
        
        [OperationContract]
        resultAndBladeName _requestAnySingleVM(string requestorIP, VMHardwareSpec hwSpec, VMSoftwareSpec swSpec);

        [OperationContract]
        resultAndBladeName[] _requestAsManyVMAsPossible(string requestorIP, VMHardwareSpec hwSpec, VMSoftwareSpec swSpec);
        
        [OperationContract]
        void setKeepAliveTimeout(int newTimeoutSeconds);
    }
}