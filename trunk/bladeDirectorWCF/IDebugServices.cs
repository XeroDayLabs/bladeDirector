using System.ServiceModel;
using System.Text;
using hypervisors;

namespace bladeDirectorWCF
{
    [ServiceContract]
    public interface IDebugServices
    {
        [OperationContract]
        void initWithBladesFromIPList(string[] bladeIPs, bool useMockedManager, NASFaultInjectionPolicy faultInjection);

        [OperationContract]
        void initWithBladesFromBladeSpec(bladeSpec[] spec, bool useMockedManager, NASFaultInjectionPolicy faultInjection);

        [OperationContract]
        bladeSpec createBladeSpec(string newBladeIP, string newISCSIIP, string newILOIP, ushort newILOPort, 
            bool newCurrentlyHavingBIOSDeployed, VMDeployStatus newvmDeployState, string newCurrentBIOS, 
            bladeLockType permittedAccessRead, bladeLockType permittedAccessWrite);

        [OperationContract]
        resultAndWaitToken _logIn(string requestorIP);
        
        [OperationContract]
        mockedCall[] _getNASEventsIfMocked();
        
        [OperationContract]
        void _setExecutionResultsIfMocked(mockedExecutionResponses respType);
        
        [OperationContract]
        void _setBIOSOperationTimeIfMocked(int operationTimeSeconds);
        
        [OperationContract]
        resultAndWaitToken _rebootAndStartReadingBIOSConfiguration(string requestorIP, string nodeIP);
        
        [OperationContract]
        resultAndWaitToken _rebootAndStartDeployingBIOSToBlade(string requestorIP, string nodeIP, string BIOSXML);

        [OperationContract]
        resultAndBladeName _RequestAnySingleNode(string requestorIP);
        
        [OperationContract]
        GetBladeStatusResult _GetBladeStatus(string requestorIP, string nodeIP);
        
        [OperationContract]
        bool _isBladeMine(string requestorIP, string clientIP);
        
        [OperationContract]
        resultAndWaitToken _ReleaseBladeOrVM(string requestorIP, string nodeIP, bool force = false);
        
        [OperationContract]
        void _keepAlive(string requestorIP);
        
        [OperationContract]
        resultAndBladeName _requestAnySingleVM(string requestorIP, VMHardwareSpec hwSpec, VMSoftwareSpec swSpec);

        [OperationContract]
        void setKeepAliveTimeout(int newTimeoutSeconds);
    }
}