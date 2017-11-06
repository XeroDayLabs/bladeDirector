using System;
using hypervisors;

namespace bladeDirectorWCF
{
    public class debugServices : IDebugServices
    {
        public string ping()
        {
            return "pong";
        }

        public void initWithBladesFromIPList(string[] bladeIPs, bool useMockedManager, NASFaultInjectionPolicy faultInjection)
        {
            if (useMockedManager)
            {
                hostStateManagerMocked newMgr = new hostStateManagerMocked();
                newMgr.setMockedNASFaultInjectionPolicy(faultInjection);
                services.hostStateManager = newMgr;
            }
            else
            {
                services.hostStateManager = new hostStateManager();
            }

            services.hostStateManager.initWithBlades(bladeIPs);
        }

        
        public void initWithBladesFromBladeSpec(bladeSpec[] spec, bool useMockedManager, NASFaultInjectionPolicy faultInjection)
        {
            if (useMockedManager)
            {
                hostStateManagerMocked newMgr = new hostStateManagerMocked();
                newMgr.setMockedNASFaultInjectionPolicy(faultInjection);
                services.hostStateManager = newMgr;
            }
            else
            {
                services.hostStateManager = new hostStateManager();
            }


            services.hostStateManager.initWithBlades(spec);
        }

        // Reusing existing types isn't supported for non-WCF services, so we need to do this silly workaround until we scrap IIS
        // and run entirely from WCF.
        public bladeSpec createBladeSpec(string newBladeIP, string newISCSIIP, string newILOIP, ushort newILOPort,
            bool newCurrentlyHavingBIOSDeployed, VMDeployStatus newvmDeployState, string newCurrentBIOS,
            bladeLockType permittedAccessRead, bladeLockType permittedAccessWrite)
        {
            return new bladeSpec(null, newBladeIP, newISCSIIP, newILOIP, newILOPort, newCurrentlyHavingBIOSDeployed,
                newvmDeployState, newCurrentBIOS, permittedAccessRead, permittedAccessWrite);
        }
        
        public resultAndWaitToken _logIn(string requestorIP)
        {
            return services._logIn(requestorIP);
        }
        
        public mockedCall[] _getNASEventsIfMocked()
        {
            return ((hostStateManagerMocked)services.hostStateManager).getNASEvents().ToArray();
        }

        public void _setExecutionResultsIfMocked(mockedExecutionResponses respType)
        {
            ((hostStateManagerMocked)services.hostStateManager).setExecutionResults(respType);
        }
        
        public void _setBIOSOperationTimeIfMocked(int operationTimeSeconds)
        {
            ((biosReadWrite_mocked)services.hostStateManager.biosRWEngine).biosOperationTime = TimeSpan.FromSeconds(operationTimeSeconds);
        }
        
        public resultAndWaitToken _rebootAndStartReadingBIOSConfiguration(string requestorIP, string nodeIP)
        {
            return services.hostStateManager.rebootAndStartReadingBIOSConfiguration(nodeIP, requestorIP);
        }

        public resultAndWaitToken _rebootAndStartDeployingBIOSToBlade(string requestorIP, string nodeIP, string BIOSXML)
        {
            return services.hostStateManager.rebootAndStartDeployingBIOSToBlade(requestorIP, nodeIP, BIOSXML);
        }
        
        public resultAndBladeName _RequestAnySingleNode(string requestorIP)
        {
            return services.hostStateManager.RequestAnySingleNode(requestorIP);
        }

        public resultAndBladeName _RequestSpecificNode(string requestorIP, string nodeIP)
        {
            return services.hostStateManager.RequestSpecificNode(requestorIP, nodeIP);
        }

        public GetBladeStatusResult _GetBladeStatus(string requestorIP, string nodeIP)
        {
            return services.hostStateManager.getBladeStatus(requestorIP, nodeIP);
        }

        public GetBladeStatusResult _GetVMStatus(string requestorIP, string VMIP)
        {
            return services.hostStateManager.getVMStatus(requestorIP, VMIP);
        }

        public bool _isBladeMine(string requestorIP, string clientIP, bool ignoreDeployments)
        {
            return services.hostStateManager.isBladeMine(clientIP, requestorIP, ignoreDeployments);
        }

        public resultAndWaitToken _ReleaseBladeOrVM(string requestorIP, string nodeIP, bool force = false)
        {
            return services.hostStateManager.releaseBladeOrVM(nodeIP, requestorIP, force);
        }

        public void _keepAlive(string requestorIP)
        {
            services.hostStateManager.keepAlive(requestorIP);
        }

        public resultAndBladeName _requestAnySingleVM(string requestorIP, VMHardwareSpec hwSpec, VMSoftwareSpec swSpec)
        {
            return services.hostStateManager.RequestAnySingleVM(requestorIP, hwSpec, swSpec);
        }

        public resultAndBladeName[] _requestAsManyVMAsPossible(string requestorIP, VMHardwareSpec hwSpec, VMSoftwareSpec swSpec)
        {
            return services.hostStateManager.requestAsManyVMAsPossible(requestorIP, hwSpec, swSpec);
        }

        public void setKeepAliveTimeout(int newTimeoutSeconds)
        {
            services.hostStateManager.setKeepAliveTimeout(TimeSpan.FromSeconds(newTimeoutSeconds));
        }
    }
}