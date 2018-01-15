using System;
using System.ServiceModel;
using hypervisors;

namespace bladeDirectorWCF
{
    public class debugServices : IDebugServices
    {
        public string ping()
        {
            return "pong";
        }

        public void lockAndSleep(string bladeToLock)
        {
            services._lockAndSleep(bladeToLock);
        }

        public void lockAndNeverRelease(string bladeToLock)
        {
            services._lockAndNeverRelease(bladeToLock);
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

        public bladeSpec createBladeSpec(string newBladeIP, string newISCSIIP, string newILOIP, ushort newKernelDebugPort,
            bool newCurrentlyHavingBIOSDeployed, VMDeployStatus newvmDeployState, string newCurrentBIOS, string newDebugKey,
            string newFriendlyName,
            bladeLockType permittedAccessRead, bladeLockType permittedAccessWrite)
        {
            return new bladeSpec(null, newBladeIP, newISCSIIP, newILOIP, newKernelDebugPort, newCurrentlyHavingBIOSDeployed,
                newvmDeployState, newCurrentBIOS, newDebugKey, newFriendlyName, permittedAccessRead, permittedAccessWrite);
        }

        public bladeSpec createBladeSpecForXDLNode(int nodeIndex, string newDebugKey, bladeLockType permittedAccessRead, bladeLockType permittedAccessWrite)
        {
            string newBladeIP = xdlClusterNaming.makeBladeIP(nodeIndex);
            string newISCSIIP = xdlClusterNaming.makeBladeISCSIIP(nodeIndex);
            string newILOIP = xdlClusterNaming.makeBladeILOIP(nodeIndex);
            ushort newKernelDebugPort = xdlClusterNaming.makeBladeKernelDebugPort(nodeIndex);
            string newFriendlyName = xdlClusterNaming.makeBladeFriendlyName(nodeIndex);

            return new bladeSpec(null, newBladeIP, newISCSIIP, newILOIP, newKernelDebugPort, false, VMDeployStatus.notBeingDeployed,
                null, newDebugKey, newFriendlyName, permittedAccessRead, permittedAccessWrite);
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

        public resultAndWaitToken _selectSnapshotForBladeOrVM(string requestorIP, string nodeIP, string snapshotName)
        {
            return services.hostStateManager.selectSnapshotForBladeOrVM(requestorIP, nodeIP, snapshotName);
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