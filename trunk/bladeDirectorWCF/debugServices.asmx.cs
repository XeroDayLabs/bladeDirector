using System;
using System.Web.Services;
using hypervisors;

namespace bladeDirectorWCF
{
    [WebService(Namespace = "http://tempuri.org/")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]
    // To allow this Web Service to be called from script, using ASP.NET AJAX, uncomment the following line. 
    // [System.Web.Script.Services.ScriptService]
    public class debugServices : System.Web.Services.WebService, IDebugServices
    {
        [WebMethod]
        public void initWithBladesFromIPList(string[] bladeIPs, bool useMockedManager)
        {
            if (useMockedManager)
                services.hostStateManager = new hostStateManagerMocked();
            else
                services.hostStateManager = new hostStateManager();

            services.hostStateManager.initWithBlades(bladeIPs);
        }

        [WebMethod]
        public void initWithBladesFromBladeSpec(bladeSpec[] spec, bool useMockedManager)
        {
            if (useMockedManager)
                services.hostStateManager = new hostStateManagerMocked();
            else
                services.hostStateManager = new hostStateManager();

            services.hostStateManager.initWithBlades(spec);
        }

        // Reusing existing types isn't supported for non-WCF services, so we need to do this silly workaround until we scrap IIS
        // and run entirely from WCF.
        [WebMethod]
        public bladeSpec createBladeSpec(string newBladeIP, string newISCSIIP, string newILOIP, ushort newILOPort, 
            bool newCurrentlyHavingBIOSDeployed, VMDeployStatus newVMDeployState, string newCurrentBIOS, 
            bladeLockType permittedAccess)
        {
            return new bladeSpec(newBladeIP, newISCSIIP, newILOIP, newILOPort, newCurrentlyHavingBIOSDeployed,
                newVMDeployState, newCurrentBIOS, permittedAccess);
        }

        [WebMethod]
        public string _logIn(string requestorIP)
        {
            return services.logIn(requestorIP);
        }

        [WebMethod]
        public mockedCall[] _getNASEventsIfMocked()
        {
            return ((hostStateManagerMocked)services.hostStateManager).getNASEvents().ToArray();
        }

        [WebMethod]
        public void _setExecutionResultsIfMocked(mockedExecutionResponses respType)
        {
            ((hostStateManagerMocked)services.hostStateManager).setExecutionResults(respType);
        }

        [WebMethod]
        public void _setBIOSOperationTimeIfMocked(int operationTimeSeconds)
        {
            ((biosReadWrite_mocked)services.hostStateManager.biosRWEngine).biosOperationTime = TimeSpan.FromSeconds(operationTimeSeconds);
        }

        [WebMethod]
        public resultCode _rebootAndStartReadingBIOSConfiguration(string requestorIP, string nodeIP)
        {
            return services.rebootAndStartReadingBIOSConfiguration(nodeIP, requestorIP);
        }

        [WebMethod]
        public resultCode _rebootAndStartDeployingBIOSToBlade(string requestorIP, string nodeIP, string BIOSXML)
        {
            return services.rebootAndStartDeployingBIOSToBlade(nodeIP, BIOSXML, requestorIP);
        }

        [WebMethod]
        public resultCodeAndBIOSConfig _checkBIOSOperationProgress(string requestorIP, string nodeIP)
        {
            return services.checkBIOSOperationProgress(requestorIP, nodeIP);
        }

        [WebMethod]
        public resultCodeAndBladeName _RequestAnySingleNode(string requestorIP)
        {
            return services.RequestAnySingleNode(requestorIP);
        }

        [WebMethod]
        public GetBladeStatusResult _GetBladeStatus(string requestorIP, string nodeIP)
        {
            return services.GetBladeStatus(requestorIP, nodeIP);
        }

        [WebMethod]
        public bool _isBladeMine(string requestorIP, string clientIP)
        {
            return services.isBladeMine(clientIP, requestorIP);
        }

        [WebMethod]
        public resultCode _ReleaseBlade(string requestorIP, string nodeIP, bool force = false)
        {
            return services.ReleaseBlade(nodeIP, requestorIP, force);
        }

        [WebMethod]
        public void _keepAlive(string requestorIP)
        {
            services.keepAlive(requestorIP);
        }

        [WebMethod]
        public resultAndBladeName _requestAnySingleVM(string requestorIP, VMHardwareSpec hwSpec, VMSoftwareSpec swSpec)
        {
            return services.RequestAnySingleVM(requestorIP, hwSpec, swSpec);
        }

        [WebMethod]
        public void setKeepAliveTimeout(int newTimeoutSeconds)
        {
            services.hostStateManager.setKeepAliveTimeout(TimeSpan.FromSeconds(newTimeoutSeconds));
        }

    }
}
