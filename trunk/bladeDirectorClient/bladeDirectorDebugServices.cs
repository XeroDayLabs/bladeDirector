using System;
using System.IO;
using System.ServiceModel;
using bladeDirectorClient.bladeDirectorService;

namespace bladeDirectorClient
{
    public class bladeDirectorDebugServices : BladeDirectorServices, IDisposable
    {
        public readonly DebugServicesClient svcDebug;

        public string servicesDebugURL { get; private set; }

        private static int _portNum = 90;

        public bladeDirectorDebugServices(string executablePath)
            : base(executablePath, (_portNum++))
        {
            servicesDebugURL = baseURL + "/bladeDirectorDebug";

            WSHttpBinding debugBinding = new WSHttpBinding
            {
                MaxReceivedMessageSize = Int32.MaxValue,
                ReaderQuotas = { MaxStringContentLength = Int32.MaxValue }
            };
            svcDebug = new DebugServicesClient(debugBinding, new EndpointAddress(servicesDebugURL));
        }

        public bladeDirectorDebugServices(string debugURL, string serviceURL)
            : base(serviceURL)
        {
            servicesDebugURL = debugURL;

            WSHttpBinding debugBinding = new WSHttpBinding
            {
                MaxReceivedMessageSize = Int32.MaxValue,
                ReaderQuotas = { MaxStringContentLength = Int32.MaxValue }
            };
            svcDebug = new DebugServicesClient(debugBinding, new EndpointAddress(servicesDebugURL));
        }
        public bladeDirectorDebugServices(string executablePath, string[] IPAddresses, bool isMocked = true)
            : this(executablePath)
        {
            svcDebug.initWithBladesFromIPList(IPAddresses, isMocked, NASFaultInjectionPolicy.retunSuccessful);
        }

        public bladeDirectorDebugServices(string executablePath, bladeSpec[] bladeSpecs, bool isMocked = true)
            : this(executablePath)
        {
            svcDebug.initWithBladesFromBladeSpec(bladeSpecs, isMocked, NASFaultInjectionPolicy.retunSuccessful);
        }

        public bladeDirectorDebugServices(string executablePath, string ipAddress, bool isMocked = true)
            : this(executablePath, new string[] { ipAddress }, isMocked)
        {
        }

        public override void Dispose()
        {
            // FIXME: why this cast?
            try { ((IDisposable)svcDebug).Dispose(); }
            catch (CommunicationException) { }

            base.Dispose();
        }
    }
}