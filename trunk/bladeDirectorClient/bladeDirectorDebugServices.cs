using System;
using System.IO;
using System.ServiceModel;
using bladeDirectorClient.bladeDirectorService;

namespace bladeDirectorClient
{
    public class bladeDirectorDebugServices : BladeDirectorServices, IDisposable
    {
        public DebugServicesClient svcDebug;

        public string servicesDebugURL { get; private set; }

        private static ushort _portNum = 90;

        public bladeDirectorDebugServices(string executablePath, bool withWeb)
            : base(executablePath, (_portNum++), withWeb)
        {
            servicesDebugURL = baseURL + "/bladeDirectorDebug";

            WSHttpBinding debugBinding = new WSHttpBinding
            {
                MaxReceivedMessageSize = Int32.MaxValue,
                ReaderQuotas = { MaxStringContentLength = Int32.MaxValue }
            };
            waitUntilReady(() =>
            {
                svcDebug = new DebugServicesClient(debugBinding, new EndpointAddress(servicesDebugURL));
                svcDebug.ping();
            });
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
            waitUntilReady(() =>
            {
                svcDebug = new DebugServicesClient(debugBinding, new EndpointAddress(servicesDebugURL));
                svcDebug.ping();
            });
        }
        public bladeDirectorDebugServices(string executablePath, string[] IPAddresses, bool isMocked = true, bool withWeb = false)
            : this(executablePath, withWeb)
        {
            svcDebug.initWithBladesFromIPList(IPAddresses, isMocked, NASFaultInjectionPolicy.retunSuccessful);
        }
        public bladeDirectorDebugServices(string executablePath, string ipAddress, bool isMocked, bool withWeb )
            : this(executablePath, new string[] { ipAddress }, isMocked, withWeb)
        {
        }

        public override void Dispose()
        {
            // FIXME: why this cast?
            try { ((IDisposable)svcDebug).Dispose(); }
            catch (CommunicationException) { }
            catch (TimeoutException) { }

            base.Dispose();
        }
    }
}