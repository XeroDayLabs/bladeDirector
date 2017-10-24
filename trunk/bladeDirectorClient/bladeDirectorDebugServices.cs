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

        //private static readonly string WCFPath = Path.Combine(Properties.Settings.Default.repoRoot, "trunk\\bladeDirectorWCF\\bin\\x64\\Debug\\bladeDirectorWCF.exe");

        // TODO: init with path to bladeDirectorWCF.exe
        public bladeDirectorDebugServices()
            : base("idk", (_portNum++))
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
        public bladeDirectorDebugServices(string[] IPAddresses, bool isMocked = true)
            : this()
        {
            svcDebug.initWithBladesFromIPList(IPAddresses, isMocked, NASFaultInjectionPolicy.retunSuccessful);
        }

        public bladeDirectorDebugServices(bladeSpec[] bladeSpecs, bool isMocked = true)
            : this()
        {
            svcDebug.initWithBladesFromBladeSpec(bladeSpecs, isMocked, NASFaultInjectionPolicy.retunSuccessful);
        }

        public bladeDirectorDebugServices(string ipAddress, bool isMocked = true)
            : this(new string[] { ipAddress }, isMocked)
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