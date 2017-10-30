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

        private readonly WSHttpBinding debugBinding = createBinding();

        private static ushort _portNum = 90;

        public bladeDirectorDebugServices(string executablePath, bool withWeb)
            : base(executablePath, (_portNum++), withWeb)
        {
            servicesDebugURL = baseURL + "/bladeDirectorDebug";

            connect();
        }

        public bladeDirectorDebugServices(string debugURL, string serviceURL)
            : base(serviceURL)
        {
            servicesDebugURL = debugURL;

            connect();
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

        private void connect()
        {
            waitUntilReady(() =>
            {
                if (svcDebug != null)
                {
                    try { ((IDisposable)svcDebug).Dispose(); }
                    catch (CommunicationException) { }
                    catch (TimeoutException) { }
                }

                svcDebug = new DebugServicesClient(debugBinding, new EndpointAddress(servicesDebugURL));
                svcDebug.ping();
            });
        }

        private static WSHttpBinding createBinding()
        {
            return new WSHttpBinding
            {
                MaxReceivedMessageSize = Int32.MaxValue,
                ReaderQuotas = { MaxStringContentLength = Int32.MaxValue },
                ReceiveTimeout = TimeSpan.MaxValue,
                ReliableSession = new OptionalReliableSession()
                {
                    InactivityTimeout = TimeSpan.MaxValue,
                    Enabled = true,
                    Ordered = true
                }
                Security = new WSHttpSecurity() { Mode = SecurityMode.None }
            };
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