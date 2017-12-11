using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.ServiceModel;
using System.ServiceModel.Channels;
using System.ServiceModel.Configuration;
using System.ServiceModel.Description;
using System.ServiceModel.Dispatcher;
using System.ServiceModel.Web;
using System.Text;
using System.Threading;
using bladeDirectorClient;
using CommandLine;
using CommandLine.Text;

namespace bladeDirectorWCF
{
    public class foo : IErrorHandler
    {
        public void ProvideFault(Exception error, MessageVersion version, ref Message fault)
        {
        }

        public bool HandleError(Exception error)
        {
            return true;
        }
    }

    public static class Program
    {
        public static void Main(string[] args)
        {
            Parser parser = new CommandLine.Parser();
            bladeDirectorArgs parsedArgs = new bladeDirectorArgs();
            if (parser.ParseArgumentsStrict(args, parsedArgs, () => { Console.Write(HelpText.AutoBuild(parsedArgs).ToString()); }))
                _Main(parsedArgs);
        }

        public static void _Main(bladeDirectorArgs parsedArgs)
        {
            AppDomain.CurrentDomain.FirstChanceException += (sender, args) =>
            {
                try
                {
                    if (services.hostStateManager != null)
                    {
                        // We don't care about this kind of exception. It happens very frequently during normal use of the WMI
                        // executor.
                        if (!(args.Exception is COMException) &&
                            !(args.Exception is AggregateException && ((AggregateException)args.Exception).InnerExceptions.All(x => x is COMException)) )
                        {
                            services.hostStateManager.addLogEvent("First-chance exception: " + args.Exception.ToString());
                        }
                    }

                    string dumpDir = Properties.Settings.Default.internalErrorDumpPath.Trim();
                    if (dumpDir != "")
                    {
                        // If this is a 'System.ServiceModel.FaultException', then it is destined to get to the caller via WCF. This is pretty bad, so we dump on these
                        // to aid debugging.
                        // We just use text matching here since perf shouldn't be critical (not many exceptions should happen).
                        if (args.Exception.GetType().ToString().StartsWith("System.ServiceModel.FaultException"))
                        {
                            miniDumpUtils.dumpSelf(Path.Combine(dumpDir, "FaultException_" + Guid.NewGuid().ToString() + ".dmp"));
                        }
                        // Lock violations are a pain to debug, even with all our debug output, so also drop a dump for those.
                        else if (args.Exception is ApplicationException || args.Exception is bladeLockExeception)
                        {
                            miniDumpUtils.dumpSelf(Path.Combine(dumpDir, "lockViolation_" + Guid.NewGuid().ToString() + ".dmp"));
                        }
                    }
                }
                catch (Exception)
                {
                    // ...
                }
            };

            if (!parsedArgs.baseURL.EndsWith("/"))
                parsedArgs.baseURL = parsedArgs.baseURL + "/";

            Uri baseServiceURL = new Uri(new Uri(parsedArgs.baseURL), "bladeDirector");
            Uri debugServiceURL = new Uri(new Uri(parsedArgs.baseURL), "bladeDirectorDebug");
            Uri webServiceURL = new Uri(new Uri(parsedArgs.webURL), "");

            using (WebServiceHost WebSvc = new WebServiceHost(typeof (webServices), baseServiceURL))
            {
                WebSvc.AddServiceEndpoint(typeof(IWebServices), new WebHttpBinding(), webServiceURL);
                if (!parsedArgs.disableWebPort)
                    WebSvc.Open();

                using (ServiceHost baseSvc = new ServiceHost(typeof (services), baseServiceURL))
                {
                    configureService(baseSvc, typeof(IServices));
                    baseSvc.Open();

                    using (ServiceHost debugSvc = new ServiceHost(typeof (debugServices), debugServiceURL))
                    {
                        configureService(debugSvc, typeof(IDebugServices));
                        debugSvc.Open();

                        if (!parsedArgs.disableWebPort)
                        {
                            using (bladeDirectorDebugServices conn = new bladeDirectorDebugServices(debugServiceURL.ToString(), baseServiceURL.ToString()))
                            {
                                conn.svc.setWebSvcURL(webServiceURL.ToString());
                            }
                        }

                        if (parsedArgs.stopEvent != null)
                        {
                            parsedArgs.stopEvent.WaitOne();
                        }
                        else
                        {
                            int[] bladeIDs = getBladeIDsFromString(parsedArgs.bladeList);
                            if (bladeIDs.Length > 0)
                            { 
                                Console.WriteLine("Adding blades...");
                                using (bladeDirectorDebugServices conn = new bladeDirectorDebugServices(debugServiceURL.ToString(), baseServiceURL.ToString()))
                                {
                                    foreach (int id in bladeIDs)
                                    {
                                        string bladeName = xdlClusterNaming.makeBladeFriendlyName(id);
                                        string bladeIP = xdlClusterNaming.makeBladeIP(id);
                                        string iloIP = xdlClusterNaming.makeBladeILOIP(id);
                                        string iSCSIIP = xdlClusterNaming.makeBladeISCSIIP(id);
                                        ushort debugPort = xdlClusterNaming.makeBladeKernelDebugPort(id);
                                        string debugKey = "some.test.key.here";
                                        Console.WriteLine("Creating node {0}: IP {1}, ilo {2}, iSCSI IP {3}, debug port {4}, debug key {5}", id, bladeIP, iloIP, iSCSIIP, debugPort, debugKey);
                                        conn.svc.addNode(bladeIP, iSCSIIP, iloIP, debugPort, debugKey, bladeName);
                                    }
                                }
                            }
                            Console.WriteLine("BladeDirector ready.");
                            Console.WriteLine("Listening at main endpoint:  " + baseServiceURL.ToString());
                            if (!parsedArgs.disableWebPort)
                                Console.WriteLine("Listening at web endpoint:   " + webServiceURL.ToString());
                            Console.WriteLine("Listening at debug endpoint: " + debugServiceURL.ToString());
                            Console.WriteLine("Hit [enter] to exit.");
                            Console.ReadLine();
                        }
                    }
                }
            }
        }

        private static int[] getBladeIDsFromString(string spec)
        {
            // We accept comma-delimited or ranges here, for example:
            // 1,2,4-6,9
            List<int> toRet = new List<int>();

            string[] bladeParts = spec.Split(',');
            for (int n = 0; n < bladeParts.Length; n++)
            {
                string bladePart = bladeParts[n];

                int start;
                int end;
                if (bladePart.Contains('-'))
                {
                    start = Int32.Parse(bladePart.Split('-')[0]);
                    end = Int32.Parse(bladePart.Split('-')[1]);
                    toRet.AddRange(Enumerable.Range(start, (end-start) + 1));
                }
                else
                {
                    toRet.Add(Int32.Parse(bladePart));
                }
            }

            return toRet.ToArray();
        }

        private static void configureService(ServiceHost svc, Type contractInterfaceType)
        {
            ServiceMetadataBehavior metaBehav = svc.Description.Behaviors.Find<ServiceMetadataBehavior>();
            if (metaBehav == null) 
                metaBehav = new ServiceMetadataBehavior();
            metaBehav.HttpGetEnabled = true;
            metaBehav.MetadataExporter.PolicyVersion = PolicyVersion.Policy15;
            svc.Description.Behaviors.Add(metaBehav);
            svc.AddServiceEndpoint(ServiceMetadataBehavior.MexContractName, MetadataExchangeBindings.CreateMexHttpBinding(), "mex");

            WSHttpBinding baseBinding = new WSHttpBinding
            {
                MaxReceivedMessageSize = Int32.MaxValue,
                ReaderQuotas = { MaxStringContentLength = Int32.MaxValue },
                OpenTimeout = TimeSpan.FromSeconds(55),
                CloseTimeout = TimeSpan.FromSeconds(55),
                SendTimeout = TimeSpan.FromSeconds(55),
                //ReceiveTimeout = TimeSpan.FromSeconds(55),
                ReceiveTimeout = TimeSpan.MaxValue,
                ReliableSession = new OptionalReliableSession()
                {
                    InactivityTimeout = TimeSpan.MaxValue,
                    Enabled = true,
                    Ordered = true
                },
                Security = new WSHttpSecurity() { Mode = SecurityMode.None }
            };
            svc.AddServiceEndpoint(contractInterfaceType, baseBinding, "");
            
            ServiceBehaviorAttribute svcBehav = svc.Description.Behaviors.Find<ServiceBehaviorAttribute>();
            svcBehav.IncludeExceptionDetailInFaults = true;

            foreach (ChannelDispatcherBase channelDispatcherBase in svc.ChannelDispatchers)
            {
                ChannelDispatcher a = channelDispatcherBase as ChannelDispatcher;
                if (a == null)
                    continue;
                a.ErrorHandlers.Add(new foo());
            }
        }
    }

    public class bladeDirectorArgs
    {
        // ReSharper disable UnusedAutoPropertyAccessor.Global

        [Option('b', "baseURL", Required = false, DefaultValue = "http://127.0.0.1", HelpText = "Base URL to listen for services on")]
        public string baseURL { get; set; }

        [Option('w', "webURL", Required = false, DefaultValue = "http://0.0.0.0:81/", HelpText = "URL to provide HTTP services to IPXE on")]
        public string webURL { get; set; }

        [Option('l', "bladeList", Required = false, DefaultValue = "", HelpText = "A list of comma-seperated blade IDs in the XDL cluster to use. You can also use a hyphen to denote a range (eg, '1,2,3,10-20')")]
        public string bladeList { get; set; }

        [Option("no-web", Required = false, DefaultValue = false, HelpText = "Do not listen on port 81 (PXE boot will not function)")]
        public bool disableWebPort { get; set; }

        /// <summary>
        /// Set this to a new ManualResetEvent if you'd like service exit to be controlled remotely.
        /// </summary>
        public ManualResetEvent stopEvent = null;

        // ReSharper restore UnusedAutoPropertyAccessor.Global
    }
}