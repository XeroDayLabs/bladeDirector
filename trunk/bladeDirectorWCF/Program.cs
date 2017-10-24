﻿using System;
using System.Diagnostics;
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
using Binding = System.Web.Services.Description.Binding;

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
                        services.hostStateManager.addLogEvent("First-chance exception: " + args.Exception.ToString());
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
                WebSvc.Open();

                using (ServiceHost baseSvc = new ServiceHost(typeof (services), baseServiceURL))
                {
                    configureService(baseSvc, typeof(IServices));
                    baseSvc.Open();

                    using (ServiceHost debugSvc = new ServiceHost(typeof (debugServices), debugServiceURL))
                    {
                        configureService(debugSvc, typeof(IDebugServices));
                        debugSvc.Open();

                        if (parsedArgs.stopEvent != null)
                        {
                            parsedArgs.stopEvent.WaitOne();
                        }
                        else
                        {
                            Console.WriteLine("Adding blades...");
                            using (bladeDirectorDebugServices conn = new bladeDirectorDebugServices(debugServiceURL.ToString(), baseServiceURL.ToString()))
                            {
                                string[] bladeIDs = parsedArgs.bladeList.Split(',');
                                foreach (string idStr in bladeIDs)
                                {
                                    int id = Int32.Parse(idStr);
                                    string bladeIP = "172.17.129." + (100 + id);
                                    string iloIP = "172.17.2." + (100 + id);
                                    string iSCSIIP = "10.0.0." + (100 + id);
                                    ushort debugPort = (ushort) (60000 + id);
                                    Console.WriteLine("Creating node {0}: IP {1}, ilo {2}, iSCSI IP {3}, debug port {4}", id, bladeIP, iloIP, iSCSIIP, debugPort);
                                    conn.svc.addNode(bladeIP, iSCSIIP, iloIP, debugPort);
                                }
                            }
                            Console.WriteLine("BladeDirector ready.");
                            Console.WriteLine("Listening at main endpoint:  " + baseServiceURL.ToString());
                            Console.WriteLine("Listening at web endpoint:   " + webServiceURL.ToString());
                            Console.WriteLine("Listening at debug endpoint: " + debugServiceURL.ToString());
                            Console.WriteLine("Hit [enter] to exit.");
                            Console.ReadLine();
                        }
                    }
                }
            }
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
                ReaderQuotas = { MaxStringContentLength = Int32.MaxValue }
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

        [Option('l', "bladeList", Required = false, DefaultValue = "28,29,30,31", HelpText = "A list of comma-seperated blade IDs in the XDL cluster to use (eg, '1,2,3,7,9')")]
        public string bladeList { get; set; }
        
        /// <summary>
        /// Set this to a new ManualResetEvent if you'd like service exit to be controlled remotely.
        /// </summary>
        public ManualResetEvent stopEvent = null;

        // ReSharper restore UnusedAutoPropertyAccessor.Global
    }
}