using System;
using System.Diagnostics;
using System.ServiceModel;
using System.ServiceModel.Channels;
using System.ServiceModel.Configuration;
using System.ServiceModel.Description;
using System.ServiceModel.Dispatcher;
using System.ServiceModel.Web;
using System.Text;
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

        private static void _Main(bladeDirectorArgs parsedArgs)
        {
            AppDomain.CurrentDomain.FirstChanceException += (sender, args) =>
            {
                services.hostStateManager.addLogEvent("First-chance exception: " + args.Exception.ToString());
            };

            AppDomain.CurrentDomain.UnhandledException += (sender, args) =>
            {
                services.hostStateManager.addLogEvent("Fatal exception: " + ((Exception)args.ExceptionObject).ToString());
            };

            if (!parsedArgs.baseURL.EndsWith("/"))
                parsedArgs.baseURL = parsedArgs.baseURL + "/";

            Uri baseServiceURL = new Uri(new Uri(parsedArgs.baseURL), "bladeDirector");
            Uri debugServiceURL = new Uri(new Uri(parsedArgs.baseURL), "bladeDirectorDebug");

            using (WebServiceHost WebSvc = new WebServiceHost(typeof (webServices), baseServiceURL))
            {
                WebSvc.AddServiceEndpoint(typeof(IWebServices), new WebHttpBinding(), new Uri("http://0.0.0.0:81/foo"));
                WebSvc.Open();

                using (ServiceHost baseSvc = new ServiceHost(typeof (services), baseServiceURL))
                {
                    configureService(baseSvc, typeof(IServices));
                    baseSvc.Open();

                    using (ServiceHost debugSvc = new ServiceHost(typeof (debugServices), debugServiceURL))
                    {
                        configureService(debugSvc, typeof(IDebugServices));
                        debugSvc.Open();

                        Console.WriteLine("BladeDirector ready.");
                        Console.WriteLine("Listening at main endpoint:  " + baseServiceURL.ToString());
                        Console.WriteLine("Listening at debug endpoint: " + debugServiceURL.ToString());
                        Console.WriteLine("Hit [enter] to exit.");
                        Console.ReadLine();
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

            svc.Faulted += (sender, args) =>
            {
                Debug.WriteLine("oh no");
            };

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

        // ReSharper restore UnusedAutoPropertyAccessor.Global
    }
}