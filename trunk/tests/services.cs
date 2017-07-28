using System;
using System.Diagnostics;
using System.IO;
using System.ServiceModel;
using System.Threading;
using tests.bladeDirectorServices;
using IServices = bladeDirectorWCF.IServices;

namespace tests
{
    public class services : IDisposable
    {
        public DebugServicesClient uutDebug;
        public ServicesClient uut;

        private Process bladeDirectorProcess;

        public services()
        {
            string baseURL = "http://127.0.0.1/" + Guid.NewGuid().ToString();
            string servicesURL = baseURL + "/bladeDirector";
            string servicesDebugURL = baseURL + "/bladeDirectorDebug";

            string bladeDirectorWCFExe = Path.Combine(Properties.Settings.Default.repoRoot, "trunk\\bladeDirectorWCF\\bin\\x64\\Debug\\bladeDirectorWCF.exe");
            ProcessStartInfo bladeDirectorExeInfo = new ProcessStartInfo(bladeDirectorWCFExe);
            bladeDirectorExeInfo.WorkingDirectory = Path.GetDirectoryName(bladeDirectorWCFExe);
            bladeDirectorExeInfo.Arguments = "--baseURL " + baseURL;
            bladeDirectorExeInfo.UseShellExecute = false;
            bladeDirectorExeInfo.RedirectStandardOutput = true;
            bladeDirectorProcess = Process.Start(bladeDirectorExeInfo);

            while (true)
            {
                string line = bladeDirectorProcess.StandardOutput.ReadLine();
                if (line.Contains("to exit"))
                    break;
            }

            Thread.Sleep(TimeSpan.FromSeconds(3));

            WSHttpBinding baseBinding = new WSHttpBinding
            {
                MaxReceivedMessageSize = Int32.MaxValue, ReaderQuotas = {MaxStringContentLength = Int32.MaxValue}
            };
            uut = new ServicesClient(baseBinding, new EndpointAddress(servicesURL));

            WSHttpBinding debugBinding = new WSHttpBinding
            {
                MaxReceivedMessageSize = Int32.MaxValue,
                ReaderQuotas = { MaxStringContentLength = Int32.MaxValue }
            };
            uutDebug = new DebugServicesClient(debugBinding,new EndpointAddress(servicesDebugURL));
        }

        public services(string[] IPAddresses, bool isMocked = true)
            : this()
        {
            uutDebug.initWithBladesFromIPList(IPAddresses, isMocked, NASFaultInjectionPolicy.retunSuccessful);
        }

        public services(bladeSpec[] bladeSpecs, bool isMocked = true)
            : this()
        {
            uutDebug.initWithBladesFromBladeSpec(bladeSpecs, isMocked, NASFaultInjectionPolicy.retunSuccessful);
        }

        public services(string ipAddress, bool isMocked = true)
            : this(new string[] { ipAddress }, isMocked)
        {
        }

        public void Dispose()
        {
            try
            {
                Debug.WriteLine("Log entries from bladeDirector:");
                foreach (string msg in uut.getLogEvents())
                    Debug.WriteLine(msg);
            }
            catch (Exception) { }

            // FIXME: why these casts?
            try { ((IDisposable)uutDebug).Dispose(); } catch (CommunicationException) { }
            try { ((IDisposable)uut).Dispose(); } catch (CommunicationException) { }

            if (bladeDirectorProcess != null)
            {
                try
                {
                    bladeDirectorProcess.Kill();
                }
                catch (Exception)
                {
                    // ...
                }

                bladeDirectorProcess.Dispose();
            }
        }
    }
}