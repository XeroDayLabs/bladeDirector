using System;
using System.Diagnostics;
using System.IO;
using System.ServiceModel;
using System.Threading;
using bladeDirectorClient.bladeDirectorService;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace bladeDirectorClient
{
    public class BladeDirectorServices : IDisposable
    {
        public ServicesClient svc;

        private Process _bladeDirectorProcess;

        private WSHttpBinding baseBinding;

        public string servicesURL { get; private set; }

        protected string baseURL { get; private set; }

        /// <summary>
        /// Launch the given exe with the the specified port, on a random URL.
        /// </summary>
        /// <param name="bladeDirectorWCFExe"></param>
        /// <param name="port"></param>
        /// <param name="withWeb"></param>
        public BladeDirectorServices(string bladeDirectorWCFExe, ushort port, bool withWeb)
        {
            baseURL = string.Format("http://127.0.0.1:{0}/{1}", port, Guid.NewGuid().ToString());
            servicesURL = baseURL + "/bladeDirector";

            iLoHypervisorPool.ensurePortIsFree(port);
            if (withWeb)
                iLoHypervisorPool.ensurePortIsFree(81);

            baseBinding = new WSHttpBinding
            {
                MaxReceivedMessageSize = Int32.MaxValue,
                ReaderQuotas = { MaxStringContentLength = Int32.MaxValue }
            };
            svc = new ServicesClient(baseBinding, new EndpointAddress(servicesURL));

            connectWithArgs(bladeDirectorWCFExe, "--baseURL " + baseURL + (withWeb ? "" : " --no-web "));
        }

        /// <summary>
        /// Connect to a remote blade director as specified.
        /// </summary>
        public BladeDirectorServices(string url)
        {
            servicesURL = url;
            WSHttpBinding baseBinding = new WSHttpBinding
            {
                MaxReceivedMessageSize = Int32.MaxValue,
                ReaderQuotas = { MaxStringContentLength = Int32.MaxValue }
            };
            svc = new ServicesClient(baseBinding, new EndpointAddress(servicesURL));
        }

        protected void connectWithArgs(string bladeDirectorWCFExe, string args)
        {
            ProcessStartInfo bladeDirectorExeInfo = new ProcessStartInfo(bladeDirectorWCFExe);
            bladeDirectorExeInfo.WorkingDirectory = Path.GetDirectoryName(bladeDirectorWCFExe);
            bladeDirectorExeInfo.Arguments = args;
            Debug.WriteLine("Spawning bladeDirectorWCF with args '" + args + "'");
            _bladeDirectorProcess = Process.Start(bladeDirectorExeInfo);

            // Wait until the service is ready
            DateTime deadline = DateTime.Now + TimeSpan.FromMinutes(1);
            while (true)
            {
                try
                {
                    svc.getAllBladeIP();
                }
                catch (Exception e)
                {
                    if (DateTime.Now > deadline)
                        throw new Exception("BladeDirectorWCF did not start, perhaps another instance is running?", e);
                    svc = new ServicesClient(baseBinding, new EndpointAddress(servicesURL));
                    Thread.Sleep(TimeSpan.FromSeconds(1));
                    continue;
                }
                break;
            }

        }

        public resultAndBladeName waitForSuccess(resultAndBladeName res, TimeSpan timeout)
        {
            DateTime deadline = DateTime.Now + timeout;
            while (res.result.code != resultCode.success)
            {
                switch (res.result.code)
                {
                    case resultCode.success:
                    case resultCode.noNeedLah:
                        break;
                    case resultCode.pending:
                        if (DateTime.Now > deadline)
                            throw new TimeoutException();
                        res = (resultAndBladeName) this.svc.getProgress(res.waitToken);
                        continue;
                    default:
                        throw new Exception("Unexpected status during .getProgress: " + res.result.code + " / " + res.result.errMsg);
                }
                Thread.Sleep(TimeSpan.FromSeconds(1));
            }
            return res;
        }

        public resultAndWaitToken waitForSuccess(resultAndWaitToken res, TimeSpan timeout)
        {
            DateTime deadline = DateTime.Now + timeout;
            while (res.result.code != resultCode.success)
            {
                switch (res.result.code)
                {
                    case resultCode.success:
                    case resultCode.noNeedLah:
                        break;
                    case resultCode.pending:
                        if (DateTime.Now > deadline)
                            throw new TimeoutException();
                        res = this.svc.getProgress(res.waitToken);
                        continue;
                    default:
                        throw new Exception("Unexpected status during .getProgress: " + res.result.code + " / " + res.result.errMsg);
                }
                Thread.Sleep(TimeSpan.FromSeconds(1));
            }
            return res;
        }

        public virtual void Dispose()
        {
            try
            {
                Debug.WriteLine("Log entries from bladeDirector:");
                foreach (string msg in svc.getLogEvents())
                    Debug.WriteLine(msg);
            }
            catch (Exception) { }

            // FIXME: why these casts?
            try { ((IDisposable)svc).Dispose(); }
            catch (CommunicationException) { }
            catch (TimeoutException) { }

            if (_bladeDirectorProcess != null)
            {
                try
                {
                    _bladeDirectorProcess.Kill();
                }
                catch (Exception)
                {
                    // ...
                }

                _bladeDirectorProcess.Dispose();
            }
        }
    }
}