using System;
using System.Diagnostics;
using System.IO;
using System.ServiceModel;
using System.Threading;

namespace bladeDirectorWCF
{
    public class BladeDirectorServices : IDisposable
    {
        public IServices svc;

        private Process _bladeDirectorProcess;

        private readonly WSHttpBinding baseBinding = createBinding();

        public string servicesURL { get; private set; }

        protected string baseURL { get; private set; }

        /// <summary>
        /// Launch the given exe with the the specified port, on a random URL.
        /// </summary>
        /// <param name="bladeDirectorWCFExe"></param>
        /// <param name="port"></param>
        /// <param name="withWeb"></param>
        public BladeDirectorServices(string bladeDirectorWCFExe, ushort port, Uri webUri)
        {
            baseURL = string.Format("http://127.0.0.1:{0}/{1}", port, Guid.NewGuid().ToString());
            servicesURL = baseURL + "/bladeDirector";

            ipUtils.ensurePortIsFree(port);
            if (webUri != null)
                ipUtils.ensurePortIsFree((ushort)webUri.Port);

            connectWithArgs(bladeDirectorWCFExe, "--baseURL " + baseURL + " " +
                                                 (webUri != null ? "--webURL " + webUri.AbsoluteUri : " --no-web "));

            connect();
        }

        /// <summary>
        /// Connect to a remote blade director as specified.
        /// </summary>
        public BladeDirectorServices(string url)
        {
            servicesURL = url;
            servicesURL = servicesURL.Replace("0.0.0.0", "127.0.0.1");

            connect();
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
                },
                Security = new WSHttpSecurity() { Mode = SecurityMode.None }
            };
        }

        public virtual void setReceiveTimeout(TimeSpan newTimeout)
        {
            this.baseBinding.ReceiveTimeout = newTimeout;
            this.baseBinding.ReliableSession.InactivityTimeout = newTimeout;

            connect();
        }

        private void connect()
        {
            waitUntilReady(() =>
            {
                if (svc != null)
                {
                    try { ((IDisposable)svc).Dispose(); }
                    catch (CommunicationException) { }
                    catch (TimeoutException) { }
                }

                svc = ChannelFactory<IServices>.CreateChannel(baseBinding, new EndpointAddress(servicesURL));
                svc.getAllBladeIP();
            });
        }

        protected void waitUntilReady(Action func)
        {
            // Wait until the main service is ready
            DateTime deadline = DateTime.Now + TimeSpan.FromMinutes(1);
            while (true)
            {
                try
                {
                    func();
                }
                catch (Exception e)
                {
                    if (DateTime.Now > deadline)
                        throw new Exception("BladeDirectorWCF did not start, perhaps another instance is running?", e);
                    Thread.Sleep(TimeSpan.FromSeconds(1));
                    continue;
                }
                break;
            }
        }

        protected void connectWithArgs(string bladeDirectorWCFExe, string args)
        {
            ProcessStartInfo bladeDirectorExeInfo = new ProcessStartInfo(bladeDirectorWCFExe);
            bladeDirectorExeInfo.WorkingDirectory = Path.GetDirectoryName(bladeDirectorWCFExe);
            bladeDirectorExeInfo.Arguments = args;
            Debug.WriteLine("Spawning bladeDirectorWCF with args '" + args + "'");
            _bladeDirectorProcess = Process.Start(bladeDirectorExeInfo);
        }

        public resultAndBladeName waitForSuccess(resultAndBladeName res, TimeSpan timeout)
        {
            resultAndBladeName toRet = waitForSuccessWithoutThrowing(res, timeout);

            if (toRet == null)
                throw new TimeoutException();

            if (toRet.result.code == resultCode.success ||
                toRet.result.code == resultCode.noNeedLah)
            {
                return toRet;
            }

            throw new Exception("Unexpected status during .getProgress: " + res.result.code + " / " + res.result.errMsg);
        }

        public resultAndBladeName waitForSuccessWithoutThrowing(resultAndBladeName res, TimeSpan timeout)
        {
            DateTime deadline = DateTime.Now + timeout;
            while (true)
            {
                switch (res.result.code)
                {
                    case resultCode.pending:
                        if (DateTime.Now > deadline)
                            return null;
                        res = (resultAndBladeName)this.svc.getProgress(res.waitToken);
                        break;

                    default:
                        return res;
                }
                Thread.Sleep(TimeSpan.FromSeconds(1));
            }
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
                foreach (logEntry msg in svc.getLogEvents(100))
                    Debug.WriteLine(msg.msg);
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

        public virtual void reconnect()
        {
            connect();
        }
    }
}