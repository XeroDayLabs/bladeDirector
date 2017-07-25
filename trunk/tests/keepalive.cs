using System;
using System.Threading;
using bladeDirectorClient;
using tests.bladeDirectorServices;

namespace tests
{/*
    public class keepalive :IDisposable
    {
        private string _serverName ;
        private Thread keepaliveThread;
        private bool timeToDie = false;

        public keepalive(string serverName)
        {
            _serverName = serverName;
            keepaliveThread = new Thread( keepaliveThreadMain);
            keepaliveThread.Name = "Blade director keepalive thread";
            keepaliveThread.Start();
        }

        private void keepaliveThreadMain()
        {
            servicesSoapClient director = new servicesSoapClient("servicesSoap", _serverName);
            {
                DateTime nextKeepaliveAt = DateTime.MinValue; 
                while (true)
                {
                    if (timeToDie)
                    {
                        return;
                    }

                    Thread.Sleep(TimeSpan.FromMilliseconds(100));
                    
                    if (DateTime.Now >= nextKeepaliveAt)
                    {
                        try
                        {
                            director.keepAlive();
                        }
                        catch (TimeoutException)
                        {
                            // ...
                        }
                        nextKeepaliveAt = DateTime.Now + TimeSpan.FromSeconds(3); 
                    }
                }
            }
        }

        public void Dispose()
        {
            timeToDie = true;
        }
    }*/
}