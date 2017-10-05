using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using bladeDirectorWCF;

namespace bladeDirectorService
{
    public partial class Service1 : ServiceBase
    {
        private  ManualResetEvent stopEvent = new ManualResetEvent(false);
        private Task mainTask;

        public Service1()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            bladeDirectorArgs wcfargs = new bladeDirectorArgs();
            wcfargs.baseURL = Properties.Settings.Default.baseURLAPI;
            wcfargs.webURL = Properties.Settings.Default.baseURLWeb;
            wcfargs.stopEvent = stopEvent;

            mainTask = new Task(() => bladeDirectorWCF.Program._Main(wcfargs));
            mainTask.Start();
        }

        protected override void OnStop()
        {
            stopEvent.Set();
            mainTask.Wait();
        }
    }
}
