using System;
using System.Web;
using System.Web.UI;
using bladeDirector.bladeDirectorSvc;
using bladeDirector.Properties;
using createDisks.bladeDirector;

namespace bladeDirector
{
    public partial class getIPXEScript : Page
    {
        protected void Page_Load(object sender, EventArgs e)
        {
            Response.Clear();
            try
            {
                _Page_load(sender, e);
            }
            finally 
            {
                // Don't Response.End, since that'll throw/catch an exception, which is a perf hit.
                // Instead, do this.
                HttpContext.Current.Response.Flush();
                HttpContext.Current.Response.SuppressContent = true;
                HttpContext.Current.ApplicationInstance.CompleteRequest(); 
            }
        }

        private void _Page_load(object sender, EventArgs eventArgs)
        {
            string srcIP = Request.UserHostAddress;
            // Allow source IP to be overriden by a querystring parameter.
            if (!String.IsNullOrEmpty(Request.QueryString["hostIP"]))
                srcIP = Request.QueryString["hostIP"];

            using (disposableServiceClient<bladeDirectorSvc.ServicesClient> svc = new disposableServiceClient<bladeDirectorSvc.ServicesClient>())
            {
 //               Response.Write(svc.commObj.generateIPXEScriptForOtherNode(srcIP));
            }
        }
    }
}