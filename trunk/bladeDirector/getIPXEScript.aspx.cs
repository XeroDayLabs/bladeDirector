using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;

namespace bladeDirector
{
    public partial class getIPXEScript : System.Web.UI.Page
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
                Response.End();
            }
        }

        private void _Page_load(object sender, EventArgs eventArgs)
        {
            string srcIP = Request.UserHostAddress;
            if (srcIP == null)
            {
                Response.Write("#!ipxe\r\n");
                Response.Write("prompt Cannot find host IP address\r\n");
                Response.Write("reboot\r\n");
                return;
            }

            // Allow source IP to be overriden by a querystring parameter.
            if (!String.IsNullOrEmpty(Request.QueryString["hostIP"]))
                srcIP = Request.QueryString["hostIP"];

            bladeOwnership state = hostStateDB.getBladeByIP(srcIP);
            if (state == null)
            {
                Response.Write("#!ipxe\r\n");
                Response.Write("prompt No blade configured at this IP address\r\n");
                Response.Write("reboot\r\n");
                hostStateDB.addLogEvent("IPXE script for blade " + srcIP + " requested, but blade is not configured");
                return;
            }

            string script = Properties.Resources.ipxeTemplate;

            lock (state)
            {
                if (state.state == bladeStatus.unused)
                {
                    Response.Write("#!ipxe\r\n");
                    Response.Write("prompt Blade does not have any owner");
                    hostStateDB.addLogEvent("IPXE script for blade " + srcIP + " requested, but blade is not currently owned");
                    return;
                }

                script = script.Replace("{BLADE_IP_ISCSI}", state.iscsiIP);
                script = script.Replace("{BLADE_IP_MAIN}", state.bladeIP);
                script = script.Replace("{BLADE_NETMASK_ISCSI}", "255.255.255.0");
                script = script.Replace("{BLADE_SNAPSHOT}", hostStateDB.getCurrentSnapshotForBlade(srcIP));
            }
            Response.Write(script);
            hostStateDB.addLogEvent("IPXE script for blade " + srcIP + " generated (owner " + state.currentOwner + ")");

        }
    }
}