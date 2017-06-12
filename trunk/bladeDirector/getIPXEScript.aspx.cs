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

            // Select the appropriate template
            string script;
            bladeOwnership owner = null;
            bladeSpec bladeState = hostStateDB.getBladeByIP(srcIP);
            if (bladeState != null)
            {

                if (bladeState.currentlyHavingBIOSDeployed)
                    script = Properties.Resources.ipxeTemplateForBIOS;
                else if (bladeState.currentlyBeingAVMServer)
                    script = Properties.Resources.ipxeTemplateForESXi;
                else
                    script = Properties.Resources.ipxeTemplate;

                script = script.Replace("{BLADE_IP_ISCSI}", bladeState.iscsiIP);
                script = script.Replace("{BLADE_IP_MAIN}", bladeState.bladeIP);

                owner = bladeState;
            }
            else
            {
                vmSpec vmState = hostStateDB.getVMByIP(srcIP);

                if (vmState == null)
                {
                    Response.Write("#!ipxe\r\n");
                    Response.Write("prompt No blade configured at this IP address (" + srcIP + ")\r\n");
                    Response.Write("reboot\r\n");
                    hostStateDB.addLogEvent("IPXE script for blade " + srcIP + " requested, but blade is not configured");
                    return;
                }

                script = Properties.Resources.ipxeTemplate;

                script = script.Replace("{BLADE_IP_ISCSI}", vmState.iscsiIP);
                script = script.Replace("{BLADE_IP_MAIN}", vmState.VMIP);
                
                owner = vmState;
            }

            lock (owner)    // wait what
            {
                if (owner.state == bladeStatus.unused)
                {
                    Response.Write("#!ipxe\r\n");
                    Response.Write("prompt Blade does not have any owner");
                    hostStateDB.addLogEvent("IPXE script for blade " + srcIP + " requested, but blade is not currently owned");
                    return;
                }

                bladeOwnership bladeOwnership = hostStateDB.getBladeOrVMOwnershipByIP(srcIP);
                script = script.Replace("{BLADE_NETMASK_ISCSI}", "255.255.192.0");
                script = script.Replace("{BLADE_OWNER}", bladeOwnership.currentOwner);
                script = script.Replace("{BLADE_SNAPSHOT}", bladeOwnership.currentSnapshot);
            }
            Response.Write(script);
            hostStateDB.addLogEvent("IPXE script for blade " + srcIP + " generated (owner " + owner.currentOwner + ")");
        }
    }
}