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
                Response.Write("prompt Cannot find host IP address");
                return;
            }

            bladeOwnership state = hostStateDB.getBladeByIP(srcIP);
            if (state == null)
            {
                Response.Write("prompt No blade configured at this IP address");
                return;
            }

            string script = Properties.Resources.ipxeTemplate;

            lock (state)
            {
                if (state.state == bladeStatus.unused)
                {
                    Response.Write("prompt Blade does not have any owner");
                    return;
                }

                script = script.Replace("{BLADE_IP_ISCSI}", state.iscsiIP);
                script = script.Replace("{BLADE_IP_MAIN}", state.bladeIP);
                script = script.Replace("{BLADE_NETMASK_ISCSI}", "255.255.255.0");

                script = script.Replace("{HOST_IP}", state.currentOwner);
            }
            Response.Write(script);
        }
    }
}