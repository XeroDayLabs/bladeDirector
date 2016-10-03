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
            string srcIP = Request.UserHostAddress;
            if (srcIP == null)
            {
                Response.Write("Cannot find host IP address");
                return;
            }

            bladeOwnership state = hostStateDB.getBladeByIP(srcIP);
            if (state == null)
            {
                Response.Write("No blade at this IP address");
                return;
            }

            string script = Properties.Resources.ipxeTemplate;

            lock (state)
            {
                if (state.state == bladeStatus.unused)
                    return;

                script = script.Replace("{BLADE_IP_ISCSI}", state.ISCSIIpAddress);
                script = script.Replace("{BLADE_NETMASK_ISCSI}", "255.255.255.0");
                script = script.Replace("{BLADE_DISK_NAME}", state.IPAddress);
                script = script.Replace("{HOST_IP}", state.currentOwner);
            }
            Response.Write(script);
        }
    }
}