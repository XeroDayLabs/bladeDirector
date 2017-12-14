using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using bladeDirectorWCF;
using hypervisors;

namespace bladeDirector
{
    public partial class iloConsole : System.Web.UI.Page
    {
        protected void Page_Load(object sender, EventArgs e)
        {
            if (!Request.QueryString.HasKeys())
                return;
            string ip = Request.QueryString["bladeip"];

            string serverURL = status.getCurrentServerURL(this);
            if (serverURL == null)
            {
                Response.Write("you're not logged in");
            }
            else
            {
                using (BladeDirectorServices services = new BladeDirectorServices(serverURL))
                {
                    var spec = services.svc.getBladeByIP_withoutLocking(ip);

                    using (
                        hypervisor_iLo_HTTP theIlo = new hypervisor_iLo_HTTP(spec.iLOIP, spec.iLoUsername,
                            spec.iLoPassword))
                    {
                        theIlo.logoutOnDisposal = false;
                        theIlo.connect();
                        Response.Redirect(theIlo.makeHPLOLink());
                    }
                }
            }
        }
    }
}