using System.Linq;
using System.Web;

namespace bladeDirector
{
    /// <summary>
    /// Summary description for getBIOSConfig
    /// </summary>
    public class getBIOSConfig : IHttpHandler
    {
        public void ProcessRequest(HttpContext context)
        {
            context.Response.ContentType = "text/plain";

            if (!context.Request.QueryString.AllKeys.Contains("hostip"))
            {
                context.Response.Write("Please supply the 'hostip' querystring parameter, denoting which blade to query for");
                return;
            }

            bladeSpec res = services.hostStateManager.db.getBladeByIP_withoutLocking(context.Request.QueryString["hostip"]);
            if (res == null)
            {
                context.Response.Write("Blade not found");
                return;
            }

            context.Response.Write(res.lastDeployedBIOS);
        }

        public bool IsReusable
        {
            get
            {
                return false;
            }
        }
    }
}