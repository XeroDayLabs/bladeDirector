using System;
using System.Diagnostics;
using System.IO;
using System.ServiceModel.Web;
using System.Text;

namespace bladeDirectorWCF
{
    /// <summary>
    /// These methods are exposed over plain HTTP, so they can be called by external, non-C# implementations, such as iPXE.
    /// </summary>
    public class webServices : IWebServices
    {
        public Stream generateIPXEScript()
        {
            WebOperationContext.Current.OutgoingResponse.Headers.Add("Access-Control-Allow-Origin", "*");

            string srcIP = services.getSrcIP();

            if (WebOperationContext.Current.IncomingRequest.UriTemplateMatch.QueryParameters["hostip"] != null)
            {
                srcIP = services.sanitizeAddress(WebOperationContext.Current.IncomingRequest.UriTemplateMatch.QueryParameters["hostip"]);
            }

            WebOperationContext.Current.OutgoingResponse.ContentType = "text/plain";
            string toRet = services._generateIPXEScript(srcIP);
            return new MemoryStream(Encoding.ASCII.GetBytes(toRet));
        }

        public string lockAndReturn()
        {
            string srcIP = services.getSrcIP();

            if (WebOperationContext.Current.IncomingRequest.UriTemplateMatch.QueryParameters["hostip"] != null)
            {
                srcIP = services.sanitizeAddress(WebOperationContext.Current.IncomingRequest.UriTemplateMatch.QueryParameters["hostip"]);
            }

            services._lockAndNeverRelease(srcIP);

            return srcIP;
        }
    }
}