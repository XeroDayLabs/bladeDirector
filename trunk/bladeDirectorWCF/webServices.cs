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
            WebOperationContext.Current.OutgoingResponse.ContentType = "text/plain";
            string toRet = services._generateIPXEScript(services.getSrcIP());
            return new MemoryStream(Encoding.ASCII.GetBytes(toRet));
        }
    }
}