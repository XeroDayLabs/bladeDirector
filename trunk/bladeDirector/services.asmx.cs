using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Services;

namespace bladeDirector
{
    /// <summary>
    /// Summary description for requestNode
    /// </summary>
    [WebService(Namespace = "http://tempuri.org/")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]
    // To allow this Web Service to be called from script, using ASP.NET AJAX, uncomment the following line. 
    // [System.Web.Script.Services.ScriptService]
    public class services : System.Web.Services.WebService
    {
        public static void initWithBlades(string[] bladeIPs)
        {
            hostStateDB.initWithBlades(bladeIPs);
        }

        public static void initWithBlades(bladeSpec[] spec)
        {
            hostStateDB.initWithBlades(spec);
        }

        [WebMethod]
        public string ListNodes()
        {
            string[] IPAddresses = hostStateDB.getAllBladeIP();
            return String.Join(",", IPAddresses);
        }

        [WebMethod]
        public string getBladesByAllocatedServer(string NodeIP)
        {
            string[] IPAddresses = hostStateDB.getBladesByAllocatedServer(NodeIP);
            return String.Join(",", IPAddresses);
        }

        [WebMethod]
        public resultCodeAndBladeName RequestAnySingleNode()
        {
            string srcIp = HttpContext.Current.Request.UserHostAddress;
            return RequestAnySingleNode(srcIp);
        }

        public resultCodeAndBladeName RequestAnySingleNode(string NodeIP)
        {
            resultCodeAndBladeName s = hostStateDB.RequestAnySingleNode(NodeIP);
            return s;
        }

        [WebMethod]
        public string RequestNode(string NodeIP)
        {
            string srcIp = HttpContext.Current.Request.UserHostAddress;
            return RequestNode(NodeIP, srcIp);
        }

        public string RequestNode(string NodeIP, string requestorIP)
        {
            resultCode s = hostStateDB.tryRequestNode(NodeIP, requestorIP);
            return  s.ToString();
        }

        [WebMethod]
        public string GetBladeStatus(string NodeIP)
        {
            string srcIp = HttpContext.Current.Request.UserHostAddress;
            return GetBladeStatus(NodeIP, srcIp);
        }

        public string GetBladeStatus(string nodeIP, string requestorIP)
        {
            GetBladeStatusResult s = hostStateDB.getBladeStatus(nodeIP, requestorIP);
            return s.ToString();
        }

        [WebMethod]
        public string releaseBlade(string NodeIP)
        {
            string srcIp = HttpContext.Current.Request.UserHostAddress;
            return releaseBlade(NodeIP, srcIp);
        }

        public string releaseBlade(string nodeIP, string requestorIP)
        {
            resultCode s = hostStateDB.releaseBlade(nodeIP, requestorIP);
            return s.ToString();
        }

        [WebMethod]
        public string forceBladeAllocation(string NodeIP, string newOwner)
        {
            resultCode s = hostStateDB.forceBladeAllocation(NodeIP, newOwner);
            return s.ToString();
        }

        [WebMethod]
        public bladeSpec getConfigurationOfBlade(string NodeIP)
        {
            return hostStateDB.getConfigurationOfBlade(NodeIP);
        }

        [WebMethod]
        public string getCurrentSnapshotForBlade(string NodeIP)
        {
            return hostStateDB.getCurrentSnapshotForBlade(NodeIP);
        }
    }
}
