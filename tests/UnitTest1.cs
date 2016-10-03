using System;
using System.Linq;
using bladeDirector;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace tests
{
    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public void canInitWithBladesAndGetListBack()
        {
            bladeDirector.requestNode.initWithBlades(new[] { "1.1.1.1", "2.2.2.2", "3.3.3.3" });

            requestNode uut = new bladeDirector.requestNode();

            string res = uut.ListNodes();
            string[] foundIPs = res.Split(',');
            Assert.AreEqual(3, foundIPs.Length);
            Assert.IsTrue(foundIPs.Contains("1.1.1.1"));
            Assert.IsTrue(foundIPs.Contains("2.2.2.2"));
            Assert.IsTrue(foundIPs.Contains("3.3.3.3"));
        }

        [TestMethod]
        public void canAllocateBlade()
        {
            bladeDirector.requestNode.initWithBlades(new[] { "1.1.1.1", "2.2.2.2", "3.3.3.3" });
            requestNode uut = new bladeDirector.requestNode();

            Assert.AreEqual(resultCode.success.ToString(), uut.RequestNode("1.1.1.1", "192.168.1.1"));

            string allocated = uut.getBladesByAllocatedServer("192.168.1.1");
            Assert.IsTrue(allocated.Contains("1.1.1.1"), "String '" + allocated + "' does not contiain IP we allocated");
        }
        
        [TestMethod]
        public void willReAllocateNode()
        {
            bladeDirector.requestNode.initWithBlades(new[] { "1.1.1.1", "2.2.2.2", "3.3.3.3" });
            requestNode uut = new bladeDirector.requestNode();

            Assert.AreEqual(resultCode.success.ToString(), uut.RequestNode("1.1.1.1", "192.168.1.1"));

            // First, the node should be ours.
            Assert.AreEqual(GetBladeStatusResult.yours.ToString(), uut.GetBladeStatus("1.1.1.1", "192.168.1.1"));

            // Then, someoene else requests it..
            Assert.AreEqual(resultCode.pending.ToString(), uut.RequestNode("1.1.1.1", "192.168.2.2"));

            // and it should be pending.
            Assert.AreEqual(GetBladeStatusResult.releasePending.ToString(), uut.GetBladeStatus("1.1.1.1", "192.168.1.1"));
            Assert.AreEqual(GetBladeStatusResult.releasePending.ToString(), uut.GetBladeStatus("1.1.1.1", "192.168.2.2"));

            // Then, we release it.. 
            Assert.AreEqual(resultCode.success.ToString(), uut.releaseBlade("1.1.1.1", "192.168.1.1"));

            // and it should belong to the second requestor.
            Assert.AreEqual(GetBladeStatusResult.notYours.ToString(), uut.GetBladeStatus("1.1.1.1", "192.168.1.1"));
            Assert.AreEqual(GetBladeStatusResult.yours.ToString(), uut.GetBladeStatus("1.1.1.1", "192.168.2.2"));
        }

    }
}
