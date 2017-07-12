using System;
using System.Diagnostics;
using bladeDirector;
using System.Linq;
using System.Net;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace tests
{
    [TestClass]
    public class mockedBladeDestruction
    {
        [TestMethod]
        public void willDeallocateBlade()
        {
            string hostIP = "1.1.1.1";

            hostStateManagerMocked uut = new hostStateManagerMocked();
            string ourBlade = testUtils.doBladeAllocationForTest(uut, hostIP);

            // It should be ours..
            GetBladeStatusResult bladeState = uut.db.getBladeStatus(ourBlade, hostIP);
            Assert.AreEqual(bladeState, GetBladeStatusResult.yours);

            // Then free it
            resultCode releaseRes = uut.releaseBladeOrVM(ourBlade, hostIP);
            Assert.AreEqual(resultCode.success, releaseRes);

            // And it should be unused.
            bladeState = uut.db.getBladeStatus(ourBlade, hostIP);
            Assert.AreEqual(bladeState, GetBladeStatusResult.unused);
        }

        [TestMethod]
        public void willDeallocateBladeAfterVMDestruction()
        {
            string hostIP = "1.1.1.1";

            hostStateManagerMocked uut = new hostStateManagerMocked();
            uut.initWithBlades(new[] {"2.2.2.2"});
            string ourVM = testUtils.doVMAllocationForTest(uut, hostIP);

            // Our blade should become a VM server
            GetBladeStatusResult bladeState = uut.db.getBladeStatus("2.2.2.2", hostIP);
            Assert.AreEqual(GetBladeStatusResult.notYours, bladeState);

            // Then free the VM.
            resultCode releaseRes = uut.releaseBladeOrVM(ourVM, hostIP);
            Assert.AreEqual(resultCode.success, releaseRes);

            // The blade itself should become unused.
            bladeState = uut.db.getBladeStatus("2.2.2.2", hostIP);
            Assert.AreEqual(GetBladeStatusResult.unused, bladeState);
        }

        [TestMethod]
        public void willDeallocateBladeDuringBIOSSetting()
        {
            string hostIP = "1.1.1.1";

            hostStateManagerMocked uut = new hostStateManagerMocked();
            string ourBlade = testUtils.doBladeAllocationForTest(uut, hostIP, false);

            // Start a slowwww BIOS read
            ((biosReadWrite_mocked) uut.biosRWEngine).biosOperationTime = TimeSpan.FromMinutes(10);
            resultCode readRes = uut.rebootAndStartReadingBIOSConfiguration(ourBlade, hostIP);
            Assert.AreEqual(resultCode.success, readRes);

            // Then free the blade. The BIOS operation should be cancelled before it soaks up all the ten minutes of time.
            resultCode releaseRes = uut.releaseBladeOrVM(ourBlade, hostIP);
            Assert.AreEqual(resultCode.success, releaseRes);

            // And it should be no longer getting.
            resultCodeAndBIOSConfig bladeState = uut.checkBIOSReadProgress(ourBlade);
            Assert.AreEqual(resultCode.cancelled, bladeState.code);
        }

        [TestMethod]
        public void willDeallocateOldBladesOnLogon()
        {
            string hostIP = "1.1.1.1";

            // Allocate a blade, then login again. The allocated blade should no longer be allocated.
            hostStateManagerMocked uut = new hostStateManagerMocked();
            string ourBlade = testUtils.doBladeAllocationForTest(uut, hostIP, true);

            testUtils.doLogin(uut, hostIP);

            GetBladeStatusResult bladeState = uut.db.getBladeStatus(ourBlade, hostIP);
            Assert.AreEqual(bladeState, GetBladeStatusResult.unused);
        }
    }
}