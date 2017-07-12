using System;
using bladeDirector;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace tests
{
    [TestClass]
    public class mockedVMDestruction
    {

        [TestMethod]
        public void willDeallocateOldVMsOnLogon()
        {
            string hostIP = "1.1.1.1";

            // Allocate all the blades, then login again. The allocated blades should no longer be allocated.
            hostStateManagerMocked uut = new hostStateManagerMocked();
            uut.initWithBlades(new[] { "172.17.129.131" });
            string ourVM = testUtils.doVMAllocationForTest(uut, hostIP);

            // Find the parent blade of the VM we got, and make sure it is now in use (by the blade director)
            string bladeIP;
            using (lockableVMSpec VMSpec = uut.db.getVMByIP(ourVM))
            {
                using (lockableBladeSpec bladeSpec = uut.db.getBladeByIP(VMSpec.spec.parentBladeIP, bladeLockType.lockAll))
                {
                    GetBladeStatusResult bladeState = uut.db.getBladeStatus(bladeSpec.spec.bladeIP, hostIP);
                    bladeIP = bladeSpec.spec.bladeIP;
                    Assert.AreEqual(bladeState, GetBladeStatusResult.notYours);
                }
            }

            testUtils.doLogin(uut, hostIP);

            // The VM should now not exist.
            Assert.AreEqual(null, uut.db.getVMByIP(ourVM));

            // Find the parent blade of the VM we got, and make sure it is now unused.
            GetBladeStatusResult bladeState2 = uut.db.getBladeStatus(bladeIP, hostIP);
            Assert.AreEqual(bladeState2, GetBladeStatusResult.unused);
        }

        [TestMethod]
        public void willReUseOldVMsAfterLogon()
        {
            string hostIP = "1.1.1.1";

            hostStateManagerMocked uut = new hostStateManagerMocked();
            uut.initWithBlades(new[] { "172.17.129.131" });

            testUtils.doLogin(uut, hostIP);
            string ourVM = testUtils.doVMAllocationForTest(uut, hostIP);

            testUtils.doLogin(uut, hostIP);
            ourVM = testUtils.doVMAllocationForTest(uut, hostIP);
        }

        [TestMethod]
        public void willReUseOldVMsAfterLogonDuringBladeBoot()
        {
            string hostIP = "1.1.1.1";

            hostStateManagerMocked uut = new hostStateManagerMocked();

            testUtils.doLogin(uut, hostIP);
            string bladeIP = testUtils.doBladeAllocationForTest(uut, hostIP);
            // Start a 5-minute long BIOS operation, then cancel it by logging in again.
            ((biosReadWrite_mocked)(uut.biosRWEngine)).biosOperationTime = TimeSpan.FromMinutes(5);
            resultCode res = uut.rebootAndStartDeployingBIOSToBlade(bladeIP, hostIP, ".... some bios file here ... ");
            Assert.AreEqual(resultCode.pending, res);
            Assert.AreEqual(true, uut.isBladeMine(bladeIP, hostIP));

            // Now login again, cancelling the BIOS operation.
            testUtils.doLogin(uut, hostIP);

            // The blade should no longer be ours.
            Assert.AreEqual(false, uut.isBladeMine(bladeIP, hostIP));
        }

        [TestMethod]
        public void willReUseOldVMsAfterLogonDuringBiosOperation()
        {
            string hostIP = "1.1.1.1";

            hostStateManagerMocked uut = new hostStateManagerMocked();
            uut.initWithBlades(new[] { "172.17.129.131" });

            testUtils.doLogin(uut, hostIP);
            waitTokenType waitToken = testUtils.startSlowVMAllocationForTest(uut, hostIP);

            testUtils.doLogin(uut, hostIP, TimeSpan.FromMinutes(10));
            string ourVM = testUtils.doVMAllocationForTest(uut, hostIP);
        }
    }
}