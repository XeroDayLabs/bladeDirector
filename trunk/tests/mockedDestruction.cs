using System;
using System.Diagnostics;
using bladeDirector;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace tests
{
    [TestClass]
    public class mockedDestruction
    {
        [TestMethod]
        public void willDeallocateBlade()
        {
            string hostIP = "1.1.1.1";

            hostStateDB_mocked uut = new hostStateDB_mocked();
            string ourBlade = doBladeAllocationForTest(uut, hostIP);

            // It should be ours..
            GetBladeStatusResult bladeState = uut.getBladeStatus(ourBlade, hostIP);
            Assert.AreEqual(bladeState, GetBladeStatusResult.yours);

            // Then free it
            uut.releaseBladeOrVM(ourBlade, hostIP);

            // And it should be unused.
            bladeState = uut.getBladeStatus(ourBlade, hostIP);
            Assert.AreEqual(bladeState, GetBladeStatusResult.unused);
        }

        [TestMethod]
        public void willDeallocateBladeDuringBIOSSetting()
        {
            string hostIP = "1.1.1.1";

            hostStateDB_mocked uut = new hostStateDB_mocked();
            string ourBlade = doBladeAllocationForTest(uut, hostIP, true);

            resultCode readRes = uut.rebootAndStartReadingBIOSConfiguration(ourBlade, hostIP);
            Assert.AreEqual(readRes, resultCode.pending);

            // Then free it
            uut.releaseBladeOrVM(ourBlade, hostIP);

            // And it should be no longer getting.
            resultCodeAndBIOSConfig bladeState = uut.checkBIOSReadProgress(ourBlade);
            Assert.AreEqual(bladeState.code, resultCode.bladeNotFound);
        }

        [TestMethod]
        public void willDeallocateOldBladesOnLogon()
        {
            AppDomain.CurrentDomain.AssemblyResolve += (object sender, ResolveEventArgs args) =>
            {
                Debug.WriteLine("Client end loading module " + args.Name);
                if (args.Name.Contains("Hyper"))
                {
                    Debugger.Break();
                }
                return null; 
            };

            string hostIP = "1.1.1.1";

            // Allocate all the blades, then login again. The allocated blades should no longer be allocated.
            hostStateDB_mocked uut = new hostStateDB_mocked();
            string ourBlade = doBladeAllocationForTest(uut, hostIP, true);

            uut.logIn(hostIP);
            
            GetBladeStatusResult bladeState = uut.getBladeStatus(ourBlade, hostIP);
            Assert.AreEqual(bladeState, GetBladeStatusResult.unused);
        }

        private static string doBladeAllocationForTest(hostStateDB_mocked uut, string hostIP, bool failTCPConnectAttempts = false)
        {
            uut.initWithBlades(new[] { "172.17.129.131" });

            uut.onMockedExecution += mockedAllocation.respondToExecutionsCorrectly;
            uut.onTCPConnectionAttempt += (ip, port, finish, error, time, state) => { return !failTCPConnectAttempts; };

            resultCodeAndBladeName allocRes = uut.RequestAnySingleNode(hostIP);
            Assert.AreEqual(resultCode.success, allocRes.code);

            return allocRes.bladeName;
        }    
    }
}