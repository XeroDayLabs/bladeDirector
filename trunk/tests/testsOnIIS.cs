using System;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using tests.networkService;

namespace tests
{
    [TestClass]
    public class testsOnIIS
    { 
        [TestMethod]
        public void willSetBIOS()
        {
            string testBiosXML = Properties.Resources.testBIOS;

            using (servicesSoapClient uut = new networkService.servicesSoapClient("servicesSoap"))
            {
                resultCodeAndBladeName res = uut.RequestAnySingleNode();
                Assert.AreEqual(networkService.resultCode.success, res.code);

                // Write the new BIOS. This configuration has the 'boot state' of the numlock key set to 'off', so we can
                // read that back after writing to ensure that the BIOS write succeeds.
                resultCodeAndBIOSConfig resultAndReadBack = writeBIOSAndReadBack(uut, res.bladeName, testBiosXML);
                Assert.IsTrue(resultAndReadBack.BIOSConfig.Contains("<Section name=\"NumLock\">Off</Section>"));
                // Now we can modify the BIOS slightly, to set the numlock boot state to 'on', and again write/readback, and see
                // if the change was carried out.
                testBiosXML = testBiosXML.Replace("<Section name=\"NumLock\">Off</Section>", "<Section name=\"NumLock\">On</Section>");
                resultAndReadBack = writeBIOSAndReadBack(uut, res.bladeName, testBiosXML);
                Assert.IsTrue(resultAndReadBack.BIOSConfig.Contains("<Section name=\"NumLock\">On</Section>"));

                Assert.AreEqual("success", uut.releaseBlade(res.bladeName));
            }
        }

        private static resultCodeAndBIOSConfig writeBIOSAndReadBack(servicesSoapClient uut, string bladeIP, string testBiosXML)
        {
            networkService.resultCode result;
    
            result = uut.rebootAndStartDeployingBIOSToBlade(bladeIP, testBiosXML);
            if (result != networkService.resultCode.success && result != networkService.resultCode.noNeedLah)
                Assert.Fail("checkBIOSDeployProgress returned " + result + ", but we expected success or noNeedLah");
            do
            {
                uut.keepAlive();
                Thread.Sleep(TimeSpan.FromSeconds(5));
                result = uut.checkBIOSDeployProgress(bladeIP);
            } while (result == networkService.resultCode.pending);

            // Either of these codes are okay here.
            if (result != networkService.resultCode.success && result != networkService.resultCode.noNeedLah)
                Assert.Fail("checkBIOSDeployProgress returned " + result + ", but we expected success or noNeedLah");

            // Now check it wrote OK by reading it back and comparing the numlock key state.
            Assert.AreEqual(networkService.resultCode.pending, uut.rebootAndStartReadingBIOSConfiguration(bladeIP));
            resultCodeAndBIOSConfig resultAndReadBack;
            do
            {
                uut.keepAlive();
                Thread.Sleep(TimeSpan.FromSeconds(5));
                resultAndReadBack = uut.checkBIOSReadProgress(bladeIP);
            } while (resultAndReadBack.code == networkService.resultCode.pending);

            // We should definitely have written the config, so we don't permit the 'no need' code here.
            Assert.AreEqual(networkService.resultCode.success, resultAndReadBack.code);

            return resultAndReadBack;
        }
    }
}