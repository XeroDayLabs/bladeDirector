using System;
using System.Threading;
using System.Threading.Tasks;
using bladeDirectorClient.bladeDirectorService;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace tests
{
    [TestClass]
    public class bladeLockCollection
    {
        [TestMethod]
        public void testLockingBlocksWriterOnWrite()
        {
            bladeDirectorWCF.bladeLockCollection uut = new bladeDirectorWCF.bladeLockCollection(bladeDirectorWCF.bladeLockType.lockNone, bladeDirectorWCF.bladeLockType.lockIPAddresses);

            bool failed = false;

            bool resourceInUse = true;
            Task testLockTask = new Task(() =>
            {
                uut.acquire(bladeDirectorWCF.bladeLockType.lockNone, bladeDirectorWCF.bladeLockType.lockIPAddresses);
                if (resourceInUse)
                    failed = true;
            });
            testLockTask.Start();

            Thread.Sleep(TimeSpan.FromSeconds(1));
            resourceInUse = false;
            uut.release(bladeDirectorWCF.bladeLockType.lockNone, bladeDirectorWCF.bladeLockType.lockIPAddresses);

            Assert.IsFalse(failed);
        }

        [TestMethod]
        public void testLockingBlocksReaderOnWrite()
        {
            bladeDirectorWCF.bladeLockCollection uut = new bladeDirectorWCF.bladeLockCollection(bladeDirectorWCF.bladeLockType.lockBIOS, bladeDirectorWCF.bladeLockType.lockIPAddresses);

            bool failed = false;

            bool resourceInUse = true;
            Task testLockTask = new Task(() =>
            {
                uut.acquire(bladeDirectorWCF.bladeLockType.lockIPAddresses, bladeDirectorWCF.bladeLockType.lockNone);
                if (resourceInUse)
                    failed = true;
            });
            testLockTask.Start();

            Thread.Sleep(TimeSpan.FromSeconds(1));
            resourceInUse = false;
            uut.release(bladeDirectorWCF.bladeLockType.lockBIOS, bladeDirectorWCF.bladeLockType.lockIPAddresses);

            Assert.IsFalse(failed);
        }

        [TestMethod]
        public void testLockingDoesNotBlockOnMultipleReaders()
        {
            bladeDirectorWCF.bladeLockCollection uut = new bladeDirectorWCF.bladeLockCollection(bladeDirectorWCF.bladeLockType.lockBIOS, bladeDirectorWCF.bladeLockType.lockNone);

            bool failed = true;

            bool resourceInUse = true;
            Task testLockTask = new Task(() =>
            {
                uut.acquire(bladeDirectorWCF.bladeLockType.lockBIOS, bladeDirectorWCF.bladeLockType.lockNone);
                if (resourceInUse)
                    failed = false;
            });
            testLockTask.Start();

            Thread.Sleep(TimeSpan.FromSeconds(1));
            resourceInUse = false;
            uut.release(bladeDirectorWCF.bladeLockType.lockBIOS, bladeDirectorWCF.bladeLockType.lockNone);

            Assert.IsFalse(failed);
        }

        [TestMethod]
        public void testLockingWritesImplyReads()
        {
            bladeDirectorWCF.bladeLockCollection uut = new bladeDirectorWCF.bladeLockCollection(bladeDirectorWCF.bladeLockType.lockNone, bladeDirectorWCF.bladeLockType.lockBIOS);

            uut.release(bladeDirectorWCF.bladeLockType.lockNone, bladeDirectorWCF.bladeLockType.lockBIOS);

            Assert.IsFalse(uut.assertLocks(bladeDirectorWCF.bladeLockType.lockNone, bladeDirectorWCF.bladeLockType.lockNone));
        }

        [TestMethod]
        public void testReaderLockCanBeUpgradedAndThenReleased()
        {
            bladeDirectorWCF.bladeLockCollection uut = new bladeDirectorWCF.bladeLockCollection(bladeDirectorWCF.bladeLockType.lockBIOS, bladeDirectorWCF.bladeLockType.lockNone);

            uut.acquire(bladeDirectorWCF.bladeLockType.lockNone, bladeDirectorWCF.bladeLockType.lockBIOS);
            uut.release(bladeDirectorWCF.bladeLockType.lockNone, bladeDirectorWCF.bladeLockType.lockBIOS);

            Assert.IsTrue(uut.assertLocks(bladeDirectorWCF.bladeLockType.lockBIOS, bladeDirectorWCF.bladeLockType.lockNone));

            uut.release(bladeDirectorWCF.bladeLockType.lockBIOS, bladeDirectorWCF.bladeLockType.lockNone);

            Assert.IsTrue(uut.isUnlocked());
        }

        [TestMethod]
        public void testLockCanBeUpgradedAndThenReleased()
        {
            bladeDirectorWCF.bladeLockCollection uut = new bladeDirectorWCF.bladeLockCollection(bladeDirectorWCF.bladeLockType.lockNone, bladeDirectorWCF.bladeLockType.lockNone);
            uut.acquire(bladeDirectorWCF.bladeLockType.lockOwnership, bladeDirectorWCF.bladeLockType.lockBIOS);
            uut.release(bladeDirectorWCF.bladeLockType.lockOwnership | bladeDirectorWCF.bladeLockType.lockBIOS, bladeDirectorWCF.bladeLockType.lockBIOS);
            Assert.IsTrue(uut.isUnlocked());
        }

        [TestMethod]
        public void testWriterLockCanBeDowngradedAndThenReleased()
        {
            bladeDirectorWCF.bladeLockCollection uut = new bladeDirectorWCF.bladeLockCollection(bladeDirectorWCF.bladeLockType.lockNone, bladeDirectorWCF.bladeLockType.lockSnapshot);
            uut.release(bladeDirectorWCF.bladeLockType.lockNone, bladeDirectorWCF.bladeLockType.lockSnapshot);
            
            // Downgrade should've happened.
            Assert.IsTrue(uut.assertLocks(bladeDirectorWCF.bladeLockType.lockSnapshot, bladeDirectorWCF.bladeLockType.lockNone));

            uut.release(bladeDirectorWCF.bladeLockType.lockSnapshot, bladeDirectorWCF.bladeLockType.lockNone);
            Assert.IsTrue(uut.isUnlocked());
        }

        [TestMethod]
        public void testReaderLockCanBeUpgradedAndDowngradedRepeatedlyAndThenReleased()
        {
            bladeDirectorWCF.bladeLockCollection uut = new bladeDirectorWCF.bladeLockCollection(bladeDirectorWCF.bladeLockType.lockBIOS, bladeDirectorWCF.bladeLockType.lockNone);

            uut.acquire(bladeDirectorWCF.bladeLockType.lockNone, bladeDirectorWCF.bladeLockType.lockBIOS);
            Assert.IsTrue(uut.assertLocks(bladeDirectorWCF.bladeLockType.lockBIOS, bladeDirectorWCF.bladeLockType.lockBIOS));
            uut.release(bladeDirectorWCF.bladeLockType.lockNone, bladeDirectorWCF.bladeLockType.lockBIOS);
            Assert.IsTrue(uut.assertLocks(bladeDirectorWCF.bladeLockType.lockBIOS, bladeDirectorWCF.bladeLockType.lockNone));

            uut.acquire(bladeDirectorWCF.bladeLockType.lockNone, bladeDirectorWCF.bladeLockType.lockBIOS);
            Assert.IsTrue(uut.assertLocks(bladeDirectorWCF.bladeLockType.lockBIOS, bladeDirectorWCF.bladeLockType.lockBIOS));
            uut.release(bladeDirectorWCF.bladeLockType.lockNone, bladeDirectorWCF.bladeLockType.lockBIOS);
            Assert.IsTrue(uut.assertLocks(bladeDirectorWCF.bladeLockType.lockBIOS, bladeDirectorWCF.bladeLockType.lockNone));

            uut.acquire(bladeDirectorWCF.bladeLockType.lockNone, bladeDirectorWCF.bladeLockType.lockBIOS);
            Assert.IsTrue(uut.assertLocks(bladeDirectorWCF.bladeLockType.lockBIOS, bladeDirectorWCF.bladeLockType.lockBIOS));
            uut.release(bladeDirectorWCF.bladeLockType.lockNone, bladeDirectorWCF.bladeLockType.lockBIOS);
            Assert.IsTrue(uut.assertLocks(bladeDirectorWCF.bladeLockType.lockBIOS, bladeDirectorWCF.bladeLockType.lockNone));

            uut.release(bladeDirectorWCF.bladeLockType.lockBIOS, bladeDirectorWCF.bladeLockType.lockNone);
            Assert.IsTrue(uut.isUnlocked());
        }
    }
}