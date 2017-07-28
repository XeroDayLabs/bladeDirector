using System;
using System.Threading;
using System.Threading.Tasks;
using bladeDirectorWCF;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace tests
{
    [TestClass]
    public class testBladeLockCollection
    {
        [TestMethod]
        public void testLockingBlocksWriterOnWrite()
        {
            bladeLockCollection uut = new bladeLockCollection(bladeLockType.lockNone, bladeLockType.lockIPAddresses);

            bool failed = false;

            bool resourceInUse = true;
            Task testLockTask = new Task(() =>
            {
                uut.acquire(bladeLockType.lockNone, bladeLockType.lockIPAddresses);
                if (resourceInUse)
                    failed = true;
            });
            testLockTask.Start();

            Thread.Sleep(TimeSpan.FromSeconds(1));
            resourceInUse = false;
            uut.release(bladeLockType.lockNone, bladeLockType.lockIPAddresses);

            Assert.IsFalse(failed);
        }

        [TestMethod]
        public void testLockingBlocksReaderOnWrite()
        {
            bladeLockCollection uut = new bladeLockCollection(bladeLockType.lockBIOS, bladeLockType.lockIPAddresses);

            bool failed = false;

            bool resourceInUse = true;
            Task testLockTask = new Task(() =>
            {
                uut.acquire(bladeLockType.lockIPAddresses, bladeLockType.lockNone);
                if (resourceInUse)
                    failed = true;
            });
            testLockTask.Start();

            Thread.Sleep(TimeSpan.FromSeconds(1));
            resourceInUse = false;
            uut.release(bladeLockType.lockBIOS, bladeLockType.lockIPAddresses);

            Assert.IsFalse(failed);
        }

        [TestMethod]
        public void testLockingDoesNotBlockOnMultipleReaders()
        {
            bladeLockCollection uut = new bladeLockCollection(bladeLockType.lockBIOS, bladeLockType.lockNone);

            bool failed = true;

            bool resourceInUse = true;
            Task testLockTask = new Task(() =>
            {
                uut.acquire(bladeLockType.lockBIOS, bladeLockType.lockNone);
                if (resourceInUse)
                    failed = false;
            });
            testLockTask.Start();

            Thread.Sleep(TimeSpan.FromSeconds(1));
            resourceInUse = false;
            uut.release(bladeLockType.lockBIOS, bladeLockType.lockNone);

            Assert.IsFalse(failed);
        }

        [TestMethod]
        public void testLockingWritesImplyReads()
        {
            bladeLockCollection uut = new bladeLockCollection(bladeLockType.lockNone, bladeLockType.lockBIOS);

            uut.release(bladeLockType.lockNone, bladeLockType.lockBIOS);

            Assert.IsFalse(uut.assertLocks(bladeLockType.lockNone, bladeLockType.lockNone));
        }

        [TestMethod]
        public void testReaderLockCanBeUpgradedAndThenReleased()
        {
            bladeLockCollection uut = new bladeLockCollection(bladeLockType.lockBIOS, bladeLockType.lockNone);

            uut.acquire(bladeLockType.lockNone, bladeLockType.lockBIOS);
            uut.release(bladeLockType.lockNone, bladeLockType.lockBIOS);

            Assert.IsTrue(uut.assertLocks(bladeLockType.lockBIOS, bladeLockType.lockNone));

            uut.release(bladeLockType.lockBIOS, bladeLockType.lockNone);

            Assert.IsTrue(uut.isUnlocked());
        }

        [TestMethod]
        public void testLockCanBeUpgradedAndThenReleased()
        {
            bladeLockCollection uut = new bladeLockCollection(bladeLockType.lockNone, bladeLockType.lockNone);
            uut.acquire(bladeLockType.lockOwnership, bladeLockType.lockBIOS);
            uut.release(bladeLockType.lockOwnership | bladeLockType.lockBIOS, bladeLockType.lockBIOS);
            Assert.IsTrue(uut.isUnlocked());
        }

        [TestMethod]
        public void testWriterLockCanBeDowngradedAndThenReleased()
        {
            bladeLockCollection uut = new bladeLockCollection(bladeLockType.lockNone, bladeLockType.lockSnapshot);
            uut.release(bladeLockType.lockNone, bladeLockType.lockSnapshot);
            
            // Downgrade should've happened.
            Assert.IsTrue(uut.assertLocks(bladeLockType.lockSnapshot, bladeLockType.lockNone));

            uut.release(bladeLockType.lockSnapshot, bladeLockType.lockNone);
            Assert.IsTrue(uut.isUnlocked());
        }

        [TestMethod]
        public void testReaderLockCanBeUpgradedAndDowngradedRepeatedlyAndThenReleased()
        {
            bladeLockCollection uut = new bladeLockCollection(bladeLockType.lockBIOS, bladeLockType.lockNone);

            uut.acquire(bladeLockType.lockNone, bladeLockType.lockBIOS);
            Assert.IsTrue(uut.assertLocks(bladeLockType.lockBIOS, bladeLockType.lockBIOS));
            uut.release(bladeLockType.lockNone, bladeLockType.lockBIOS);
            Assert.IsTrue(uut.assertLocks(bladeLockType.lockBIOS, bladeLockType.lockNone));

            uut.acquire(bladeLockType.lockNone, bladeLockType.lockBIOS);
            Assert.IsTrue(uut.assertLocks(bladeLockType.lockBIOS, bladeLockType.lockBIOS));
            uut.release(bladeLockType.lockNone, bladeLockType.lockBIOS);
            Assert.IsTrue(uut.assertLocks(bladeLockType.lockBIOS, bladeLockType.lockNone));

            uut.acquire(bladeLockType.lockNone, bladeLockType.lockBIOS);
            Assert.IsTrue(uut.assertLocks(bladeLockType.lockBIOS, bladeLockType.lockBIOS));
            uut.release(bladeLockType.lockNone, bladeLockType.lockBIOS);
            Assert.IsTrue(uut.assertLocks(bladeLockType.lockBIOS, bladeLockType.lockNone));

            uut.release(bladeLockType.lockBIOS, bladeLockType.lockNone);
            Assert.IsTrue(uut.isUnlocked());
        }
    }
}