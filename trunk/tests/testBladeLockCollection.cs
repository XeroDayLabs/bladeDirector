using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using bladeLockType = bladeDirectorWCF.bladeLockType;

namespace tests
{
    [TestClass]
    public class bladeLockCollection
    {
        [TestMethod]
        public void testLockingBlocksWriterOnWrite()
        {
            bladeDirectorWCF.bladeLockCollection uut = new bladeDirectorWCF.bladeLockCollection(bladeLockType.lockNone, bladeLockType.lockIPAddresses);

            bool failed = false;

            bool resourceInUse = true;
            Thread testLockThread = new Thread(() =>
            {
                uut.acquire(bladeLockType.lockNone, bladeLockType.lockIPAddresses);
                if (resourceInUse)
                    failed = true;
            });
            testLockThread.Start();

            Thread.Sleep(TimeSpan.FromSeconds(1));
            resourceInUse = false;
            uut.release(bladeLockType.lockNone, bladeLockType.lockIPAddresses);

            Assert.IsFalse(failed);
        }

        [TestMethod]
        public void testLockingBlocksReaderOnWrite()
        {
            bladeDirectorWCF.bladeLockCollection uut = new bladeDirectorWCF.bladeLockCollection(bladeLockType.lockBIOS, bladeLockType.lockIPAddresses);

            bool failed = false;

            bool resourceInUse = true;
            Thread testLockThread = new Thread(() =>
            {
                uut.acquire(bladeLockType.lockIPAddresses, bladeLockType.lockNone);
                if (resourceInUse)
                    failed = true;
            });
            testLockThread.Start();

            Thread.Sleep(TimeSpan.FromSeconds(1));
            resourceInUse = false;
            uut.release(bladeLockType.lockBIOS, bladeLockType.lockIPAddresses);

            Assert.IsFalse(failed);
        }

        [TestMethod]
        public void testLockingDoesNotBlockOnMultipleReaders()
        {
            bladeDirectorWCF.bladeLockCollection uut = new bladeDirectorWCF.bladeLockCollection(bladeLockType.lockBIOS, bladeLockType.lockNone);

            bool failed = true;

            bool resourceInUse = true;
            Thread testLockThread = new Thread(() =>
            {
                uut.acquire(bladeLockType.lockBIOS, bladeLockType.lockNone);
                if (resourceInUse)
                    failed = false;
            });
            testLockThread.Start();

            Thread.Sleep(TimeSpan.FromSeconds(1));
            resourceInUse = false;
            uut.release(bladeLockType.lockBIOS, bladeLockType.lockNone);

            Assert.IsFalse(failed);
        }

        [TestMethod]
        public void testLockingWritesImplyReads()
        {
            bladeDirectorWCF.bladeLockCollection uut = new bladeDirectorWCF.bladeLockCollection(bladeLockType.lockNone, bladeLockType.lockBIOS);

            uut.release(bladeLockType.lockNone, bladeLockType.lockBIOS);

            Assert.IsFalse(uut.assertLocks(bladeLockType.lockNone, bladeLockType.lockNone));
        }

        [TestMethod]
        public void testReaderLockCanBeUpgradedAndThenReleased()
        {
            bladeDirectorWCF.bladeLockCollection uut = new bladeDirectorWCF.bladeLockCollection(bladeLockType.lockBIOS, bladeLockType.lockNone);

            uut.acquire(bladeLockType.lockNone, bladeLockType.lockBIOS);
            uut.release(bladeLockType.lockNone, bladeLockType.lockBIOS);

            Assert.IsTrue(uut.assertLocks(bladeLockType.lockBIOS, bladeLockType.lockNone));

            uut.release(bladeLockType.lockBIOS, bladeLockType.lockNone);

            Assert.IsTrue(uut.isUnlocked());
        }

        [TestMethod]
        public void testLockCanBeUpgradedAndThenReleased()
        {
            bladeDirectorWCF.bladeLockCollection uut = new bladeDirectorWCF.bladeLockCollection(bladeLockType.lockNone, bladeLockType.lockNone);
            uut.acquire(bladeLockType.lockOwnership, bladeLockType.lockBIOS);
            uut.release(bladeLockType.lockOwnership | bladeLockType.lockBIOS, bladeLockType.lockBIOS);
            Assert.IsTrue(uut.isUnlocked());
        }

        [TestMethod]
        public void testWriterLockCanBeDowngradedAndThenReleased()
        {
            bladeDirectorWCF.bladeLockCollection uut = new bladeDirectorWCF.bladeLockCollection(bladeLockType.lockNone, bladeLockType.lockSnapshot);
            uut.release(bladeLockType.lockNone, bladeLockType.lockSnapshot);
            
            // Downgrade should've happened.
            Assert.IsTrue(uut.assertLocks(bladeLockType.lockSnapshot, bladeLockType.lockNone));

            uut.release(bladeLockType.lockSnapshot, bladeLockType.lockNone);
            Assert.IsTrue(uut.isUnlocked());
        }

        [TestMethod]
        public void testReaderLockCanBeUpgradedAndDowngradedRepeatedlyAndThenReleased()
        {
            bladeDirectorWCF.bladeLockCollection uut = new bladeDirectorWCF.bladeLockCollection(bladeLockType.lockBIOS, bladeLockType.lockNone);

            Assert.IsTrue(uut.assertLocks(bladeLockType.lockBIOS, bladeLockType.lockNone));
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

        [TestMethod]
        public void testWriterLockWontLeaveThingsLockedOnException()
        {
            bladeDirectorWCF.bladeLockCollection uut = new bladeDirectorWCF.bladeLockCollection(bladeLockType.lockNone, bladeLockType.lockSnapshot);

            // Attempt to lock for two things - NAS and BIOS. Inject an exception on the NAS exception, which happens _after_ the
            // BIOS lock. Verify that the BIOS lock is also released.
            bool didThrow = false;
            try
            {
                uut.faultInjectOnLockOfThis = bladeLockType.lockNASOperations;
                uut.acquire(bladeLockType.lockNASOperations | bladeLockType.lockBIOS, bladeLockType.lockNASOperations | bladeLockType.lockBIOS);
            }
            catch (ApplicationException)
            {
                didThrow = true;
            }
            Assert.IsTrue(didThrow, "Fault was not injected?");
            uut.faultInjectOnLockOfThis = 0;

            uut.release(bladeLockType.lockNone, bladeLockType.lockSnapshot);
            Assert.IsTrue(uut.isUnlocked());
            uut.assertLocks(bladeLockType.lockNone, bladeLockType.lockNone);
        }

        [TestMethod]
        public void testTrickyDeadlockSituation()
        {
            bladeDirectorWCF.bladeLockCollection uut = new bladeDirectorWCF.bladeLockCollection(bladeLockType.lockNone, bladeLockType.lockNone);

            int threadCount = 2;
            int counts = 1000;
            Exception[] exceptions = new Exception[threadCount];
            Thread[] threads = new Thread[threadCount];

            for (int i = 0; i < threadCount; i++)
            {
                int idx = i;
                threads[idx] = new Thread(() =>
                {
                    try
                    {
                        for (int n = 0; n < counts; n++)
                        {
                            if (idx%2 == 0)
                            {
                                uut.acquire(bladeLockType.lockBIOS, bladeLockType.lockOwnership);
                                uut.release(bladeLockType.lockBIOS, bladeLockType.lockOwnership);
                            }
                            else
                            {
                                uut.acquire(bladeLockType.lockOwnership, bladeLockType.lockBIOS);
                                uut.release(bladeLockType.lockOwnership, bladeLockType.lockBIOS);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        exceptions[idx] = e;
                    }
                });
            }

            foreach (Thread t in threads)
                t.Start();
            foreach (Thread t in threads)
                t.Join();

            foreach (Exception e in exceptions)
            {
                if (e != null)
                    throw e;
            }

            uut.release(bladeLockType.lockNone, bladeLockType.lockNone);
            Assert.IsTrue(uut.isUnlocked());
        }
    }
}