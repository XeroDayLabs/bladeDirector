using System;
using System.Diagnostics;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using bladeLockType = bladeDirectorWCF.bladeLockType;
using lockableVMSpec = bladeDirectorWCF.lockableVMSpec;
using hostDB = bladeDirectorWCF.hostDB;

namespace tests
{
    [TestClass]
    public class databaseConcurrencyTests_VMs
    {
        [TestMethod]
        public void testDBObjectIsUpdatedFromDBOnPermissionUpgrade()
        {
            using (hostDB db = new hostDB())
            {
                bladeDirectorWCF.vmSpec toDB = new bladeDirectorWCF.vmSpec(db.conn, "1.1.1.2", bladeLockType.lockAll, bladeLockType.lockAll);
                db.addNode(toDB);

                using (lockableVMSpec refB = db.getVMByIP("1.1.1.2", bladeLockType.lockNone, bladeLockType.lockNone))
                {
                    Thread innerThread = new Thread(() =>
                    {
                        using (lockableVMSpec refA = db.getVMByIP("1.1.1.2", bladeLockType.lockNone,
                            bladeLockType.lockVirtualHW | bladeLockType.lockOwnership))
                        {
                            refA.spec.friendlyName = "test data";
                            refA.spec.currentOwner = "Dave_Lister";
                        }
                    });
                    innerThread.Start();
                    innerThread.Join();

                    refB.upgradeLocks(bladeLockType.lockVirtualHW | bladeLockType.lockOwnership, bladeLockType.lockNone);
                    Assert.AreEqual("test data", refB.spec.friendlyName);
                    Assert.AreEqual("Dave_Lister", refB.spec.currentOwner);
                }
            }
        }

        [TestMethod]
        public void testDBObjectThrowsAfterDowngradeToNoAccess()
        {
            using (hostDB db = new hostDB())
            {
                bladeDirectorWCF.vmSpec toDB = new bladeDirectorWCF.vmSpec(db.conn, "1.1.1.3", bladeLockType.lockAll, bladeLockType.lockAll);
                db.addNode(toDB);

                // Lock with write access to a field, and then downgrade to no access. Then, access the field we originally
                // locked, and expect an exception to be thrown.
                using (lockableVMSpec refA = db.getVMByIP("1.1.1.3", bladeLockType.lockNone,
                    bladeLockType.lockVirtualHW | bladeLockType.lockOwnership))
                {
                    refA.downgradeLocks(
                        bladeLockType.lockVirtualHW | bladeLockType.lockOwnership,
                        bladeLockType.lockVirtualHW | bladeLockType.lockOwnership);

                    Assert.AreEqual(bladeLockType.lockIPAddresses, refA.spec.permittedAccessRead);
                    Assert.AreEqual(bladeLockType.lockNone, refA.spec.permittedAccessWrite);

                    failIfNoThrow(() => { refA.spec.friendlyName = "test data"; });
                    failIfNoThrow(() => { refA.spec.currentOwner = "Dave_Lister"; });
                }
            }
        }

        [TestMethod]
        public void testDBObjectThrowsAfterDowngradeToReadOnlyAccess()
        {
            using (hostDB db = new hostDB())
            {
                bladeDirectorWCF.vmSpec toDB = new bladeDirectorWCF.vmSpec(db.conn, "1.1.1.4", bladeLockType.lockAll, bladeLockType.lockAll);
                db.addNode(toDB);

                // Lock with write access to a field, and then downgrade to read-only access. Then, try to write to the field we 
                // originally locked, and expect an exception to be thrown.
                using (lockableVMSpec refA = db.getVMByIP("1.1.1.4", bladeLockType.lockNone,
                    bladeLockType.lockVirtualHW | bladeLockType.lockOwnership))
                {
                    refA.downgradeLocks(
                        bladeLockType.lockNone | bladeLockType.lockNone,
                        bladeLockType.lockVirtualHW | bladeLockType.lockOwnership);

                    // We have released the write lock, so we should be holding the read lock only.
                    Assert.AreEqual(bladeLockType.lockIPAddresses | bladeLockType.lockOwnership | bladeLockType.lockVirtualHW, refA.spec.permittedAccessRead);
                    Assert.AreEqual(bladeLockType.lockNone, refA.spec.permittedAccessWrite);

                    // We should not be permitted to write fields
                    failIfNoThrow(() => { refA.spec.friendlyName = "test data"; });
                    failIfNoThrow(() => { refA.spec.currentOwner = "Dave_Lister"; });
                    // but should be permitted to read them.
                    failIfThrow(() => { Debug.WriteLine(refA.spec.currentOwner); });
                    failIfThrow(() => { Debug.WriteLine(refA.spec.friendlyName); });
                }
            }
        }

        [TestMethod]
        public void testDBObjectThrowsAfterUpgradeToReadOnlyAccess()
        {
            using (hostDB db = new hostDB())
            {
                bladeDirectorWCF.vmSpec toDB = new bladeDirectorWCF.vmSpec(db.conn, "1.1.1.5", bladeLockType.lockAll, bladeLockType.lockAll);
                db.addNode(toDB);

                using (lockableVMSpec refA = db.getVMByIP("1.1.1.5", bladeLockType.lockNone, bladeLockType.lockNone))
                {
                    refA.upgradeLocks(
                        bladeLockType.lockVirtualHW | bladeLockType.lockOwnership,
                        bladeLockType.lockNone);

                    Assert.AreEqual(bladeLockType.lockIPAddresses | bladeLockType.lockOwnership | bladeLockType.lockVirtualHW, refA.spec.permittedAccessRead);
                    Assert.AreEqual(bladeLockType.lockNone, refA.spec.permittedAccessWrite);

                    failIfNoThrow(() => { refA.spec.friendlyName = "test data"; });
                    failIfNoThrow(() => { refA.spec.currentOwner = "Dave_Lister"; });
                    failIfThrow(() => { Debug.WriteLine(refA.spec.friendlyName); });
                    failIfThrow(() => { Debug.WriteLine(refA.spec.currentOwner); });
                }
            }
        }

        [TestMethod]
        public void testDBObjectThrowsAfterUpgradeToWriteAccess()
        {
            using (hostDB db = new hostDB())
            {
                bladeDirectorWCF.vmSpec toDB = new bladeDirectorWCF.vmSpec(db.conn, "1.1.1.6", bladeLockType.lockAll, bladeLockType.lockAll);
                db.addNode(toDB);

                using (lockableVMSpec refA = db.getVMByIP("1.1.1.6", bladeLockType.lockNone, bladeLockType.lockNone))
                {
                    refA.upgradeLocks(
                        bladeLockType.lockVirtualHW | bladeLockType.lockOwnership,
                        bladeLockType.lockVirtualHW | bladeLockType.lockOwnership);

                    Assert.AreEqual(bladeLockType.lockIPAddresses | bladeLockType.lockOwnership | bladeLockType.lockVirtualHW, refA.spec.permittedAccessRead);
                    Assert.AreEqual(bladeLockType.lockOwnership | bladeLockType.lockVirtualHW, refA.spec.permittedAccessWrite);

                    failIfThrow(() => { refA.spec.friendlyName = "test data"; });
                    failIfThrow(() => { refA.spec.currentOwner = "Dave_Lister"; });
                    failIfThrow(() => { Debug.WriteLine(refA.spec.friendlyName); });
                    failIfThrow(() => { Debug.WriteLine(refA.spec.currentOwner); });
                }
            }
        }

        private void failIfThrow(Action action)
        {
            action.Invoke();
        }

        private void failIfNoThrow(Action action)
        {
            try
            {
                action.Invoke();

                Assert.Fail("did not throw");
            }
            catch (Exception)
            {
                return;
            }
        }

        [TestMethod]
        public void testDBObjectFlushesToDBOnLockDowngrade()
        {
            using (hostDB db = new hostDB())
            {
                bladeDirectorWCF.vmSpec toDB = new bladeDirectorWCF.vmSpec(db.conn, "1.1.1.7", bladeLockType.lockAll, bladeLockType.lockAll);
                db.addNode(toDB);

                ManualResetEvent canCheckRefB = new ManualResetEvent(false);
                ManualResetEvent testEnded = new ManualResetEvent(false);

                Thread innerThread = new Thread(() =>
                {
                    using (lockableVMSpec refA = db.getVMByIP("1.1.1.7", bladeLockType.lockNone,
                        bladeLockType.lockVirtualHW | bladeLockType.lockOwnership))
                    {
                        // Set some data, and then downgrade to a read-only lock. 
                        // The data should be flushed to the DB at that point, so we set a ManualResetEvent and the main thread
                        // will check that the data has indeed been flushed, by reading from the DB.

                        refA.spec.friendlyName = "test data";
                        refA.spec.currentOwner = "Dave_Lister";

                        refA.downgradeLocks(
                            bladeLockType.lockNone | bladeLockType.lockNone,
                            bladeLockType.lockVirtualHW | bladeLockType.lockOwnership);

                        Assert.AreEqual(bladeLockType.lockIPAddresses | bladeLockType.lockOwnership | bladeLockType.lockVirtualHW, refA.spec.permittedAccessRead);
                        Assert.AreEqual(bladeLockType.lockNone, refA.spec.permittedAccessWrite);

                        canCheckRefB.Set();
                        testEnded.WaitOne();
                    }
                });
                innerThread.Start();
                canCheckRefB.WaitOne();
                try
                {
                    using (lockableVMSpec refB = db.getVMByIP("1.1.1.7",
                        bladeLockType.lockVirtualHW | bladeLockType.lockOwnership,
                        bladeLockType.lockNone))
                    {
                        Assert.AreEqual("Dave_Lister", refB.spec.currentOwner);
                        Assert.AreEqual("test data", refB.spec.friendlyName);
                    }
                }
                finally
                {
                    testEnded.Set();
                    innerThread.Join();
                }
            }
        }
    }
}