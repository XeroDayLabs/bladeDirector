using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using bladeDirectorWCF;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace tests
{
    [TestClass]
    public class databaseConcurrencyTests_blades
    {
        [TestMethod]
        public void testObjectStorageAndRetrieval()
        {
            using (hostDB db = new hostDB())
            {
                bladeSpec toDB = new bladeSpec(db.conn, "1.1.1.1", "2.1.1.1", "3.1.1.1", 1234);
                db.addNode(toDB);

                using (lockableBladeSpec fromDB = db.getBladeByIP("1.1.1.1", bladeLockType.lockAll, bladeLockType.lockAll))
                {
                    Assert.AreEqual(toDB.bladeIP, fromDB.spec.bladeIP);
                    Assert.AreEqual(toDB.ESXiPassword, fromDB.spec.ESXiPassword);
                    Assert.AreEqual(toDB.ESXiUsername, fromDB.spec.ESXiUsername);
                    Assert.AreEqual(toDB.currentlyBeingAVMServer, fromDB.spec.currentlyBeingAVMServer);
                    Assert.AreEqual(toDB.currentlyHavingBIOSDeployed, fromDB.spec.currentlyHavingBIOSDeployed);
                    Assert.AreEqual(toDB.iLOIP, fromDB.spec.iLOIP);
                    Assert.AreEqual(toDB.iLOPort, fromDB.spec.iLOPort);
                    Assert.AreEqual(toDB.iLoPassword, fromDB.spec.iLoPassword);
                    Assert.AreEqual(toDB.iLoUsername, fromDB.spec.iLoUsername);
                    Assert.AreEqual(toDB.iscsiIP, fromDB.spec.iscsiIP);
                    Assert.AreEqual(toDB.lastDeployedBIOS, fromDB.spec.lastDeployedBIOS);
                    Assert.AreEqual(toDB.currentOwner, fromDB.spec.currentOwner);
                    Assert.AreEqual(toDB.vmDeployState, fromDB.spec.vmDeployState);
                    Assert.AreEqual(toDB.lastKeepAlive, fromDB.spec.lastKeepAlive);
                    Assert.AreEqual(toDB.nextOwner, fromDB.spec.nextOwner);
                    Assert.AreEqual(toDB.state, fromDB.spec.state);
                }
            }
        }

        [TestMethod]
        public void testDBObjectIsUpdatedFromDBOnPermissionUpgrade()
        {
            using (hostDB db = new hostDB())
            {
                bladeSpec toDB = new bladeSpec(db.conn, "1.1.1.2", "2.1.1.1", "3.1.1.1", 1234);
                db.addNode(toDB);

                using (lockableBladeSpec refB = db.getBladeByIP("1.1.1.2", bladeLockType.lockNone, bladeLockType.lockNone))
                {
                    Thread innerThread = new Thread(() =>
                    {
                        using (lockableBladeSpec refA = db.getBladeByIP("1.1.1.2", bladeLockType.lockNone,
                            bladeLockType.lockBIOS | bladeLockType.lockOwnership))
                        {
                            refA.spec.lastDeployedBIOS = "test data";
                            refA.spec.currentOwner = "Dave_Lister";
                        }
                    });
                    innerThread.Start();
                    innerThread.Join();

                    refB.upgradeLocks(bladeLockType.lockBIOS | bladeLockType.lockOwnership, bladeLockType.lockNone);
                    Assert.AreEqual("test data", refB.spec.lastDeployedBIOS);
                    Assert.AreEqual("Dave_Lister", refB.spec.currentOwner);
                }
            }
        }

        [TestMethod]
        public void testDBObjectThrowsAfterDowngradeToNoAccess()
        {
            using (hostDB db = new hostDB())
            {
                bladeSpec toDB = new bladeSpec(db.conn, "1.1.1.3", "2.1.1.1", "3.1.1.1", 1234);
                db.addNode(toDB);

                // Lock with write access to a field, and then downgrade to no access. Then, access the field we originally
                // locked, and expect an exception to be thrown.
                using (lockableBladeSpec refA = db.getBladeByIP("1.1.1.3", bladeLockType.lockNone,
                    bladeLockType.lockBIOS | bladeLockType.lockOwnership))
                {
                    refA.downgradeLocks(
                        bladeLockType.lockBIOS | bladeLockType.lockOwnership,
                        bladeLockType.lockBIOS | bladeLockType.lockOwnership);

                    Assert.AreEqual(bladeLockType.lockIPAddresses, refA.spec.permittedAccessRead);
                    Assert.AreEqual(bladeLockType.lockNone, refA.spec.permittedAccessWrite);

                    failIfNoThrow(() => { refA.spec.lastDeployedBIOS = "test data"; });
                    failIfNoThrow(() => { refA.spec.currentOwner = "Dave_Lister"; });
                }
            }
        }

        [TestMethod]
        public void testDBObjectThrowsAfterDowngradeToReadOnlyAccess()
        {
            using (hostDB db = new hostDB())
            {
                bladeSpec toDB = new bladeSpec(db.conn, "1.1.1.4", "2.1.1.1", "3.1.1.1", 1234);
                db.addNode(toDB);

                // Lock with write access to a field, and then downgrade to read-only access. Then, try to write to the field we 
                // originally locked, and expect an exception to be thrown.
                using (lockableBladeSpec refA = db.getBladeByIP("1.1.1.4", bladeLockType.lockNone,
                    bladeLockType.lockBIOS | bladeLockType.lockOwnership))
                {
                    refA.downgradeLocks(
                        bladeLockType.lockNone | bladeLockType.lockNone,
                        bladeLockType.lockBIOS | bladeLockType.lockOwnership);

                    // We have released the write lock, so we should be holding the read lock only.
                    Assert.AreEqual(bladeLockType.lockIPAddresses | bladeLockType.lockOwnership | bladeLockType.lockBIOS, refA.spec.permittedAccessRead);
                    Assert.AreEqual(bladeLockType.lockNone, refA.spec.permittedAccessWrite);

                    // We should not be permitted to write fields
                    failIfNoThrow(() => { refA.spec.lastDeployedBIOS = "test data"; });
                    failIfNoThrow(() => { refA.spec.currentOwner = "Dave_Lister"; });
                    // but should be permitted to read them.
                    failIfThrow(() => { Debug.WriteLine(refA.spec.currentOwner); });
                    failIfThrow(() => { Debug.WriteLine(refA.spec.lastDeployedBIOS); });
                }
            }
        }

        [TestMethod]
        public void testDBObjectThrowsAfterUpgradeToReadOnlyAccess()
        {
            using (hostDB db = new hostDB())
            {
                bladeSpec toDB = new bladeSpec(db.conn, "1.1.1.5", "2.1.1.1", "3.1.1.1", 1234);
                db.addNode(toDB);

                using (lockableBladeSpec refA = db.getBladeByIP("1.1.1.5", bladeLockType.lockNone, bladeLockType.lockNone))
                {
                    refA.upgradeLocks(
                        bladeLockType.lockBIOS | bladeLockType.lockOwnership,
                        bladeLockType.lockNone);

                    Assert.AreEqual(bladeLockType.lockIPAddresses | bladeLockType.lockOwnership | bladeLockType.lockBIOS, refA.spec.permittedAccessRead);
                    Assert.AreEqual(bladeLockType.lockNone, refA.spec.permittedAccessWrite);

                    failIfNoThrow(() => { refA.spec.lastDeployedBIOS = "test data"; });
                    failIfNoThrow(() => { refA.spec.currentOwner = "Dave_Lister"; });
                    failIfThrow(() => { Debug.WriteLine(refA.spec.lastDeployedBIOS); });
                    failIfThrow(() => { Debug.WriteLine(refA.spec.currentOwner); });
                }
            }
        }

        [TestMethod]
        public void testDBObjectThrowsAfterUpgradeToWriteAccess()
        {
            using (hostDB db = new hostDB())
            {
                bladeSpec toDB = new bladeSpec(db.conn, "1.1.1.6", "2.1.1.1", "3.1.1.1", 1234);
                db.addNode(toDB);

                using (lockableBladeSpec refA = db.getBladeByIP("1.1.1.6", bladeLockType.lockNone, bladeLockType.lockNone))
                {
                    refA.upgradeLocks(
                        bladeLockType.lockBIOS | bladeLockType.lockOwnership,
                        bladeLockType.lockBIOS | bladeLockType.lockOwnership);

                    Assert.AreEqual(bladeLockType.lockIPAddresses | bladeLockType.lockOwnership | bladeLockType.lockBIOS, refA.spec.permittedAccessRead);
                    Assert.AreEqual(bladeLockType.lockOwnership | bladeLockType.lockBIOS, refA.spec.permittedAccessWrite);

                    failIfThrow(() => { refA.spec.lastDeployedBIOS = "test data"; });
                    failIfThrow(() => { refA.spec.currentOwner = "Dave_Lister"; });
                    failIfThrow(() => { Debug.WriteLine(refA.spec.lastDeployedBIOS); });
                    failIfThrow(() => { Debug.WriteLine(refA.spec.currentOwner); });
                }
            }
        }

        [TestMethod]
        public void testDBObjectWritePermsImplyReadPerms()
        {
            using (hostDB db = new hostDB())
            {
                bladeSpec toDB = new bladeSpec(db.conn, "1.1.1.1", "2.1.1.1", "3.1.1.1", 1234);
                db.addNode(toDB);

                using (lockableBladeSpec fromDB = db.getBladeByIP("1.1.1.1", bladeLockType.lockNone, bladeLockType.lockOwnership))
                {
                    Debug.WriteLine(fromDB.spec.currentlyBeingAVMServer);
                    fromDB.spec.currentlyBeingAVMServer = true;
                }
            }
        }

        [TestMethod]
        public void testDBObjectWritePermsImplyReadPermsViaUpgrade()
        {
            using (hostDB db = new hostDB())
            {
                bladeSpec toDB = new bladeSpec(db.conn, "1.1.1.1", "2.1.1.1", "3.1.1.1", 1234);
                db.addNode(toDB);

                using (lockableBladeSpec fromDB = db.getBladeByIP("1.1.1.1", bladeLockType.lockNone, bladeLockType.lockNone))
                {
                    fromDB.upgradeLocks(bladeLockType.lockNone, bladeLockType.lockOwnership);
                    Debug.WriteLine(fromDB.spec.currentlyBeingAVMServer);
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
                bladeSpec toDB = new bladeSpec(db.conn, "1.1.1.7", "2.1.1.1", "3.1.1.1", 1234);
                db.addNode(toDB);

                ManualResetEvent canCheckRefB = new ManualResetEvent(false);
                ManualResetEvent testEnded = new ManualResetEvent(false);

                Thread innerThread = new Thread(() =>
                {
                    using (lockableBladeSpec refA = db.getBladeByIP("1.1.1.7", bladeLockType.lockNone,
                        bladeLockType.lockBIOS | bladeLockType.lockOwnership))
                    {
                        // Set some data, and then downgrade to a read-only lock. 
                        // The data should be flushed to the DB at that point, so we set a ManualResetEvent and the main thread
                        // will check that the data has indeed been flushed, by reading from the DB.

                        refA.spec.lastDeployedBIOS = "test data";
                        refA.spec.currentOwner = "Dave_Lister";

                        refA.downgradeLocks(
                            bladeLockType.lockNone | bladeLockType.lockNone,
                            bladeLockType.lockBIOS | bladeLockType.lockOwnership);

                        Assert.AreEqual(bladeLockType.lockIPAddresses | bladeLockType.lockOwnership | bladeLockType.lockBIOS, refA.spec.permittedAccessRead);
                        Assert.AreEqual(bladeLockType.lockNone, refA.spec.permittedAccessWrite);

                        canCheckRefB.Set();
                        testEnded.WaitOne();
                    }
                });
                innerThread.Start();
                canCheckRefB.WaitOne();
                try
                {
                    using (lockableBladeSpec refB = db.getBladeByIP("1.1.1.7", 
                        bladeLockType.lockBIOS | bladeLockType.lockOwnership, 
                        bladeLockType.lockNone))
                    {
                        Assert.AreEqual("Dave_Lister", refB.spec.currentOwner);
                        Assert.AreEqual("test data", refB.spec.lastDeployedBIOS);
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