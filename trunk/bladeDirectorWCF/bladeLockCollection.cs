using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Web.UI.WebControls;

namespace bladeDirectorWCF
{
    public class takenLockInfo
    {
        public int threadID = -1;
        public string stackTrace;
    }

    public class bladeLockCollection
    {
        private readonly string _name;
        private ConcurrentDictionary<string, nonDeadlockingRWLock> locksForThisBlade = new ConcurrentDictionary<string, nonDeadlockingRWLock>();
//        private ConcurrentDictionary<string, LockCookie> lockCookiesForThisBlade = new ConcurrentDictionary<string, LockCookie>();
        private ConcurrentDictionary<string, ConcurrentDictionary<int, takenLockInfo>> _readTakenList = new ConcurrentDictionary<string, ConcurrentDictionary<int, takenLockInfo>>();
        private ConcurrentDictionary<string, takenLockInfo> _writeTakenList = new ConcurrentDictionary<string, takenLockInfo>();

        private readonly TimeSpan lockTimeout = TimeSpan.FromSeconds(30);

        private const bool beSuperDuperVerbose = false;

        public bladeLockType faultInjectOnLockOfThis = 0;

        public bladeLockCollection(bladeLockType readLocks, bladeLockType writeLocks)
            : this("idk", readLocks, writeLocks)
        {
        }

        public bladeLockCollection(string name, bladeLockType readLocks, bladeLockType writeLocks)
        {
            _name = name;
            foreach (string lockTypeName in getLockNames())
            {
                locksForThisBlade.TryAdd(lockTypeName, new nonDeadlockingRWLock());
                _readTakenList.TryAdd(lockTypeName, new ConcurrentDictionary<int, takenLockInfo>());
                _writeTakenList.TryAdd(lockTypeName, new takenLockInfo());
            }

            debugmsg(Thread.CurrentThread.ManagedThreadId + " bladeLockCollection for blade " + _name + " constructed");
            acquire(readLocks, writeLocks);
        }

        private void debugmsg(string msg)
        {
            if (beSuperDuperVerbose)
                Debug.WriteLine(msg);
        }

        public static string[] getLockNames()
        {
            return Enum.GetNames(typeof (bladeLockType)).Where(x => x != "lockNone" && !x.StartsWith("lockAll")).ToArray();
        }

        public static bladeLockType clearField(bladeLockType src, bladeLockType toClear)
        {
            return (bladeLockType)(((int)src) & ~((int)toClear));
        }

        public void release(bladeLockType readTypes, bladeLockType writeTypes)
        {
            debugmsg(Thread.CurrentThread.ManagedThreadId + " bladeLockCollection release for blade " + _name + " releasing " + readTypes + " / " + writeTypes);
            debugmsg(Thread.CurrentThread.ManagedThreadId + Environment.StackTrace);
            foreach (string lockTypeName in getLockNames())
            {
                bladeLockType lockType = (bladeLockType)Enum.Parse(typeof(bladeLockType), lockTypeName);
                bool willReleaseReadLock = (((int)readTypes & (int)lockType) != 0);
                bool willReleaseWriteLock = (((int)writeTypes & (int)lockType) != 0);

                debugmsg(lockTypeName + " releasing " + willReleaseReadLock + "/" + willReleaseWriteLock + " : current access " + locksForThisBlade[lockTypeName].IsReaderLockHeld + "/" + locksForThisBlade[lockTypeName].IsWriterLockHeld);

                if (willReleaseWriteLock)
                {
                    _writeTakenList[lockTypeName].threadID = -1;

                    // We will downgrade the lock if neccessary.
                    if (willReleaseReadLock)
                    {
                        // We are releasing the whole lock.

                        debugmsg(Thread.CurrentThread.ManagedThreadId + " bladeLockCollection release for blade " + _name + lockTypeName + " downgrading");
                        locksForThisBlade[lockTypeName].DowngradeFromWriterLock();
                        debugmsg(Thread.CurrentThread.ManagedThreadId + " bladeLockCollection release for blade " + _name + lockTypeName + " releasing reader lock");
                        locksForThisBlade[lockTypeName].ReleaseReaderLock();

                        // The read lock is now not taken by this thread.
                        takenLockInfo tmp;
                        _readTakenList[lockTypeName].TryRemove(Thread.CurrentThread.ManagedThreadId, out tmp);
                    }
                    else
                    {
                        debugmsg(Thread.CurrentThread.ManagedThreadId + " bladeLockCollection release for blade " + _name + lockTypeName + " downgrading (!willReleaseReaderLock)");
                        locksForThisBlade[lockTypeName].DowngradeFromWriterLock();

                        takenLockInfo tmp;
                        _readTakenList[lockTypeName].TryRemove(Thread.CurrentThread.ManagedThreadId, out tmp);
                    }
                }
                else
                {
                    // Only release a read lock if we aren't releasing the whole write lock.
                    if (willReleaseReadLock)
                    {
                        takenLockInfo tmp;
                        _readTakenList[lockTypeName].TryRemove(Thread.CurrentThread.ManagedThreadId, out tmp);

                        debugmsg(Thread.CurrentThread.ManagedThreadId + " bladeLockCollection release for blade " + _name + " " + lockTypeName + " releasing reader lock (!willReleaseWriterLock)");
                        locksForThisBlade[lockTypeName].ReleaseReaderLock();
                    }
                }
            }
            debugmsg(Thread.CurrentThread.ManagedThreadId + " bladeLockCollection release for blade " + _name + " finished");
        }

        public void acquire(bladeLockType readTypes, bladeLockType writeTypes)
        {
            if (!tryAcquire(readTypes, writeTypes))
                throw new Exception("..");
            return;

            debugmsg(Thread.CurrentThread.ManagedThreadId + " bladeLockCollection for blade " + _name + " acquiring " + readTypes + " / " + writeTypes);
            debugmsg(Thread.CurrentThread.ManagedThreadId + Environment.StackTrace);

            DateTime deadline = DateTime.Now + TimeSpan.FromSeconds(10); 
            while (true)
            {
                if (tryAcquire(readTypes, writeTypes))
                    break;

                if (DateTime.Now > deadline)
                    throw new ApplicationException("can't lock");
                Thread.Sleep(TimeSpan.FromSeconds(1));
            }

            debugmsg(Thread.CurrentThread.ManagedThreadId + " bladeLockCollection acquisition for blade " + _name + " finished");
        }

        private bool tryAcquire(bladeLockType readTypes, bladeLockType writeTypes)
        {
            List<string> succededLocks = new List<string>();
            foreach (string lockTypeName in getLockNames())
            {
                bool didTake;
                try
                {
                    didTake = attemptToTakeSingleLock(readTypes, writeTypes, lockTypeName);
                }
                catch (Exception e)
                {
                    // this is a little tricky, because we need to release any locks we have taken before we return, but since
                    // attemptToTakeSingleLock failed, we have not taken all requested locks. Note that attemptToTakeSingleLock
                    // will undo the _current_ call if it throws. 
                    foreach (string lockToRelease in getLockNames())
                    {
                        if (!succededLocks.Contains(lockToRelease) || lockTypeName == lockToRelease)
                        {
                            // We did not set this one, so clear it from the collection.
                            readTypes &= ~((bladeLockType) (Enum.Parse(typeof(bladeLockType), lockToRelease)));
                            writeTypes &= ~((bladeLockType) (Enum.Parse(typeof(bladeLockType), lockToRelease)));
                        }
                    }
                    release(readTypes, writeTypes);

                    //if (e is ApplicationException)
                    //    return false;

                    throw;
                }
                if (didTake)
                    succededLocks.Add(lockTypeName);
            }

            return true;
        }

        private bool attemptToTakeSingleLock(bladeLockType readTypes, bladeLockType writeTypes, string singleLockToTake)
        {
            int lockBitMask = (int) Enum.Parse(typeof(bladeLockType), singleLockToTake);
            bool readRequested = ((int) readTypes & lockBitMask) != 0;
            bool writeRequested = ((int) writeTypes & lockBitMask) != 0;
            bool readAlreadyTaken = locksForThisBlade[singleLockToTake].IsReaderLockHeld;
            bool writeAlreadyTaken = locksForThisBlade[singleLockToTake].IsWriterLockHeld;

            if (!readRequested && !writeRequested)
                return false;

            if (readRequested && writeAlreadyTaken)
                throw new Exception("Read lock requested, but this owner already has a write on this lock");

            debugmsg(singleLockToTake + " requested " + readRequested + "/" + writeRequested + " : current access " + readAlreadyTaken + "/" + writeAlreadyTaken);

            if (faultInjectOnLockOfThis.ToString() == singleLockToTake)
                throw new ApplicationException("Injected fault on field " + faultInjectOnLockOfThis);

            if (readRequested)
            {
                lock (_readTakenList)
                {
                    foreach (takenLockInfo takenLockInfo in _readTakenList[singleLockToTake].Values)
                    {
                        if (takenLockInfo.threadID == Thread.CurrentThread.ManagedThreadId)
                        {
                            miniDumpUtils.dumpSelf(Path.Combine(Properties.Settings.Default.internalErrorDumpPath, string.Format("_lockAlreadyTaken_{0}.dmp", Guid.NewGuid().ToString())));
                            throw new Exception(string.Format("this thread already owns the read lock on {0}! Previous lock was taken by {1} <stack trace end>", singleLockToTake, takenLockInfo.stackTrace));
                        }
                    }
                }
            }

            if (writeRequested && _writeTakenList[singleLockToTake].threadID == Thread.CurrentThread.ManagedThreadId)
            {
                miniDumpUtils.dumpSelf(Path.Combine(Properties.Settings.Default.internalErrorDumpPath, string.Format("_lockAlreadyTaken_{0}.dmp", Guid.NewGuid().ToString())));
                throw new Exception(string.Format("this thread already owns the write lock on {0}! Previous lock was taken by {1} <stack trace end>", singleLockToTake, _writeTakenList[singleLockToTake].stackTrace));
            }

            // If we get to this point, we have either a read lock, a writer lock, or both. Writer locks imply read locks, so
            // we need to take the read lock now.
            // Note that we always acquire the reader lock and then upgrade to a writer lock,  instead of acquiring the writer 
            // lock, so that we can downgrade if required later on.
            if (!readAlreadyTaken)
            {
                try
                {
                    debugmsg(Thread.CurrentThread.ManagedThreadId + " bladeLockCollection for blade " + _name + singleLockToTake + " acquiring reader lock");
                    locksForThisBlade[singleLockToTake].AcquireReaderLock(lockTimeout);
                }
                catch (ApplicationException)
                {
                    // If .AcquireReaderLock has thrown, we assume that it has left the object unlocked.
                    string msg = makeAcquireFailMsgForRead(singleLockToTake);
                    throw new Exception(msg);
                }

                takenLockInfo newInfo = new takenLockInfo();
                newInfo.threadID = Thread.CurrentThread.ManagedThreadId;
                newInfo.stackTrace = Environment.StackTrace;
                if (!_readTakenList[singleLockToTake].TryAdd(Thread.CurrentThread.ManagedThreadId, newInfo))
                    throw new Exception();
            }

            if (writeRequested)
            {
                try
                {
                    debugmsg(Thread.CurrentThread.ManagedThreadId + " bladeLockCollection for blade " + _name + singleLockToTake + " UpgradeToWriterLock");
                    locksForThisBlade[singleLockToTake].UpgradeToWriterLock(TimeSpan.FromSeconds(10));
                }
                catch (ApplicationException)
                {
                    // If .UpgradeToWriterLock has thrown, we assume that it has left the object as it was before. Since this may
                    // be locked for read, we will release that here if neccessary.
                    if (!readAlreadyTaken)
                    {
                        locksForThisBlade[singleLockToTake].ReleaseReaderLock();
                        takenLockInfo tmp;
                        _readTakenList[singleLockToTake].TryRemove(Thread.CurrentThread.ManagedThreadId, out tmp);
                    }
                    string msg = makeAcquireFailMsgForWrite(singleLockToTake);
                    throw new Exception(msg);
                }

                _writeTakenList[singleLockToTake].threadID = Thread.CurrentThread.ManagedThreadId;
                _writeTakenList[singleLockToTake].stackTrace = Environment.StackTrace;
            }

            return true;
        }

        private string makeAcquireFailMsgForRead(string lockTypeName)
        {
            string msg = "Failed to acquire lock '" + lockTypeName + "' for read. \nRead locks are currently held by: ";
            foreach (takenLockInfo info in _readTakenList[lockTypeName].Values)
            {
                if (info.threadID == Thread.CurrentThread.ManagedThreadId)
                    continue;
                msg += " ***  Thread ID " + info.threadID + " allocated at " + info.stackTrace + "\n\n";
            }
            if (_writeTakenList[lockTypeName].threadID != -1)
                msg += "\nWrite lock is currently held by thread ID " + _writeTakenList[lockTypeName].threadID +
                       " allocated at " + _writeTakenList[lockTypeName].stackTrace;
            return msg;
        }

        private string makeAcquireFailMsgForWrite(string lockTypeName)
        {
            string msg = "Failed to acquire lock '" + lockTypeName + "' for write. \nRead locks are currently held by: ";
            foreach (takenLockInfo info in _readTakenList[lockTypeName].Values)
            {
                if (info.threadID == Thread.CurrentThread.ManagedThreadId)
                    continue;
                msg += " ***  Thread ID " + info.threadID + " allocated at " + info.stackTrace + "\n\n";

                bool isOK = false;
                foreach (var thread in Process.GetCurrentProcess().Threads)
                {
                    Thread managedThread = thread as Thread;
                    if (managedThread == null)
                        continue;
                    if (managedThread.ManagedThreadId == info.threadID)
                    {
                        isOK = true;
                        break;
                    }
                }
                if (!isOK)
                {
                    msg += "\n !!! Managed thread ID " + info.threadID + " holds a lock, but is dead! ;_;\n";
                }
            }
            if (_writeTakenList[lockTypeName].threadID != -1)
                msg += "\nWrite lock is currently held by thread ID " + _writeTakenList[lockTypeName].threadID +
                       " allocated at " + _writeTakenList[lockTypeName].stackTrace;
            return msg;
        }

        public void downgrade(bladeLockType toDropRead, bladeLockType toDropWrite)
        {
            release(toDropRead, toDropWrite);
        }

        public bool isUnlocked()
        {
            return _writeTakenList.Count(x => x.Value.threadID != -1) == 0 && _readTakenList.Count(x => x.Value.Count > 0) == 0;
        }

        public bool assertLocks(bladeLockType read, bladeLockType write)
        {
            foreach (string lockTypeName in getLockNames())
            {
                int lockBitMask = (int) Enum.Parse(typeof (bladeLockType), lockTypeName);
                bool readRequested = ((int) read & lockBitMask) != 0;
                bool writeRequested = ((int) write & lockBitMask) != 0;

                if (writeRequested)
                {
                    if (!locksForThisBlade[lockTypeName].IsWriterLockHeld)
                        return false;
                }
                else
                {
                    if (locksForThisBlade[lockTypeName].IsWriterLockHeld)
                        return false;

                    if (readRequested)
                    {
                        if (!locksForThisBlade[lockTypeName].IsReaderLockHeld)
                            return false;
                    }
                    else
                    {
                        if (locksForThisBlade[lockTypeName].IsReaderLockHeld)
                            return false;
                    }
                }
            }

            return true;
        }
    }

    public class nonDeadlockingRWLock
    {
        private readonly List<int> readers = new List<int>();
        private readonly List<int> writers = new List<int>();

        private readonly ConcurrentDictionary<int, bool> IsReaderLockHeldForThread = new ConcurrentDictionary<int, bool>();
        private readonly ConcurrentDictionary<int, bool> IsWriterLockHeldForThread = new ConcurrentDictionary<int, bool>();

        private int TID { get { return Thread.CurrentThread.ManagedThreadId; }}

        public nonDeadlockingRWLock()
        {

        }

        public bool IsReaderLockHeld
        {
            get
            {
                bool val;
                if (!IsReaderLockHeldForThread.TryGetValue(TID, out val))
                    return false;
                return val;
            }
        }

        public bool IsWriterLockHeld
        {
            get
            {
                bool val;
                if (!IsWriterLockHeldForThread.TryGetValue(TID, out val))
                    return false;
                return val;
            }
        }

        public void ReleaseReaderLock()
        {
            lock (readers)
            {
                readers.RemoveAll(x => x == TID);
                bool foo;
                IsReaderLockHeldForThread.TryRemove(TID, out foo);
            }
        }

        public void AcquireReaderLock(TimeSpan lockTimeout)
        {
            DateTime deadline = DateTime.Now + lockTimeout;
            while (true)
            {
                using (var lockedw = attemptLock(writers, TimeSpan.FromMilliseconds(100)))
                {
                    if (!lockedw.failed)
                    {
                        using (var lockedr = attemptLock(readers, TimeSpan.FromMilliseconds(100)))
                        {
                            if (!lockedr.failed)
                            {
                                if (lockedr.o.Count(x => x == TID) != 0)
                                    throw new Exception("Acquisition requested on already-owned read lock");

                                if (lockedw.o.Count == 0)
                                {
                                    lockedr.o.Add(TID);
                                    bool foo;
                                    IsReaderLockHeldForThread.TryRemove(TID, out foo);
                                    IsReaderLockHeldForThread.TryAdd(TID, true);
                                    return;
                                }
                            }
                        }
                    }

                    if (DateTime.Now > deadline)
                        throw new unableToLockException();
                }
            }
        }

        private attemptedLock<T> attemptLock<T>(T toLock, TimeSpan timeout)
        {
            if (!Monitor.TryEnter(toLock, timeout))
                return new attemptedLock<T>() { failed = true };
            return new attemptedLock<T>() { o = toLock };
        }

        public void DowngradeFromWriterLock()
        {
            using (var lockedw = attemptLock(writers, TimeSpan.FromMilliseconds(100)))
            {
                if (!lockedw.failed)
                {
                    var toRemove = writers.SingleOrDefault(x => x == TID);
                    if (toRemove == default(int))
                        throw new Exception("Downgrade attempted on unlocked lock");
//                    IsReaderLockHeldForThread.TryUpdate(TID, true, false);

                    bool foo;
                    IsWriterLockHeldForThread.TryRemove(TID, out foo);

                    writers.RemoveAll(x => x == TID);
                }
            }
        }

        public void UpgradeToWriterLock(TimeSpan lockTimeout)
        {
            DateTime deadline = DateTime.Now + lockTimeout;
            while (true)
            {
                using (var locked = attemptLock(readers, TimeSpan.FromMilliseconds(100)))
                {
                    if (!locked.failed)
                    {
                        var toRemove = readers.SingleOrDefault(x => x == TID);
                        if (toRemove == default(int))
                            throw new Exception("Upgrade attempted on unlocked lock");

                        if (readers.All(x => x == TID))
                        {
                            using (var lockedw = attemptLock(writers, TimeSpan.FromMilliseconds(100)))
                            {
                                if (!lockedw.failed)
                                {
                                    lockedw.o.Add(TID);

                                    bool foo;
                                    IsWriterLockHeldForThread.TryRemove(TID, out foo);
                                    IsWriterLockHeldForThread.TryAdd(TID, true);

                                    break;
                                }
                            }
                        }
                    }
                }

                if (DateTime.Now > deadline)
                    throw new unableToLockException();
            }
        }
    }

    [Serializable]
    public class unableToLockException : Exception
    {
    }

    public class attemptedLock<T> :IDisposable
    {
        public T o;
        public bool failed;

        public void Dispose()
        {
            if (!failed)
                Monitor.Exit(o);
        }
    }
}