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
        private ConcurrentDictionary<string, ReaderWriterLock> locksForThisBlade = new ConcurrentDictionary<string, ReaderWriterLock>();
        private ConcurrentDictionary<string, LockCookie> lockCookiesForThisBlade = new ConcurrentDictionary<string, LockCookie>();
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
                locksForThisBlade.TryAdd(lockTypeName, new ReaderWriterLock());
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

                        LockCookie lockCookie;
                        lockCookiesForThisBlade.TryRemove(lockTypeName, out lockCookie);
                        debugmsg(Thread.CurrentThread.ManagedThreadId + " bladeLockCollection release for blade " + _name + lockTypeName + " downgrading");
                        locksForThisBlade[lockTypeName].DowngradeFromWriterLock(ref lockCookie);
                        debugmsg(Thread.CurrentThread.ManagedThreadId + " bladeLockCollection release for blade " + _name + lockTypeName + " releasing reader lock");
                        locksForThisBlade[lockTypeName].ReleaseReaderLock();

                        // The read lock is now not taken by this thread.
                        takenLockInfo tmp;
                        _readTakenList[lockTypeName].TryRemove(Thread.CurrentThread.ManagedThreadId, out tmp);
                    }
                    else
                    {
                        LockCookie lockCookie;
                        lockCookiesForThisBlade.TryRemove(lockTypeName, out lockCookie);
                        debugmsg(Thread.CurrentThread.ManagedThreadId + " bladeLockCollection release for blade " + _name + lockTypeName + " downgrading (!willReleaseReaderLock)");
                        locksForThisBlade[lockTypeName].DowngradeFromWriterLock(ref lockCookie);

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
            debugmsg(Thread.CurrentThread.ManagedThreadId + " bladeLockCollection for blade " + _name + " acquiring " + readTypes + " / " + writeTypes);
            debugmsg(Thread.CurrentThread.ManagedThreadId + Environment.StackTrace);

            List<string> succededLocks = new List<string>();
            foreach (string lockTypeName in getLockNames())
            {
                try
                {
                    attemptToTakeSingleLock(readTypes, writeTypes, lockTypeName);
                }
                catch (Exception)
                {
                    // this is a little tricky, because we need to release any locks we have taken before we return, but since
                    // attemptToTakeSingleLock failed, we have not taken all requested locks. Note that attemptToTakeSingleLock
                    // will undo the _current_ call if it throws. 
                    foreach (string lockToRelease in getLockNames())
                    {
                        if (!succededLocks.Contains(lockToRelease))
                        {
                            // We did not set this one, so clear it from the collection.
                            readTypes &= ~((bladeLockType) (Enum.Parse(typeof(bladeLockType), lockTypeName)));
                            writeTypes &= ~((bladeLockType) (Enum.Parse(typeof(bladeLockType), lockTypeName)));
                        }
                    }
                    release(readTypes, writeTypes);

                    throw;
                }
                succededLocks.Add(lockTypeName);
            }
            debugmsg(Thread.CurrentThread.ManagedThreadId + " bladeLockCollection acquisition for blade " + _name + " finished");
        }

        private void attemptToTakeSingleLock(bladeLockType readTypes, bladeLockType writeTypes, string singleLockToTake)
        {
            int lockBitMask = (int) Enum.Parse(typeof(bladeLockType), singleLockToTake);
            bool readRequested = ((int) readTypes & lockBitMask) != 0;
            bool writeRequested = ((int) writeTypes & lockBitMask) != 0;
            bool readAlreadyTaken = locksForThisBlade[singleLockToTake].IsReaderLockHeld;
            bool writeAlreadyTaken = locksForThisBlade[singleLockToTake].IsWriterLockHeld;

            if (!readRequested && !writeRequested)
                return;

            if (readRequested && writeAlreadyTaken)
                throw new Exception("oh no");

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
                    lockCookiesForThisBlade[singleLockToTake] = locksForThisBlade[singleLockToTake].UpgradeToWriterLock(TimeSpan.FromSeconds(10));
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
}