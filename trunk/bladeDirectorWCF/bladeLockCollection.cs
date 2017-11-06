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

                debugmsg(lockTypeName + " releasing " + willReleaseReadLock + "/" + willReleaseWriteLock +
                    " : current access " + locksForThisBlade[lockTypeName].IsReaderLockHeld + "/" + locksForThisBlade[lockTypeName].IsWriterLockHeld);

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

            foreach (string lockTypeName in getLockNames())
            {
                int lockBitMask = (int)Enum.Parse(typeof(bladeLockType), lockTypeName);
                bool readRequested = ((int) readTypes & lockBitMask) != 0;
                bool writeRequested = ((int) writeTypes & lockBitMask) != 0;
                bool readAlreadyTaken = locksForThisBlade[lockTypeName].IsReaderLockHeld;
                bool writeAlreadyTaken = locksForThisBlade[lockTypeName].IsWriterLockHeld;

                if (!readRequested && !writeRequested)
                    continue;

                if (readRequested && writeAlreadyTaken)
                    throw new Exception("oh no");

                debugmsg(lockTypeName + " requested " + readRequested + "/" + writeRequested + " : current access " + readAlreadyTaken + "/" + writeAlreadyTaken);

                if (readRequested)
                {
                    lock (_readTakenList)
                    {
                        foreach (takenLockInfo takenLockInfo in _readTakenList[lockTypeName].Values)
                        {
                            if (takenLockInfo.threadID == Thread.CurrentThread.ManagedThreadId)
                            {
                                miniDumpUtils.dumpSelf(Path.Combine(Properties.Settings.Default.internalErrorDumpPath, "_lockAlreadyTaken_" + Guid.NewGuid().ToString() + ".dmp"));
                                throw new Exception("this thread already owns the read lock on " + lockTypeName + "! Previous lock was taken by " + takenLockInfo.stackTrace + " <stack trace end>");
                            }
                        }
                    }
                }

                if (writeRequested && _writeTakenList[lockTypeName].threadID == Thread.CurrentThread.ManagedThreadId)
                {
                    miniDumpUtils.dumpSelf(Path.Combine(Properties.Settings.Default.internalErrorDumpPath, "_lockAlreadyTaken_" + Guid.NewGuid().ToString() + ".dmp"));
                    throw new Exception("this thread already owns the write lock on " + lockTypeName + "! Previous lock was taken by " + _writeTakenList[lockTypeName].stackTrace + " <stack trace end>");
                }
                // IF we get to this point, we have either a read lock, a writer lock, or both. Writer locks imply read locks, so
                // we need to take the read lock now.
                // Note that we always acquire the reader lock and then upgrade to a writer lock,  instead of acquiring the writer 
                // lock, so that we can downgrade if required later on.
                if (!readAlreadyTaken)
                {
                    try
                    {
                        debugmsg(Thread.CurrentThread.ManagedThreadId + " bladeLockCollection for blade " + _name + lockTypeName + " acquiring reader lock");
                        locksForThisBlade[lockTypeName].AcquireReaderLock(lockTimeout);
                    }
                    catch (ApplicationException)
                    {
                        string msg = "Failed to acquire lock '" + lockTypeName + "' for read. \nRead locks are currently held by: ";
                        foreach (takenLockInfo info in _readTakenList[lockTypeName].Values)
                        {
                            if (info.threadID == Thread.CurrentThread.ManagedThreadId)
                                continue;
                            msg += " ***  Thread ID " + info.threadID + " allocated at " + info.stackTrace + "\n\n";
                        }
                        if (_writeTakenList[lockTypeName].threadID != -1)
                            msg += "\nWrite lock is currently held by thread ID " + _writeTakenList[lockTypeName].threadID + " allocated at " + _writeTakenList[lockTypeName].stackTrace;
                        throw new Exception(msg);
                    }
                    
                    takenLockInfo newInfo = new takenLockInfo();
                    newInfo.threadID = Thread.CurrentThread.ManagedThreadId;
                    newInfo.stackTrace = Environment.StackTrace;
                    if (!_readTakenList[lockTypeName].TryAdd(Thread.CurrentThread.ManagedThreadId, newInfo))
                        throw new Exception();
                }

                if (writeRequested)
                {
                    try
                    {
                        debugmsg(Thread.CurrentThread.ManagedThreadId + " bladeLockCollection for blade " + _name + lockTypeName + " UpgradeToWriterLock");
                        lockCookiesForThisBlade[lockTypeName] = locksForThisBlade[lockTypeName].UpgradeToWriterLock(TimeSpan.FromSeconds(10));
                    }
                    catch (ApplicationException)
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
                            msg += "\nWrite lock is currently held by thread ID " + _writeTakenList[lockTypeName].threadID + " allocated at " + _writeTakenList[lockTypeName].stackTrace;

                        throw new Exception(msg);
                    }

                    _writeTakenList[lockTypeName].threadID = Thread.CurrentThread.ManagedThreadId;
                    _writeTakenList[lockTypeName].stackTrace = Environment.StackTrace;
                }
            }
            debugmsg(Thread.CurrentThread.ManagedThreadId + " bladeLockCollection acquisition for blade " + _name + " finished");
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