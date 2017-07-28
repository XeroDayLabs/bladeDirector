using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
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
        private Dictionary<string, ReaderWriterLock> locksForThisBlade = new Dictionary<string, ReaderWriterLock>();
        private Dictionary<string, LockCookie> lockCookiesForThisBlade = new Dictionary<string, LockCookie>();
        private Dictionary<string, List<takenLockInfo>> _readTakenList = new Dictionary<string, List<takenLockInfo>>();
        private Dictionary<string, takenLockInfo> _writeTakenList = new Dictionary<string, takenLockInfo>();

        private bladeLockType _readLock;
        private bladeLockType _writeLock;

        public bladeLockCollection(bladeLockType readLocks, bladeLockType writeLocks)
            : this("idk", readLocks, writeLocks)
        {
        }

        public bladeLockCollection(string name, bladeLockType readLocks, bladeLockType writeLocks)
        {
            _name = name;
            foreach (string lockTypeName in getLockNames())
            {
                locksForThisBlade.Add(lockTypeName, new ReaderWriterLock());
                _readTakenList.Add(lockTypeName, new List<takenLockInfo>());
                _writeTakenList.Add(lockTypeName, new takenLockInfo());
            }

            Debug.WriteLine(Thread.CurrentThread.ManagedThreadId + " bladeLockCollection for blade " + _name + " constructed");
            acquire(readLocks, writeLocks);
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
            Debug.WriteLine(Thread.CurrentThread.ManagedThreadId + " bladeLockCollection release for blade " + _name + " releasing " + readTypes + " / " + writeTypes);
            Debug.WriteLine(Thread.CurrentThread.ManagedThreadId + Environment.StackTrace);
            foreach (string lockTypeName in getLockNames())
            {
                bladeLockType lockType = (bladeLockType)Enum.Parse(typeof(bladeLockType), lockTypeName);
                bool willReleaseReadLock = (((int)readTypes & (int)lockType) != 0);
                bool willReleaseWriteLock = (((int)writeTypes & (int)lockType) != 0);

                if (willReleaseWriteLock)
                {
                    _writeTakenList[lockTypeName].threadID = -1;
                    _writeLock = clearField(_writeLock, lockType);

                    // We will downgrade the lock if neccessary.
                    if (willReleaseReadLock)
                    {
                        // We are releasing the whole lock.

                        LockCookie lockCookie;
                        lock (lockCookiesForThisBlade)
                        {
                            if (!lockCookiesForThisBlade.ContainsKey(lockTypeName))
                                throw new Exception("Lock cookie not found; are you releasing something not yet acquired?");
                            lockCookie = lockCookiesForThisBlade[lockTypeName];
                            lockCookiesForThisBlade.Remove(lockTypeName);
                        }
                        Debug.WriteLine(Thread.CurrentThread.ManagedThreadId + " bladeLockCollection release for blade " + _name + lockTypeName + " downgrading");
                        locksForThisBlade[lockTypeName].DowngradeFromWriterLock(ref lockCookie);
                        Debug.WriteLine(Thread.CurrentThread.ManagedThreadId + " bladeLockCollection release for blade " + _name + lockTypeName + " releasing reader lock");
                        locksForThisBlade[lockTypeName].ReleaseReaderLock();

                        // The read lock is now not taken.
                        lock (_readTakenList) // fixme: ugh locking
                        {
                            _readTakenList[lockTypeName].RemoveAll(x => x.threadID == Thread.CurrentThread.ManagedThreadId);
                        }
                        _readLock = clearField(_readLock, lockType);
                    }
                    else
                    {
                        LockCookie lockCookie;
                        lock (lockCookiesForThisBlade)
                        {
                            if (!lockCookiesForThisBlade.ContainsKey(lockTypeName))
                                throw new Exception("Lock cookie not found; are you releasing something not yet acquired?");

                            lockCookie = lockCookiesForThisBlade[lockTypeName];
                            lockCookiesForThisBlade.Remove(lockTypeName);
                        }
                        Debug.WriteLine(Thread.CurrentThread.ManagedThreadId + " bladeLockCollection release for blade " + _name + lockTypeName + " downgrading (!willReleaseReaderLock)");
                        locksForThisBlade[lockTypeName].DowngradeFromWriterLock(ref lockCookie);

                        lock (_readTakenList) // fixme: ugh locking
                        {
                            _readTakenList[lockTypeName].RemoveAll(x => x.threadID == Thread.CurrentThread.ManagedThreadId);
                        }
                        _readLock |= lockType;
                    }
                }
                else
                {
                    // Only release a read lock if we aren't releasing the whole write lock.
                    if (willReleaseReadLock)
                    {
                        lock (_readTakenList) // fixme: ugh locking
                        {
                            _readTakenList[lockTypeName].RemoveAll(x => x.threadID == Thread.CurrentThread.ManagedThreadId);
                        }

                        _readLock = clearField(_readLock, lockType);
                        Debug.WriteLine(Thread.CurrentThread.ManagedThreadId + " bladeLockCollection release for blade " + _name + " " + lockTypeName + " releasing reader lock (!willReleaseWriterLock)");
                        locksForThisBlade[lockTypeName].ReleaseReaderLock();
                    }
                }
            }
            Debug.WriteLine(Thread.CurrentThread.ManagedThreadId + " bladeLockCollection release for blade " + _name + " finished, new access " + _readLock + " / " + _writeLock);
        }

        private List<Thread> seenThreads = new List<Thread>(); 

        public void acquire(bladeLockType readTypes, bladeLockType writeTypes)
        {
            Debug.WriteLine(Thread.CurrentThread.ManagedThreadId + " bladeLockCollection for blade " + _name + " acquiring " + readTypes + " / " + writeTypes); 
            Debug.WriteLine(Thread.CurrentThread.ManagedThreadId + Environment.StackTrace);

            lock (seenThreads)
            {
                if (!seenThreads.Contains(Thread.CurrentThread))
                    seenThreads.Add(Thread.CurrentThread);
            }

            lock (seenThreads)
            {
                foreach (Thread seenThread in seenThreads)
                {
                    if (!seenThread.IsAlive)
                    {
                        //Debug.WriteLine("managed thread " + seenThread.ManagedThreadId + " is dead");
                        if (_readTakenList.Any(y => y.Value.Any(x => x.threadID == seenThread.ManagedThreadId)))
                            throw new AbandonedMutexException();
                        if (_writeTakenList.Any(x => x.Value.threadID == seenThread.ManagedThreadId))
                            throw new AbandonedMutexException();
                        //seenThreads.Remove(seenThread);
                    }
                }
            }

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

                if (readRequested)
                {
                    lock (_readTakenList)
                    {
                        foreach (takenLockInfo takenLockInfo in _readTakenList[lockTypeName])
                        {
                            if (takenLockInfo.threadID == Thread.CurrentThread.ManagedThreadId)
                                throw new Exception("this thread already owns the read lock on " + lockTypeName + "! Previous lock was taken by " + takenLockInfo.stackTrace + " <stack trace end>");
                        }
                    }
                }

                if (writeRequested &&  _writeTakenList[lockTypeName].threadID == Thread.CurrentThread.ManagedThreadId)
                    throw new Exception("this thread already owns the write lock on " + lockTypeName + "! Previous lock was taken by " + _writeTakenList[lockTypeName].stackTrace + " <stack trace end>");

                // IF we get to this point, we have either a read lock, a writer lock, or both. Writer locks imply read locks, so
                // we need to take the read lock now.
                // Note that we always acquire the reader lock and then upgrade to a writer lock,  instead of acquiring the writer 
                // lock, so that we can downgrade if required later on.
                if (!readAlreadyTaken)
                {
                    try
                    {
                        Debug.WriteLine(Thread.CurrentThread.ManagedThreadId + " bladeLockCollection for blade " + _name + lockTypeName + " acquiring reader lock");
                        locksForThisBlade[lockTypeName].AcquireReaderLock(TimeSpan.FromSeconds(30));
                    }
                    catch (ApplicationException)
                    {
                        string msg = "Failed to acquire lock '" + lockTypeName + "' for read. \nRead locks are currently held by: ";
                        foreach (takenLockInfo info in _readTakenList[lockTypeName])
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
                    lock (_readTakenList) // fixme: ugh locking
                    {
                        _readTakenList[lockTypeName].Add(newInfo);
                    } 
                    _readLock = _readLock | (bladeLockType)lockBitMask;
                }

                if (writeRequested)
                {
                    try
                    {
                        lock (lockCookiesForThisBlade)
                        {
                            Debug.WriteLine(Thread.CurrentThread.ManagedThreadId + " bladeLockCollection for blade " + _name + lockTypeName + " UpgradeToWriterLock");
                            lockCookiesForThisBlade[lockTypeName] = locksForThisBlade[lockTypeName].UpgradeToWriterLock(TimeSpan.FromSeconds(10));
                        }
                    }
                    catch (ApplicationException)
                    {
                        string msg = "Failed to acquire lock '" + lockTypeName + "' for write. \nRead locks are currently held by: ";
                        foreach (takenLockInfo info in _readTakenList[lockTypeName])
                        {
                            if (info.threadID == Thread.CurrentThread.ManagedThreadId)
                                continue;
                            msg += " ***  Thread ID " + info.threadID + " allocated at " + info.stackTrace + "\n\n";
                        }
                        if (_writeTakenList[lockTypeName].threadID != -1)
                            msg += "\nWrite lock is currently held by thread ID " + _writeTakenList[lockTypeName].threadID + " allocated at " + _writeTakenList[lockTypeName].stackTrace;
                        throw new Exception(msg);
                    }

                    _writeTakenList[lockTypeName].threadID = Thread.CurrentThread.ManagedThreadId;
                    _writeTakenList[lockTypeName].stackTrace = Environment.StackTrace;
                    _writeLock = _writeLock | (bladeLockType)lockBitMask;
                }
            }
            Debug.WriteLine(Thread.CurrentThread.ManagedThreadId + " bladeLockCollection acquisition for blade " + _name + " finished, new access " + _readLock + " / " + _writeLock);
        }

        public void downgrade(bladeLockType toDropRead, bladeLockType toDropWrite)
        {
            release(toDropRead, toDropWrite);
        }

        public bool isUnlocked()
        {
            return _writeLock == bladeLockType.lockNone && _readLock == bladeLockType.lockNone;
        }

        public bool assertLocks(bladeLockType read, bladeLockType write)
        {
            if (_writeLock != write || _readLock != read) 
                return false;

            // If we're being asserted that no locks are held, also check they actually aren't.
            if (read == bladeLockType.lockNone && write == bladeLockType.lockNone)
            {
                foreach (string lockTypeName in getLockNames())
                {
                    locksForThisBlade[lockTypeName].AcquireReaderLock(TimeSpan.FromSeconds(1));
                    LockCookie foo = locksForThisBlade[lockTypeName].UpgradeToWriterLock(TimeSpan.FromSeconds(1));
                    locksForThisBlade[lockTypeName].ReleaseWriterLock();
                }
            }
            return true;
        }
    }
}