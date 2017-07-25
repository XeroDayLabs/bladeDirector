using System;
using System.Collections.Concurrent;
using System.Data.SQLite;
using System.Diagnostics;
using System.Threading;

namespace bladeDirectorWCF
{
    public class lockableBladeSpec : IDisposable
    {
        private readonly SQLiteConnection _conn;
        private readonly string _ip;
        private static ConcurrentDictionary<string, bladeLockCollection> bladeLockStatus = new ConcurrentDictionary<string, bladeLockCollection>();
        private bladeLockType _lockTypeRead;
        private bladeLockType _lockTypeWrite;
        private int disposalInhibition;

        public bladeSpec spec { get; private set; }
        
        public lockableBladeSpec(SQLiteConnection conn, string IP, bladeLockType lockTypeRead, bladeLockType lockTypeWrite)
        {
            _lockTypeRead = lockTypeRead;
            _lockTypeWrite = lockTypeWrite;
            _conn = conn;
            _ip = IP;
            spec = null;
            init(_ip);
        }

        public lockableBladeSpec(SQLiteConnection conn, SQLiteDataReader reader, bladeLockType lockTypeRead, bladeLockType lockTypeWrite)
        {
            _lockTypeRead = lockTypeRead;
            _lockTypeWrite = lockTypeWrite;
            _conn = conn;
            spec = new bladeSpec(conn, reader, bladeLockType.lockNone, bladeLockType.lockNone);
            init(spec.bladeIP);
            // Now do the real DB read, since we have the lock
            // FIXME: this won't re-read the DB
            spec = new bladeSpec(conn, reader, lockTypeRead, lockTypeWrite);
            Debug.WriteLine("OK, blade " + spec.bladeIP + " read lock (" + _lockTypeRead + ") write lock (" + _lockTypeWrite + ") now owned by thread " + Thread.CurrentThread.ManagedThreadId + " ('" + Thread.CurrentThread.Name + ")");
            Debug.WriteLine(Environment.StackTrace);
        }

        private void init(string IP)
        {
            Debug.WriteLine("Attempting to lock blade " + IP);

            bladeLockCollection tryAddThis = new bladeLockCollection(IP, _lockTypeRead, _lockTypeWrite);
            if (bladeLockStatus.TryAdd(IP, tryAddThis))
            {
                // Successfully added the lock collection, which is set to the locks we desire.

                // We can return now.
                Debug.WriteLine("OK, blade " + IP + " read lock " + _lockTypeRead + " now owned by thread " + Thread.CurrentThread.ManagedThreadId + " ('" + Thread.CurrentThread.Name + ")");
                Debug.WriteLine(Environment.StackTrace);
                return;
            }

            // Otherwise, the mutex already exists in the concurrentDictionary. Firstly, dispose the mutex we tried to add to the
            // dictionary, since it is now unneccessary.
            tryAddThis.release(_lockTypeRead, _lockTypeWrite);

            // and now lock the mutexes in the dictionary. Note that, since mutexes are never removed from the dict, we don't have
            // to re-check the dictionary.
            bladeLockStatus.GetOrAdd(IP, (bladeLockCollection) null).acquire(_lockTypeRead, _lockTypeWrite);
            Debug.WriteLine("OK, blade " + IP + " read lock (" + _lockTypeRead + ") write lock (" + _lockTypeWrite + ") now owned by thread " + Thread.CurrentThread.ManagedThreadId + " ('" + Thread.CurrentThread.Name + ")");
        }

        public lockableBladeSpec()
        {
            spec = null;
        }

        public void Dispose()
        {
            if (spec != null)
            {
                if (disposalInhibition == 0)
                {
                    spec.createOrUpdateInDB();
                    Debug.WriteLine("About to release blade " + spec.bladeIP + " read lock " + _lockTypeRead + " write lock " + _lockTypeWrite + " from ownership by by thread " + Thread.CurrentThread.ManagedThreadId + " ('" + Thread.CurrentThread.Name + ")");
                    bladeLockStatus.GetOrAdd(spec.bladeIP, (bladeLockCollection)null).release(_lockTypeRead, _lockTypeWrite);
                }
                else
                {
                    disposalInhibition--;
                }
            }
            GC.SuppressFinalize(this);
        }

        public void upgradeLocks(bladeLockType readToAdd, bladeLockType writeToAdd)
        {
            bladeLockStatus[spec.bladeIP].acquire(readToAdd, writeToAdd);
            _lockTypeRead |= readToAdd | writeToAdd;
            _lockTypeWrite |= writeToAdd;
            spec.notifyOfUpgradedLocks(readToAdd, writeToAdd);
        }

        public void downgradeLocks(bladeLockType readToRelease, bladeLockType writeToRelease)
        {
            spec.notifyOfDowngradedLocks(readToRelease, writeToRelease);

            _lockTypeRead = bladeLockCollection.clearField(_lockTypeRead, readToRelease);
            _lockTypeWrite = bladeLockCollection.clearField(_lockTypeWrite, writeToRelease);

            // Set any read locks that are being downgraded (ie, writes that we not also releasing reads of)
            _lockTypeRead |= writeToRelease & (~readToRelease);

            bladeLockStatus[spec.bladeIP].downgrade(readToRelease, writeToRelease);
        }

        ~lockableBladeSpec()
        {
            if (spec != null)
            {
                Debug.Fail("lockableBladeSpec for blade " + spec.bladeIP + " with lock " + _lockTypeRead + " was never released");
            }
        }

        public void inhibitNextDisposal()
        {
            disposalInhibition++;
        }

        public void setSpec(bladeSpec newSpec)
        {
            if (spec != null)
                throw new Exception();

            if (newSpec.bladeIP != _ip)
                throw new Exception();

            if (newSpec.permittedAccessRead != _lockTypeRead ||
                newSpec.permittedAccessWrite != _lockTypeWrite)
                throw new Exception();

            spec = newSpec;
        }
    }
}