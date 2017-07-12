using System;
using System.Collections.Concurrent;
using System.Data.SQLite;
using System.Diagnostics;
using System.Threading;

namespace bladeDirector
{
    public class lockableBladeSpec : IDisposable
    {
        private readonly SQLiteConnection _conn;
        private readonly string _ip;
        private static ConcurrentDictionary<string, bladeLockCollection> bladeLockStatus = new ConcurrentDictionary<string, bladeLockCollection>();
        private bladeLockType _lockType;
        private int disposalInhibition;

        public bladeSpec spec { get; private set; }


        public lockableBladeSpec(SQLiteConnection conn, string IP, bladeLockType lockType)
        {
            _lockType = lockType;
            _conn = conn;
            _ip = IP;
            spec = null;
            init(_ip);
        }

        public lockableBladeSpec(SQLiteConnection conn, SQLiteDataReader reader, bladeLockType lockType)
        {
            _lockType = lockType;
            _conn = conn;
            spec = new bladeSpec(reader, bladeLockType.lockNone);
            init(spec.bladeIP);
            // Now do the real DB read, since we have the lock
            spec = new bladeSpec(reader, lockType);
            Debug.WriteLine("OK, blade " + spec.bladeIP + " lock " + lockType + " now owned by thread " + Thread.CurrentThread.ManagedThreadId + " ('" + Thread.CurrentThread.Name + ")");
            Debug.WriteLine(Environment.StackTrace);
        }

        private void init(string IP)
        {
            Debug.WriteLine("Attempting to lock blade " + IP);

            bladeLockCollection tryAddThis = new bladeLockCollection(_lockType);
            //Mutex tryAddThis = new Mutex(true);
            if (bladeLockStatus.TryAdd(IP, tryAddThis))
            {
                // Successfully added the lock collection, which is set to the locks we desire. We can return now.
                Debug.WriteLine("OK, blade " + IP + " lock " + _lockType + " now owned by thread " + Thread.CurrentThread.ManagedThreadId + " ('" + Thread.CurrentThread.Name + ")");
                Debug.WriteLine(Environment.StackTrace);
                return;
            }

            // Otherwise, the mutex already exists in the concurrentDictionary. Firstly, dispose the mutex we tried to add to the
            // dictionary, since it is now unneccessary.
            tryAddThis.release(_lockType);

            // and now lock the mutexes in the dictionary. Note that, since mutexes are never removed from the dict, we don't have
            // to re-check the dictionary.
            bladeLockStatus.GetOrAdd(IP, (bladeLockCollection) null).acquire(_lockType);
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
                    spec.updateInDB(_conn);
                    Debug.WriteLine("About to release blade " + spec.bladeIP + " lock " + _lockType + " from ownership by by thread " + Thread.CurrentThread.ManagedThreadId + " ('" + Thread.CurrentThread.Name + ")");
                    bladeLockStatus.GetOrAdd(spec.bladeIP, (bladeLockCollection) null).release(_lockType);
                }
                else
                {
                    disposalInhibition--;
                }
            }
            GC.SuppressFinalize(this);
        }

        ~lockableBladeSpec()
        {
            if (spec != null)
            {
                Debug.Fail("lockableBladeSpec for blade " + spec.bladeIP + " with lock " + _lockType + " was never released");
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

            if (newSpec.permittedAccess != _lockType)
                throw new Exception();

            spec = newSpec;
        }
    }
}