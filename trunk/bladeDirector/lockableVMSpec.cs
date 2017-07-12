using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Diagnostics;
using System.Threading;

namespace bladeDirector
{
    public class lockableVMSpec : IDisposable
    {
        private SQLiteConnection _conn;
        private static Dictionary<string, Mutex> _bladeLockStatus = new Dictionary<string, Mutex>();
        private static Dictionary<string, int> _bladeLockThreads = new Dictionary<string, int>();
        public bool deleteOnRelease = false;

        public vmSpec spec { get; private set; }

        public lockableVMSpec(SQLiteConnection conn, SQLiteDataReader reader)
        {
            _conn = conn;
            spec = new vmSpec(reader);

            takeMutex();

            // Re-read, in case we blocked before.
            spec = new vmSpec(reader);
        }

        private void takeMutex()
        {
            lock (_bladeLockStatus)
            {
                Debug.WriteLine("Node " + spec.VMIP + " enter ");

                if (!_bladeLockStatus.ContainsKey(spec.VMIP))
                {
                    _bladeLockStatus.Add(spec.VMIP, new Mutex(true));
                    _bladeLockThreads.Add(spec.VMIP, Thread.CurrentThread.ManagedThreadId);
//                    Debug.WriteLine("OK, VM " + spec.VMIP + " now owned by thread " + Thread.CurrentThread.ManagedThreadId + " ('" + Thread.CurrentThread.Name + "')");
//                    Debug.WriteLine(Environment.StackTrace);
                    return;
                }
                _bladeLockStatus[spec.VMIP].WaitOne();
                if (_bladeLockThreads[spec.VMIP] == Thread.CurrentThread.ManagedThreadId)
                {
                    throw new Exception("this thread already has this VM lock");
                }
//                Debug.WriteLine("OK, VM " + spec.VMIP + " now owned by thread " + Thread.CurrentThread.ManagedThreadId + " ('" + Thread.CurrentThread.Name + "')");
//                Debug.WriteLine(Environment.StackTrace);
            }
        }

        public lockableVMSpec(SQLiteConnection conn, vmSpec newVm)
        {
            spec = newVm;
            _conn = conn;
            takeMutex();
        }

        public void Dispose()
        {
            lock (_bladeLockStatus)
            {
                if (spec != null)
                {
                    if (_conn != null)
                    {
                        if (deleteOnRelease)
                        {
                            spec.deleteInDB(_conn);
                        }
                        else
                        {
                            spec.updateInDB(_conn);
                        }
//                        Debug.WriteLine("Releasing " + spec.VMIP + " from thread " + Thread.CurrentThread.ManagedThreadId + " ('" + Thread.CurrentThread.Name + "')");
//                        Debug.WriteLine(Environment.StackTrace);
                        _bladeLockThreads[spec.VMIP] = -1;
                        _bladeLockStatus[spec.VMIP].ReleaseMutex();
                    }
                }
                GC.SuppressFinalize(this);
            }
        }

        ~lockableVMSpec()
        {
            Debug.Fail("lockableVMSpec for VM " + spec.VMIP + " was never released");
        }
    }
}