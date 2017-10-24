using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Diagnostics;

namespace bladeDirectorWCF
{
    public class lockableVMSpec : IDisposable
    {
        private bladeLockType _readLocks;
        private bladeLockType _writeLocks;
        private static Dictionary<string, bladeLockCollection> _bladeLockStatus = new Dictionary<string, bladeLockCollection>();
        public bool deleteOnRelease = false;
        private int disposalInhibition = 0;

        public vmSpec spec { get; private set; }

        public lockableVMSpec(SQLiteConnection conn, SQLiteDataReader reader, bladeLockType readLocks, bladeLockType writeLocks)
        {
            _readLocks = readLocks;
            _writeLocks = writeLocks;
            spec = new vmSpec(conn, reader, readLocks, writeLocks);

            acquire();

            // Re-read, in case we blocked before.
            // lol what, this won't re-query the db, FIXME/TODO
            spec = new vmSpec(conn, reader, readLocks, writeLocks);
        }

        public lockableVMSpec(SQLiteConnection conn, vmSpec newVm)
        {
            spec = newVm;

            acquire();
        }

        private void acquire()
        {
            lock (_bladeLockStatus)
            {
                Debug.WriteLine("Node " + spec.VMIP + " enter ");

                if (!_bladeLockStatus.ContainsKey(spec.VMIP))
                {
                    _bladeLockStatus.Add(spec.VMIP, new bladeLockCollection(spec.VMIP,_readLocks, _writeLocks));
                }
                else
                {
                    _bladeLockStatus[spec.VMIP].acquire(_readLocks, _writeLocks);
                }
            }
        }

        public void upgradeLocks(bladeLockType readToAdd, bladeLockType writeToAdd)
        {
            _bladeLockStatus[spec.VMIP].acquire(readToAdd, writeToAdd);
            _readLocks |= readToAdd | writeToAdd;
            _writeLocks |= writeToAdd;
            spec.notifyOfUpgradedLocks(readToAdd, writeToAdd);
        }

        public bladeLocks getCurrentLocks()
        {
            bladeLocks toRet = new bladeLocks();
            toRet.read = _readLocks;
            toRet.write= _writeLocks;
            return toRet;
        }

        public void downgradeLocks(bladeLockType readToRelease, bladeLockType writeToRelease)
        {
            spec.notifyOfDowngradedLocks(readToRelease, writeToRelease);

            _readLocks = bladeLockCollection.clearField(_readLocks, readToRelease);
            _writeLocks = bladeLockCollection.clearField(_writeLocks, writeToRelease);
            
            // Set any read locks that are being downgraded (ie, writes that we not also releasing reads of)
            _readLocks |= writeToRelease & (~readToRelease);
            
            _bladeLockStatus[spec.VMIP].downgrade(readToRelease, writeToRelease);
        }

        public void Dispose()
        {
            lock (_bladeLockStatus)
            {
                if (spec != null)
                {
                    if (disposalInhibition == 0)
                    {
                        if (deleteOnRelease)
                        {
                            spec.deleteInDB();
                        }
                        else
                        {
                            spec.createOrUpdateInDB();
                        }
                        _bladeLockStatus[spec.VMIP].release(_readLocks, _writeLocks);
                    }
                    else
                    {
                        disposalInhibition--;
                    }
                }
                GC.SuppressFinalize(this);
            }
        }

        ~lockableVMSpec()
        {
            Debug.Fail("lockableVMSpec for VM " + spec.VMIP + " was never released" );
        }

        public void inhibitNextDisposal()
        {
            disposalInhibition++;
        }
    }

    public class bladeLocks
    {
        public bladeLockType read;
        public bladeLockType write;
    }
}