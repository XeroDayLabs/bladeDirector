using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;

namespace bladeDirectorWCF
{
    public interface ILockableSpec
    {
        bladeLocks getCurrentLocks();
        void downgradeLocks(bladeLocks locks);
        void upgradeLocks(bladeLocks locks);
        void upgradeLocks(bladeLockType readToAdd, bladeLockType writeToAdd);
        void downgradeLocks(bladeLockType readToRelease, bladeLockType writeToRelease);
    }

    public class lockableVMSpec : IDisposable, ILockableSpec
    {
        private readonly string _VMIP;
        private bladeLockType _readLocks;
        private bladeLockType _writeLocks;
        private static readonly ConcurrentDictionary<string, bladeLockCollection> _bladeLockStatus = new ConcurrentDictionary<string, bladeLockCollection>();
        public bool deleteOnRelease = false;
        private int disposalInhibition = 0;

        public string allocationStack;

        public vmSpec spec { get; private set; }

        public lockableVMSpec(string bladeIP, bladeLockType readLocks, bladeLockType writeLocks)
        {
            _VMIP = bladeIP;
            _readLocks = readLocks;
            _writeLocks = writeLocks;
            spec = null;
            allocationStack = Environment.StackTrace;

            acquire();
        }
        
        private void acquire()
        {
            // it's probably already there already, and since we never remove from this collection, we can just check using
            // .ContainsKey before we add in a thread-safe fashion.
            if (_bladeLockStatus.ContainsKey(_VMIP))
            {
                _bladeLockStatus[_VMIP].acquire(_readLocks, _writeLocks);
                return;
            }

            bladeLockCollection newLock = new bladeLockCollection(_VMIP, _readLocks, _writeLocks);
            if (!_bladeLockStatus.TryAdd(_VMIP, newLock))
            {
                // Oh, someone added it already. Just use the one already there.
                _bladeLockStatus[_VMIP].acquire(_readLocks, _writeLocks);
            }
        }

        public void upgradeLocks(bladeLocks locks)
        {
            upgradeLocks(locks.read, locks.write);
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

        public void downgradeLocks(bladeLocks locks)
        {
            downgradeLocks(locks.read, locks.write);
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
            if (spec != null)
            {
                if (disposalInhibition != 0)
                {
                    disposalInhibition--;
                }
                else
                {
                    if (deleteOnRelease)
                        spec.deleteInDB();
                    else
                        spec.createOrUpdateInDB();

                    _bladeLockStatus[spec.VMIP].release(_readLocks, _writeLocks);
                }
            }

            GC.SuppressFinalize(this);
        }

        ~lockableVMSpec()
        {
            throw new bladeLockExeception("lockableVMSpec for VM " + _VMIP + " was never released! Allocation stack trace: " + allocationStack);
        }

        public void inhibitNextDisposal()
        {
            disposalInhibition++;
        }

        public void setSpec(vmSpec newSpec)
        {
            if (spec != null)
                throw new Exception();

            if (newSpec.VMIP != _VMIP)
                throw new Exception();

            if (newSpec.permittedAccessRead != _readLocks ||
                newSpec.permittedAccessWrite != _writeLocks)
                throw new Exception();

            spec = newSpec;
        }
    }

    public class bladeLocks
    {
        public bladeLockType read;
        public bladeLockType write;
    }
}