using System;
using System.Collections.Concurrent;

namespace bladeDirectorWCF
{
    public abstract class lockableOwnership : IDisposable
    {
        private readonly string IPAddress;
        private bladeLockType _readLocks;
        private bladeLockType _writeLocks;
        private static readonly ConcurrentDictionary<string, bladeLockCollection> _bladeLockStatus = new ConcurrentDictionary<string, bladeLockCollection>();
        public bool deleteOnRelease = false;

        /// <summary>
        /// We allow a '.Dispose' call to be ignored by use of this counter. 
        /// This is a little messy, but allows us to have nested using { .. } blocks.
        /// </summary>
        private int disposalInhibition = 0;

        private bool _isDisposed = false;

        public string allocationStack;

        protected bladeOwnership specOwnership { get; set; }

        public lockableOwnership(string bladeIP, bladeLockType readLocks, bladeLockType writeLocks)
        {
            IPAddress = bladeIP;
            _readLocks = readLocks;
            _writeLocks = writeLocks;
            specOwnership = null;
            allocationStack = Environment.StackTrace;

            acquire();
        }
        
        private void acquire()
        {
            // it's probably already there already, and since we never remove from this collection, we can just check using
            // .ContainsKey before we add in a thread-safe fashion.
            if (_bladeLockStatus.ContainsKey(IPAddress))
            {
                _bladeLockStatus[IPAddress].acquire(_readLocks, _writeLocks);
                return;
            }

            bladeLockCollection newLock = new bladeLockCollection(IPAddress, _readLocks, _writeLocks);
            if (!_bladeLockStatus.TryAdd(IPAddress, newLock))
            {
                // Oh, someone added it already. Just use the one already there.
                _bladeLockStatus[IPAddress].acquire(_readLocks, _writeLocks);
            }
        }
        
        public void upgradeLocks(bladeLocks locks)
        {
            upgradeLocks(locks.read, locks.write);
        }

        public void upgradeLocks(bladeLockType readToAdd, bladeLockType writeToAdd)
        {
            if (_isDisposed)
                return;

            _bladeLockStatus[specOwnership.kernelDebugAddress].acquire(readToAdd, writeToAdd);
            _readLocks |= readToAdd | writeToAdd;
            _writeLocks |= writeToAdd;
            specOwnership.notifyOfUpgradedLocks(readToAdd, writeToAdd);
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
            if (_isDisposed)
                return;

            specOwnership.notifyOfDowngradedLocks(readToRelease, writeToRelease);

            _readLocks = bladeLockCollection.clearField(_readLocks, readToRelease);
            _writeLocks = bladeLockCollection.clearField(_writeLocks, writeToRelease);
            
            // Set any read locks that are being downgraded (ie, writes that we not also releasing reads of)
            _readLocks |= writeToRelease & (~readToRelease);

            _bladeLockStatus[specOwnership.kernelDebugAddress].downgrade(readToRelease, writeToRelease);
        }

        public void Dispose()
        {
            if (specOwnership != null)
            {
                if (disposalInhibition != 0)
                {
                    disposalInhibition--;
                }
                else
                {
                    if (deleteOnRelease)
                        specOwnership.deleteInDB();
                    else
                        specOwnership.createOrUpdateInDB();

                    _bladeLockStatus[specOwnership.kernelDebugAddress].release(_readLocks, _writeLocks);
                }
            }

            _isDisposed = true;
            GC.SuppressFinalize(this);
        }

        ~lockableOwnership()
        {
            throw new bladeLockExeception("lockableOwnership for " + specOwnership.kernelDebugAddress + " was never released! Allocation stack trace: " + allocationStack);
        }

        public void inhibitNextDisposal()
        {
            disposalInhibition++;
        }

        public void setSpec(bladeOwnership newSpec)
        {
            if (specOwnership != null)
                throw new Exception();

            if (newSpec.kernelDebugAddress != IPAddress)
                throw new Exception();

            if (newSpec.permittedAccessRead != _readLocks ||
                newSpec.permittedAccessWrite != _writeLocks)
                throw new Exception();

            specOwnership = newSpec;
        }    
    }

    public abstract class lockableOwnershipWithTypeInformation<T> : lockableOwnership where T : bladeOwnership
    {
        protected lockableOwnershipWithTypeInformation(string bladeIP, bladeLockType readLocks, bladeLockType writeLocks)
            : base(bladeIP, readLocks, writeLocks)
        {
        }

        public T spec
        {
            get { return (T)base.specOwnership; }
            set { base.specOwnership = value; }
        }
    }

    public class lockableVMSpec : lockableOwnershipWithTypeInformation<vmSpec>
    {
        public lockableVMSpec(string bladeIP, bladeLockType readLocks, bladeLockType writeLocks) :
            base(bladeIP, readLocks, writeLocks) { }
    }

    public class lockableBladeSpec : lockableOwnershipWithTypeInformation<bladeSpec>
    {
        public lockableBladeSpec(string bladeIP, bladeLockType readLocks, bladeLockType writeLocks) :
            base(bladeIP, readLocks, writeLocks) { }
    }

    public class bladeLocks
    {
        public bladeLockType read;
        public bladeLockType write;
    }
}