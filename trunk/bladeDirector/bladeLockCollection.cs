using System;
using System.Collections.Generic;
using System.Threading;

namespace bladeDirector
{
    public class bladeLockCollection
    {
        private Dictionary<string, Mutex> _mutexes = new Dictionary<string, Mutex>();
        private Dictionary<string, int> _mutexLastTakenList = new Dictionary<string, int>(); 

        public bladeLockCollection(bladeLockType typesToLock)
        {
            foreach (string lockTypeName in Enum.GetNames(typeof (bladeLockType)))
            {
                if (lockTypeName.StartsWith("lockAll") | lockTypeName == "lockNone")
                    continue;

                _mutexes.Add(lockTypeName, new Mutex(false));
                _mutexLastTakenList.Add(lockTypeName, -1);
            }

            acquire(typesToLock);
        }

        public void release(bladeLockType typesToLock)
        {
            foreach (string lockTypeName in Enum.GetNames(typeof (bladeLockType)))
            {
                if (lockTypeName.StartsWith("lockAll") | lockTypeName == "lockNone")
                    continue;
                
                if (((int)typesToLock & (int)Enum.Parse(typeof(bladeLockType), lockTypeName)) != 0)
                {
                    _mutexLastTakenList[lockTypeName] = -1;
                    _mutexes[lockTypeName].ReleaseMutex();
                }
            }
        }

        public void acquire(bladeLockType typesToLock)
        {
            foreach (string lockTypeName in Enum.GetNames(typeof(bladeLockType)))
            {
                if (lockTypeName.StartsWith("lockAll") | lockTypeName == "lockNone")
                    continue;

                if (((int)typesToLock & (int)Enum.Parse(typeof(bladeLockType), lockTypeName)) != 0)
                {
                    while (true)
                    {
                        if (_mutexLastTakenList[lockTypeName] == Thread.CurrentThread.ManagedThreadId)
                            throw new Exception("this thread already has this lock");

                        if (_mutexes[lockTypeName].WaitOne(TimeSpan.FromSeconds(5)))
                        {
                            _mutexLastTakenList[lockTypeName] = Thread.CurrentThread.ManagedThreadId;
                            break;
                        }
                        Console.WriteLine("Still waiting on lock type " + lockTypeName);
                    }
                }
            }
        }
    }
}