using System;
using System.Collections.Generic;
using System.Threading;

namespace bladeDirector
{
    public class biosReadWrite_mocked : IBiosReadWrite
    {
        private Dictionary<string, mockedBiosThreadParams> _threads = new Dictionary<string, mockedBiosThreadParams>();
        public TimeSpan biosOperationTime = TimeSpan.FromSeconds(0);

        public void cancelOperationsForBlade(string nodeIP)
        {
            _threads[nodeIP].isCancelled = true;
        }

        public resultCode rebootAndStartWritingBIOSConfiguration(hostStateManager_core parent, string nodeIp, string biosxml)
        {
            lock (_threads)
            {
                if (_threads.ContainsKey(nodeIp))
                {
                    if (!_threads[nodeIp].isFinished)
                        throw new Exception("Blade " + nodeIp + " has not yet finished a previous operation");
                    _threads.Remove(nodeIp);
                }

                mockedBiosThreadParams newParams = new mockedBiosThreadParams
                {
                    bladeLockedEvent = new ManualResetEvent(false),
                    isFinished = false,
                    nodeIP = nodeIp,
                    db = parent.db,
                    deadline = DateTime.Now + biosOperationTime
                };
                _threads.Add(nodeIp, newParams);

                Thread newThread = new Thread(mockedBiosThread)
                {
                    Name = "Mocked BIOS deployment thread"
                };
                newThread.Start(newParams);
                newParams.bladeLockedEvent.WaitOne();

                return resultCode.success;
            }
        }

        public resultCode rebootAndStartReadingBIOSConfiguration(hostStateManager_core parent, string nodeIp)
        {
            lock (_threads)
            {
                if (_threads.ContainsKey(nodeIp))
                {
                    if (!_threads[nodeIp].isFinished)
                        throw new Exception("Blade " + nodeIp + " has not yet finished a previous operation");
                    _threads.Remove(nodeIp);
                }
                mockedBiosThreadParams newParams = new mockedBiosThreadParams
                {
                    bladeLockedEvent = new ManualResetEvent(false),
                    isFinished = false,
                    nodeIP = nodeIp,
                    db = parent.db,
                    deadline = DateTime.Now + biosOperationTime
                };
                _threads.Add(nodeIp, newParams);

                Thread newThread = new Thread(mockedBiosThread)
                {
                    Name = "Mocked BIOS deployment thread"
                };
                newThread.Start(newParams);
                newParams.bladeLockedEvent.WaitOne();

                return resultCode.success;
            }
        }

        public resultCode checkBIOSOperationProgress(string bladeIp)
        {
            if (_threads[bladeIp].isFinished)
            {
                return _threads[bladeIp].result;
            }
            return resultCode.pending;
        }

        private static void mockedBiosThread(Object param)
        {
            mockedBiosThread((mockedBiosThreadParams)param);
        }

        private static void mockedBiosThread(mockedBiosThreadParams param)
        {
            using (param.db.getBladeByIP(param.nodeIP, bladeLockType.lockLongRunningBIOS))
            {
                param.bladeLockedEvent.Set();
                while (true)
                {
                    if (DateTime.Now > param.deadline)
                    {
                        param.result = resultCode.success;
                        param.isFinished = true;
                    }

                    if (param.isCancelled)
                    {
                        param.result = resultCode.cancelled;
                        param.isFinished = true;
                        return;
                    }

                    Thread.Sleep(TimeSpan.FromSeconds(1));
                }
            }
        }
    }
}