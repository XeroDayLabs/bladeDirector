using System;
using System.Collections.Generic;
using System.Threading;

namespace bladeDirectorWCF
{
    public class biosReadWrite_mocked : IBiosReadWrite
    {
        private Dictionary<string, mockedBiosThreadParams> _threads = new Dictionary<string, mockedBiosThreadParams>();
        public TimeSpan biosOperationTime = TimeSpan.FromSeconds(0);

        public void cancelOperationsForBlade(string nodeIP)
        {
            _threads[nodeIP].isCancelled = true;
        }

        public result rebootAndStartWritingBIOSConfiguration(hostStateManager_core parent, string nodeIp, string biosxml)
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

                return new result(resultCode.pending, "Mocked BIOS thread created)");
            }
        }

        public result rebootAndStartReadingBIOSConfiguration(hostStateManager_core parent, string nodeIp)
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

                return new result(resultCode.success);
            }
        }

        public result checkBIOSOperationProgress(string bladeIp)
        {
            lock (_threads)
            {
                if (!_threads.ContainsKey(bladeIp))
                    return new result(resultCode.bladeNotFound, "No BIOS operation currently in progress");
                if (_threads[bladeIp].isFinished)
                    return _threads[bladeIp].result;

                return new result(resultCode.pending);
            }
        }

        public bool hasOperationStarted(string bladeIP)
        {
            lock (_threads)
            {
                if (_threads.ContainsKey(bladeIP) && _threads[bladeIP].isStarted)
                    return true;
                return false;
            }
        }

        private static void mockedBiosThread(Object param)
        {
            mockedBiosThreadParams paramTyped = (mockedBiosThreadParams) param;
            try
            {
                mockedBiosThread(paramTyped);
            }
            catch (Exception)
            {
                paramTyped.result = new result(resultCode.success);
                paramTyped.isFinished = true;
            }
        }

        private static void mockedBiosThread(mockedBiosThreadParams param)
        {
            using (lockableBladeSpec blade = param.db.getBladeByIP(param.nodeIP, bladeLockType.lockBIOS, bladeLockType.lockBIOS))
            {
                blade.spec.currentlyHavingBIOSDeployed = true;
            }
            param.isStarted = true;

            using (param.db.getBladeByIP(param.nodeIP, bladeLockType.lockLongRunningBIOS, bladeLockType.lockLongRunningBIOS))
            {
                while (true)
                {
                    if (DateTime.Now > param.deadline)
                    {
                        param.result = new result(resultCode.success);
                        param.isFinished = true;
                        return;
                    }

                    if (param.isCancelled)
                    {
                        param.result = new result(resultCode.cancelled);
                        param.isFinished = true;
                        return;
                    }

                    Thread.Sleep(TimeSpan.FromSeconds(1));
                }
            }
        }
    }
}