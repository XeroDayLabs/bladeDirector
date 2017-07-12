using System;

namespace bladeDirector
{
    [Flags]
    public enum bladeLockType
    {
        lockAll = 0xffff,
        lockAllExceptLongRunning = ~0x1000,

        lockVMCreation = 0x01,
        lockBIOS = 0x02,
        lockSnapshot = 0x04,
        lockNASOperations = 0x08,
        lockOwnership = 0x10,
        lockVMDeployState = 0x20,

        lockLongRunningBIOS = 0x1000,
        lockNone = 0x0
    }
}