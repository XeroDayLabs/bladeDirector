using System;

namespace bladeDirectorWCF
{
    [Flags]
    public enum bladeLockType
    {
        lockAll = 0xffff,

        lockVMCreation = 1 << 0,
        lockBIOS = 1 << 1,
        lockSnapshot = 1 << 2,
        lockNASOperations = 1 << 3,
        lockOwnership = 1 << 4,
        lockVMDeployState = 1 << 5,
        lockIPAddresses = 1 << 6,
        lockVirtualHW = 1 << 7,

        lockLongRunningBIOS = 0x1000,
        lockNone = 0x0,
    }
}