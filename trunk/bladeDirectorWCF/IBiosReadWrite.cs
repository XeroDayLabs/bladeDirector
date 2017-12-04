using System.Threading;

namespace bladeDirectorWCF
{
    /// <summary>
    /// Impliment this to deploy a BIOS configuration to a hardware blade.
    /// </summary>
    public interface IBiosReadWrite
    {
        void cancelOperationsForBlade(string nodeIP);
        result rebootAndStartWritingBIOSConfiguration(hostStateManager_core parent, string nodeIp, string biosxml, ManualResetEvent signalOnStartComplete);
        result rebootAndStartReadingBIOSConfiguration(hostStateManager_core parent, string nodeIp, ManualResetEvent signalOnStartComplete);
        result checkBIOSOperationProgress(string bladeIp);
    }
}