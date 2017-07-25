namespace bladeDirectorWCF
{
    /// <summary>
    /// Impliment this to deploy a BIOS configuration to a hardware blade.
    /// </summary>
    public interface IBiosReadWrite
    {
        void cancelOperationsForBlade(string nodeIP);
        result rebootAndStartWritingBIOSConfiguration(hostStateManager_core parent, string nodeIp, string biosxml);
        result rebootAndStartReadingBIOSConfiguration(hostStateManager_core parent, string nodeIp);
        result checkBIOSOperationProgress(string bladeIp);

        /// <summary>
        /// This may only return after the relevant blade has had .isCurrentlyDeployingBIOS set to true.
        /// </summary>
        /// <param name="bladeIP"></param>
        /// <returns></returns>
        bool hasOperationStarted(string bladeIP);
    }
}