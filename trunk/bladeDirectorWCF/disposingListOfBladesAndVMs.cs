using System;

namespace bladeDirectorWCF
{
    public class disposingListOfBladesAndVMs : IDisposable
    {
        public disposingList<lockableBladeSpec> blades;
        public disposingList<lockableVMSpec> VMs;

        public void Dispose()
        {
            blades.Dispose();
            VMs.Dispose();
        }
    }
}