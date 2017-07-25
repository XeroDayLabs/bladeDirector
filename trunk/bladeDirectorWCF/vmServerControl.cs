using hypervisors;

namespace bladeDirectorWCF
{
    public abstract class vmServerControl
    {
        public abstract void mountDataStore(hypervisor hyp, string dataStoreName, string serverName, string mountPath);
    }
}