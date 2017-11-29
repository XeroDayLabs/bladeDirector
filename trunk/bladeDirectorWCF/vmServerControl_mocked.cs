using hypervisors;

namespace bladeDirectorWCF
{
    public class vmServerControl_mocked : vmServerControl
    {
        public override void mountDataStore(hypervisor hyp, string vmServerBladeIpAddressISCSI, string dataStoreName, string serverName, string mountPath)
        {
            // TODO: store somewhere
        }
    }
}