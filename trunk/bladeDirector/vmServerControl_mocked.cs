using hypervisors;

namespace bladeDirector
{
    public class vmServerControl_mocked : vmServerControl
    {
        public override void mountDataStore(hypervisor hyp, string dataStoreName, string serverName, string mountPath)
        {
            // TODO: store somewhere
        }
    }
}