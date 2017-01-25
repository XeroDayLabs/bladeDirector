using System;
using System.Collections.Concurrent;
using System.Text;
using System.Threading.Tasks;
using hypervisors;

namespace bladeDirectorClient
{
    public class hypervisorCollection : ConcurrentDictionary<string, bladeDirectedHypervisor_iLo>, IDisposable
    {
        public void Dispose()
        {
            foreach (bladeDirectedHypervisor_iLo hyp in this.Values)
                hyp.Dispose();
        }
    }
}
