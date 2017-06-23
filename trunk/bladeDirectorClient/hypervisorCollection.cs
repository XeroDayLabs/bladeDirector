using System;
using System.Collections.Concurrent;
using System.Text;
using System.Threading.Tasks;
using bladeDirectorClient.bladeDirector;
using hypervisors;

namespace bladeDirectorClient
{
    public class hypervisorCollection<T> : ConcurrentDictionary<string, hypervisorWithSpec<T>>, IDisposable
    {
        public void Dispose()
        {
            foreach (hypervisorWithSpec<T> hyp in this.Values)
                hyp.Dispose();
        }
    }
}