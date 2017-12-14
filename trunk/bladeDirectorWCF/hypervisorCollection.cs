using System;
using System.Collections.Concurrent;
using hypervisors;

namespace bladeDirectorWCF
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