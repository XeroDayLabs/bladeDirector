using System;
using hypervisors;

namespace bladeDirectorWCF
{
    public abstract class mockedExecutionHandler
    {
        public abstract executionResult callMockedExecutionHandler(hypervisor sender, string command, string args, string workingdir, cancellableDateTime deadline);
    }
}