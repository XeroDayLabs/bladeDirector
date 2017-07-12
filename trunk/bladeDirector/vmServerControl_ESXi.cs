using System;
using System.Linq;
using hypervisors;

namespace bladeDirector
{
    public class vmServerControl_ESXi : vmServerControl
    {
        public override void mountDataStore(hypervisor hyp, string dataStoreName, string serverName, string mountPath)
        {
            string expectedLine = String.Format("{0} is {1} from {2} mounted available", dataStoreName, mountPath, serverName);

            string[] nfsMounts = hypervisor.doWithRetryOnSomeExceptions(() => hyp.startExecutable("esxcfg-nas", "-l")).stdout.Split(new[] { "\n", "\r" }, StringSplitOptions.RemoveEmptyEntries);
            string foundMount = nfsMounts.SingleOrDefault(x => x.Contains(expectedLine));
            while (foundMount == null)
            {
                hypervisor.doWithRetryOnSomeExceptions(() => hyp.startExecutable("esxcfg-nas", "-d " + dataStoreName));
                hypervisor.doWithRetryOnSomeExceptions(() => hyp.startExecutable("esxcfg-nas", "-a --host " + serverName + " --share " + mountPath + " " + dataStoreName));
                hypervisor.doWithRetryOnSomeExceptions(() => hyp.startExecutable("esxcfg-rescan", "--all"));

                nfsMounts = hypervisor.doWithRetryOnSomeExceptions(() => hyp.startExecutable("esxcfg-nas", "-l")).stdout.Split(new[] { "\n", "\r" }, StringSplitOptions.RemoveEmptyEntries);
                foundMount = nfsMounts.SingleOrDefault(x => x.Contains(expectedLine));
            }
        }
    }
}