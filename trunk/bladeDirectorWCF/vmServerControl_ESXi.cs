using System;
using System.Linq;
using System.Threading;
using hypervisors;

namespace bladeDirectorWCF
{
    public class vmServerControl_ESXi : vmServerControl
    {
        public override void mountDataStore(hypervisor hyp, string srcAddress, string dataStoreName, string serverName, string mountPath, cancellableDateTime deadline)
        {
            while (ensureISCSIInterfaceIsSetUp(hyp, srcAddress) == false)
            {
                deadline.doCancellableSleep(TimeSpan.FromSeconds(5));
            }

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

        private bool ensureISCSIInterfaceIsSetUp(hypervisor hyp, string srcAddress)
        {
            executionResult res;

            // First, add our portgroup
            res = hypervisor.doWithRetryOnSomeExceptions(() => hyp.startExecutable("esxcli", "network vswitch standard portgroup add -p vlan11_iscsi -v vSwitch1"));
            if (res.resultCode != 0)
                return false;
            // And then give the portgroup an address.
            res = hypervisor.doWithRetryOnSomeExceptions(() => hyp.startExecutable(
                "esxcfg-vmknic", "--add --ip " + srcAddress + " --netmask 255.255.0.0 vlan11_iscsi"));
            if (res.resultCode != 0)
                return false;

            // Finally, tag it for management traffic. For this, we'll need to get the adaptor name for this portgroup.
            res = hypervisor.doWithRetryOnSomeExceptions(() => hyp.startExecutable( "esxcfg-vmknic", "--list"));
            if (res.resultCode != 0)
                return false;
            string[] lines = res.stdout.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
            string adaptorName = null;
            foreach (var line in lines)
            {
                string[] fields = line.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                if (fields[1] == "vlan11_iscsi")
                {
                    adaptorName = fields[0];
                    break;
                }
            }
            if (adaptorName == null)
                return false;

            res = hypervisor.doWithRetryOnSomeExceptions(() => hyp.startExecutable(
                "esxcli", "network ip interface tag add -i " + adaptorName + " -t Management"));
            if (res.resultCode != 0)
                return false;

            return true;
        }
    }
}