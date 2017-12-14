using System.Linq;
using hypervisors;

namespace bladeDirectorWCF
{
    public static class utils
    {
        public static hypervisor_iLo createHypForBlade(bladeSpec blade, snapshotDetails snap, NASParams nas)
        {
            userDesc usernameToUse = blade.credentials.First();

            return new hypervisor_iLo(new hypSpec_iLo(
                blade.bladeIP, usernameToUse.username, usernameToUse.password,
                blade.iLOIP, blade.iLoUsername, blade.iLoPassword,
                nas.IP, nas.username, nas.password,
                snap.friendlyName, snap.path,
                blade.kernelDebugPort, blade.kernelDebugKey));
        }

        public static hypervisor_vmware_FreeNAS createHypForVM(vmSpec vmSpec, bladeSpec vmServerSpec, snapshotDetails snapshotInfo, NASParams nas, clientExecutionMethod exec = clientExecutionMethod.smbWithWMI)
        {
            userDesc usernameToUse = vmSpec.credentials.First();

            return new hypervisor_vmware_FreeNAS(
                new hypSpec_vmware(
                    vmSpec.friendlyName, vmServerSpec.bladeIP, vmServerSpec.ESXiUsername, vmServerSpec.ESXiPassword,
                    usernameToUse.username, usernameToUse.password, snapshotInfo.friendlyName, snapshotInfo.path,
                    vmSpec.kernelDebugPort, vmSpec.kernelDebugKey, vmSpec.VMIP
                    ),
                nas.IP, nas.username, nas.password,
                exec);
        }
    }
}