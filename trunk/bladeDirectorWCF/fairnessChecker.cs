using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace bladeDirectorWCF
{
    public abstract class fairnessChecker
    {
        public static fairnessChecker create(fairnessType type)
        {
            switch (type)
            {
                case fairnessType.fair:
                    return new fairness_fair();
                case fairnessType.allowAny:
                    return new fairness_allowAny();
                default:
                    throw new ArgumentOutOfRangeException("type", type, null);
            }
        }

        public enum fairnessType
        {
            fair,
            allowAny
        }

        public abstract void checkFairness_blades(hostDB db, disposingList<lockableBladeSpec> blades);
    }

    public class fairness_allowAny : fairnessChecker
    {
        public override void checkFairness_blades(hostDB db, disposingList<lockableBladeSpec> blades)
        {
            // If anyone is queued, promote them.
            foreach (lockableBladeSpec blade in blades)
            {
                if (blade.spec.currentlyBeingAVMServer)
                {
                    using (disposingList<lockableVMSpec> childVMs = db.getVMByVMServerIP(blade,
                        bladeLockType.lockNone, bladeLockType.lockOwnership))
                    {
                        foreach (lockableVMSpec VM in childVMs)
                        {
                            Debug.WriteLine("Requesting release for VM " + VM.spec.VMIP);
                            VM.spec.state = bladeStatus.releaseRequested;
                        }
                    }
                }
                else
                {
                    if (blade.spec.currentOwner != "vmserver" && blade.spec.nextOwner != null)
                    {
                        Debug.WriteLine("Requesting release for blade " + blade.spec.bladeIP);
                        blade.spec.state = bladeStatus.releaseRequested;
                    }
                }
            }
        }
    }

    public class fairness_fair : fairnessChecker
    {
        public override void checkFairness_blades(hostDB db, disposingList<lockableBladeSpec> blades)
        {
            // If a blade owner is under its quota, then promote it in any queues where the current owner is over-quota.
            currentOwnerStat[] stats = db.getFairnessStats(blades);
            string[] owners = stats.Where(x => x.ownerName != "vmserver").Select(x => x.ownerName).ToArray();
            if (owners.Length == 0)
                return;
            float fairShare = (float)db.getAllBladeIP().Length / (float)owners.Length;

            currentOwnerStat[] ownersOverQuota = stats.Where(x => x.allocatedBlades > fairShare).ToArray();
            List<currentOwnerStat> ownersUnderQuota = stats.Where(x => x.allocatedBlades < fairShare).ToList();

            foreach (currentOwnerStat migrateTo in ownersUnderQuota)
            {
                var migratory = blades.Where(x =>
                    (
                        // Migrate if the dest is currently owned by someone over-quota
                        (ownersOverQuota.Count(y => y.ownerName == x.spec.currentOwner) > 0) ||
                        // Or if it is a VM server, and currently holds VMs that are _all_ allocated to over-quota users
                        (
                            x.spec.currentOwner == "vmserver" &&

                            db.getVMByVMServerIP_nolocking(x.spec.bladeIP).All(vm =>
                                (ownersOverQuota.Count(overQuotaUser => overQuotaUser.ownerName == vm.currentOwner) > 0)
                                )
                            )
                        )
                    &&
                    x.spec.nextOwner == migrateTo.ownerName &&
                    (x.spec.state == bladeStatus.inUse || x.spec.state == bladeStatus.inUseByDirector)).ToList();
                {
                    if (migratory.Count == 0)
                    {
                        // There is nowhere to migrate this owner from. Try another owner.
                        continue;
                    }

                    // Since migration cannot fail, we just take the first potential.
                    // TODO: should we prefer non VM-servers here?
                    lockableBladeSpec newHost = migratory.First();

                    if (newHost.spec.currentlyBeingAVMServer)
                    {
                        // It's a VM server. Migrate all the VMs off it (ie, request them to be destroyed).
                        newHost.spec.nextOwner = migrateTo.ownerName;
                        using (disposingList<lockableVMSpec> childVMs = db.getVMByVMServerIP(newHost,
                            bladeLockType.lockNone, bladeLockType.lockOwnership))
                        {
                            foreach (lockableVMSpec VM in childVMs)
                            {
                                Debug.WriteLine("Requesting release for VM " + VM.spec.VMIP);
                                VM.spec.state = bladeStatus.releaseRequested;
                            }
                        }
                        newHost.spec.nextOwner = migrateTo.ownerName;
                        newHost.spec.state = bladeStatus.releaseRequested;
                    }
                    else
                    {
                        // It's a physical server. Just mark it as .releaseRequested.
                        Debug.WriteLine("Requesting release for blade " + newHost.spec.bladeIP);
                        newHost.spec.nextOwner = migrateTo.ownerName;
                        newHost.spec.state = bladeStatus.releaseRequested;
                    }
                }
            }
        }
    }
}