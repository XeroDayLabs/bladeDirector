using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using bladeDirectorClient.bladeDirectorService;
using hypervisors;

namespace bladeDirectorClient
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

    public interface IBladeOwnershipChecker: IDisposable
    {
        TimeSpan pollTime { get; }
        void beginOperation();
        bool isResourceReleaseRequested(int bladeID);
    }

    public class bladeOwnershipNull : IBladeOwnershipChecker
    {
        public void Dispose()
        {
            
        }

        public TimeSpan pollTime { get { return TimeSpan.FromSeconds(5);} }

        public void beginOperation()
        {
            
        }

        public bool isResourceReleaseRequested(int bladeID)
        {
            return false;
        }
    }

    public class mockedOwnershipChecker : IBladeOwnershipChecker
    {
        private bool hasBegunOperation = false;
        public TimeSpan pollTime { get { return TimeSpan.FromMilliseconds(500);} }

        private readonly Dictionary<int, bool> isBladeWaiting = new Dictionary<int, bool>();

        public mockedOwnershipChecker(int maxBladeID)
        {
            hasBegunOperation = true;
            for(int i = 0; i < maxBladeID; i++)
                isBladeWaiting.Add(i, false);
        }

        public void beginOperation()
        {
            hasBegunOperation = true;
        }

        public bool isResourceReleaseRequested(int bladeID)
        {
            if (!hasBegunOperation)
                throw new Exception(".beginOperation not yet called");

            return isBladeWaiting[bladeID];
        }

        public void setBladeWaitingStatus(int bladeID, bool shouldBeWaiting)
        {
            isBladeWaiting[bladeID] = shouldBeWaiting;
        }

        public void Dispose()
        {
        }
    }

    public class bladeOwnershipChecker : IBladeOwnershipChecker
    {
        private readonly string _baseURL;

        private class ownedBlade
        {
            public int bladeID;
            public string bladeIPAddress;
            public bool isReleaseRequested;
        }

        private readonly List<ownedBlade> _blades = new List<ownedBlade>();
        public TimeSpan pollTime { get { return TimeSpan.FromMinutes(1); } }

        private TimeSpan updateTime { get { return TimeSpan.FromMinutes(1); } }

        private readonly ManualResetEvent updateEvent = new ManualResetEvent(false);

        private bool _isDisposed = false;

        private readonly Task _updateTask;

        public bladeOwnershipChecker(string baseURL, string[] bladeIPs)
        {
            _baseURL = baseURL;
            for (int index = 0; index < bladeIPs.Length; index++)
            {
                var bladeIP = bladeIPs[index];
                _blades.Add(new ownedBlade()
                {
                    bladeID = index,
                    bladeIPAddress = bladeIP,
                    isReleaseRequested = false
                });
            }

            _updateTask = new Task(() =>
            {
                updateEvent.WaitOne(updateTime);
                if (_isDisposed)
                {
                    return;
                }
                updateNow();
            });
        }

        public void beginOperation()
        {
            lock (this)
            {
                _blades.All(x => x.isReleaseRequested = false);
            }
        }

        private void updateNow()
        {
            lock (this)
            {
                using (BladeDirectorServices svc = new BladeDirectorServices(_baseURL))
                {
                    foreach (ownedBlade blade in _blades)
                    {
                        GetBladeStatusResult bladeStatus = svc.svc.GetBladeStatus(blade.bladeIPAddress);
                        if (bladeStatus == GetBladeStatusResult.releasePending)
                            blade.isReleaseRequested = true;
                        else
                            blade.isReleaseRequested = false;
                    }
                }
            }
        }
        
        public bool isResourceReleaseRequested(int bladeID)
        {
            lock (this)
            {
                return _blades.Single(x => x.bladeID == bladeID).isReleaseRequested;
            }
        }

        public void Dispose()
        {
            _isDisposed = true;
            updateEvent.Set();
            _updateTask.Wait();
        }
    }
}