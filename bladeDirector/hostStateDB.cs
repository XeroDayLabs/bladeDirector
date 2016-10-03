using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace bladeDirector
{
    public static class hostStateDB
    {
        public static List<bladeOwnership> bladeStates;

        static hostStateDB()
        {
            bladeStates = new List<bladeOwnership>();
            foreach (string bladeAddr in Properties.Settings.Default.bladeList.Split(','))
            {
                int bladeAddrInt = int.Parse(bladeAddr);
                bladeSpec newSpec = new bladeSpec()
                {
                    bladeIP = "172.17.129." + (100 + bladeAddrInt),
                    iscsiIP = "192.168.66." + (100 + bladeAddrInt)
                };
                bladeStates.Add(new bladeOwnership(newSpec));
            }
        }

        public static string[] getAllBladeIP()
        {
            lock (bladeStates)
            {
                return bladeStates.Select(x => x.IPAddress).ToArray();
            }
        }

        public static bladeOwnership getBladeByIP(string IP)
        {
            lock (bladeStates)
            {
                return bladeStates.SingleOrDefault(x => x.IPAddress == IP);
            }
        }

        public static resultCode tryRequestNode(string bladeIP, string requestorID)
        {
            lock (bladeStates)
            {
                bladeOwnership reqBlade = bladeStates.SingleOrDefault(x => x.IPAddress == bladeIP);
                if (reqBlade == null)
                    return resultCode.bladeNotFound;

                lock (reqBlade)
                {
                    // If the blade is currently unused, we can just take it.
                    if (reqBlade.state == bladeStatus.unused)
                    {
                        reqBlade.currentOwner = requestorID;
                        reqBlade.state = bladeStatus.inUse;
                        return resultCode.success;
                    }

                    // Otherwise, we need to request that the blade is released. 
                    if (reqBlade.nextOwner != null)
                        return resultCode.bladeQueueFull;

                    reqBlade.state = bladeStatus.releaseRequested;
                    reqBlade.nextOwner = requestorID;

                    return resultCode.pending;
                }
            }
        }

        public static string[] getBladesByAllocatedServer(string NodeIP)
        {
            lock (bladeStates)
            {
                return bladeStates.Where(x => x.currentOwner == NodeIP).Select(x => x.IPAddress).ToArray();
            }
        }

        public static void initWithBlades(string[] bladeIPs)
        {
            lock (bladeStates)
            {
                bladeStates.Clear();
                foreach (string bladeIP in bladeIPs)
                    bladeStates.Add(new bladeOwnership(bladeIP, ""));
            }
        }

        public static void initWithBlades(bladeSpec[] specs)
        {
            lock (bladeStates)
            {
                foreach (bladeSpec spec in specs)
                    bladeStates.Add(new bladeOwnership(spec.bladeIP, spec.iscsiIP));
            }
        }

        public static GetBladeStatusResult getBladeStatus(string nodeIp, string requestorIp)
        {
            lock (bladeStates)
            {
                bladeOwnership reqBlade = bladeStates.SingleOrDefault(x => x.IPAddress == nodeIp);
                if (reqBlade == null)
                    return GetBladeStatusResult.bladeNotFound;

                lock (reqBlade)
                {
                    switch (reqBlade.state)
                    {
                        case bladeStatus.unused:
                            return GetBladeStatusResult.unused;
                        case bladeStatus.releaseRequested:
                            return GetBladeStatusResult.releasePending;
                        case bladeStatus.inUse:
                            if (reqBlade.currentOwner == requestorIp)
                                return GetBladeStatusResult.yours;
                            else
                                return GetBladeStatusResult.notYours;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
            }
        }

        public static resultCode releaseBlade(string NodeIP, string RequestorIP)
        {
            lock (bladeStates)
            {
                bladeOwnership reqBlade = bladeStates.SingleOrDefault(x => x.IPAddress == NodeIP);
                if (reqBlade == null)
                    return resultCode.bladeNotFound;

                lock (reqBlade)
                {
                    if (reqBlade.currentOwner != RequestorIP)
                        return resultCode.bladeInUse;

                    // If there's no-one waiting, just set it to idle.
                    if (reqBlade.state == bladeStatus.inUse)
                    {
                        reqBlade.state = bladeStatus.unused;
                        reqBlade.currentOwner = null;
                        return resultCode.success;
                    }
                    // If there's someone waiting, allocate it to that blade.
                    if (reqBlade.state == bladeStatus.releaseRequested)
                    {
                        reqBlade.state = bladeStatus.inUse;
                        reqBlade.currentOwner = reqBlade.nextOwner;
                        reqBlade.nextOwner = null;

                        return resultCode.success;
                    }
                }
            }
            return resultCode.genericFail;
        }
    }

    public class bladeSpec
    {
        public string iscsiIP;
        public string bladeIP;
    }

    public class bladeOwnership
    {
        public readonly string IPAddress;
        public readonly string ISCSIIpAddress;

        public bladeStatus state = bladeStatus.unused;
        public string currentOwner = null;
        public string nextOwner = null;

        public bladeOwnership(bladeSpec spec)
            : this(spec.bladeIP, spec.iscsiIP)
        {
            
        }

        public bladeOwnership(string newIPAddress, string newICSIIP)
        {
            IPAddress = newIPAddress;
            ISCSIIpAddress = newICSIIP;
        }
    }

    public enum GetBladeStatusResult
    {
        bladeNotFound,
        unused,
        yours,
        releasePending,
        notYours
    }

    public enum bladeStatus
    {
        unused,
        releaseRequested,
        inUse
    }

    public enum resultCode
    {
        success,
        bladeNotFound,
        bladeInUse,
        bladeQueueFull,
        pending,
        genericFail
    }
}