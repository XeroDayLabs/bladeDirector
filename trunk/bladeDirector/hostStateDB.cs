using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace bladeDirector
{
    public static class hostStateDB
    {
        private static List<string> logEvents = new List<string>(); 
        private static TimeSpan keepAliveTimeout = TimeSpan.FromMinutes(1);
        
        public static List<bladeOwnership> bladeStates;

        public static void resetAll()
        {
            bladeStates = new List<bladeOwnership>();
            foreach (string bladeAddr in Properties.Settings.Default.bladeList.Split(','))
            {
                ushort bladeAddrInt = ushort.Parse(bladeAddr);

                string bladeIP = "172.17.129." + (100 + bladeAddrInt);
                string iLOIP = "172.17.2." + (100 + bladeAddrInt);
                ushort iLOPort = (ushort)(Properties.Settings.Default.baseDebugPort + bladeAddrInt);
                string iscsiIP = "192.168.66." + (100 + bladeAddrInt);

                bladeSpec newSpec = new bladeSpec(bladeIP, iscsiIP, iLOIP, iLOPort);
                bladeStates.Add(new bladeOwnership(newSpec));
            }
        }

        public static void setKeepAliveTimeout(TimeSpan newTimeout)
        {
            keepAliveTimeout = newTimeout;
        }

        static hostStateDB()
        {
            resetAll();
        }

        public static void addLogEvent(string newEntry)
        {
            lock (logEvents)
            {
                logEvents.Add(DateTime.Now + " : " + newEntry);
            }
        }

        public static string[] getAllBladeIP()
        {
            lock (bladeStates)
            {
                return bladeStates.Select(x => x.bladeIP).ToArray();
            }
        }

        public static bladeOwnership getBladeByIP(string IP)
        {
            lock (bladeStates)
            {
                return bladeStates.SingleOrDefault(x => x.bladeIP == IP);
            }
        }

        public static resultCode tryRequestNode(string bladeIP, string requestorID)
        {
            lock (bladeStates)
            {
                bladeOwnership reqBlade = bladeStates.SingleOrDefault(x => x.bladeIP == bladeIP);
                if (reqBlade == null)
                {
                    addLogEvent("Blade " + requestorID + " requested blade " + bladeIP + "(not found)");
                    return resultCode.bladeNotFound;
                }

                lock (reqBlade)
                {
                    checkKeepAlives(reqBlade);

                    // If the blade is currently unused, we can just take it.
                    if (reqBlade.state == bladeStatus.unused)
                    {
                        reqBlade.currentOwner = requestorID;
                        reqBlade.state = bladeStatus.inUse;
                        reqBlade.lastKeepAlive = DateTime.Now;

                        addLogEvent("Blade " + requestorID + " requested blade " + bladeIP + "(success, blade was idle)");
                        return resultCode.success;
                    }

                    // Otherwise, we need to request that the blade is released, and return 'pending'. 
                    // Note that we don't permit a requestor to both own the blade, and be in the queue - this is because the
                    // requestor would be unable to determine when its blade is allocated. We just return queuefull in that
                    // situation.
                    if (reqBlade.currentOwner == requestorID)
                    {
                        addLogEvent("Blade " + requestorID + " requested blade " + bladeIP + "(failure, blade is already owned by this blade)");
                        return resultCode.bladeQueueFull;
                    }

                    // If the blade is already queued as requested, just report OK and leave it there,
                    if (reqBlade.nextOwner == requestorID)
                    {
                        addLogEvent("Blade " + requestorID + " requested blade " + bladeIP + "(success, requestor was already in queue)");
                        return resultCode.success;
                    }

                    // See if the blade queue is actually full
                    if (reqBlade.nextOwner != null)
                    {
                        addLogEvent("Blade " + requestorID + " requested blade " + bladeIP + "(failure, blade queue is full)");
                        return resultCode.bladeQueueFull;
                    }

                    // It's all okay, so request the release.
                    reqBlade.state = bladeStatus.releaseRequested;
                    reqBlade.nextOwner = requestorID;

                    addLogEvent("Blade " + requestorID + " requested blade " + bladeIP + "(success, requestor added to blade queue)");
                    return resultCode.pending;
                }
            }
        }

        private static void checkKeepAlives(bladeOwnership reqBlade)
        {
            lock (reqBlade)
            {
                if (reqBlade.state != bladeStatus.unused)
                {
                    if (reqBlade.lastKeepAlive + keepAliveTimeout < DateTime.Now)
                    {
                        // Oh no, the blade owner failed to send a keepalive in time!
                        addLogEvent("Requestor " + reqBlade.currentOwner + " failed to keepalive for " + reqBlade.bladeIP + ", releasing blade");
                        releaseBlade(reqBlade.bladeIP, reqBlade.currentOwner);
                    }
                }
            }
        }

        public static string[] getBladesByAllocatedServer(string NodeIP)
        {
            lock (bladeStates)
            {
                return bladeStates.Where(x => x.currentOwner == NodeIP).Select(x => x.bladeIP).ToArray();
            }
        }

        public static void initWithBlades(string[] bladeIPs)
        {
            lock (bladeStates)
            {
                bladeStates.Clear();
                foreach (string bladeIP in bladeIPs)
                    bladeStates.Add(new bladeOwnership(bladeIP, "", "", 0));
            }
        }

        public static void initWithBlades(bladeSpec[] bladeSpecs)
        {
            lock (bladeStates)
            {
                bladeStates.Clear();
                foreach (bladeSpec spec in bladeSpecs)
                    bladeStates.Add(new bladeOwnership(spec));
            }
        }
        
        public static GetBladeStatusResult getBladeStatus(string nodeIp, string requestorIp)
        {
            lock (bladeStates)
            {
                bladeOwnership reqBlade = bladeStates.SingleOrDefault(x => x.bladeIP == nodeIp);
                if (reqBlade == null)
                    return GetBladeStatusResult.bladeNotFound;

                lock (reqBlade)
                {
                    checkKeepAlives(reqBlade);

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
                bladeOwnership reqBlade = bladeStates.SingleOrDefault(x => x.bladeIP == NodeIP);
                if (reqBlade == null)
                {
                    addLogEvent("Requestor " + reqBlade.currentOwner + " attempted to release blade " + NodeIP + " (blade not found)");
                    return resultCode.bladeNotFound;
                }

                lock (reqBlade)
                {
                    if (reqBlade.currentOwner != RequestorIP)
                    {
                        addLogEvent("Requestor " + reqBlade.currentOwner + " attempted to release blade " + NodeIP + " (failure: blade is not owned by requestor)");
                        return resultCode.bladeInUse;
                    }

                    // If there's no-one waiting, just set it to idle.
                    if (reqBlade.state == bladeStatus.inUse)
                    {
                        reqBlade.state = bladeStatus.unused;
                        reqBlade.currentOwner = null;
                        addLogEvent("Requestor " + reqBlade.currentOwner + " attempted to release blade " + NodeIP + " (success, blade is now idle)");
                        return resultCode.success;
                    }
                    // If there's someone waiting, allocate it to that blade.
                    if (reqBlade.state == bladeStatus.releaseRequested)
                    {
                        addLogEvent("Requestor " + reqBlade.currentOwner + " attempted to release blade " + NodeIP + " (success, blade is now owned by queue entry " + reqBlade.nextOwner + ")");

                        reqBlade.state = bladeStatus.inUse;
                        reqBlade.currentOwner = reqBlade.nextOwner;
                        reqBlade.nextOwner = null;
                        reqBlade.lastKeepAlive = DateTime.Now;

                        return resultCode.success;
                    }
                }
            }
            addLogEvent("Requestor " + RequestorIP + " attempted to release blade " + NodeIP + " (generic failure)");
            return resultCode.genericFail;
        }

        public static resultCode forceBladeAllocation(string nodeIp, string newOwner)
        {
            lock (bladeStates)
            {
                bladeOwnership reqBlade = bladeStates.SingleOrDefault(x => x.bladeIP == nodeIp);
                if (reqBlade == null)
                    return resultCode.bladeNotFound;

                lock (reqBlade)
                {
                    reqBlade.state = bladeStatus.inUse;
                    reqBlade.currentOwner = newOwner;
                    reqBlade.nextOwner = null;

                    return resultCode.success;
                }
            }
        }

        public static bladeSpec getConfigurationOfBlade(string nodeIp)
        {
            lock (bladeStates)
            {
                bladeOwnership reqBlade = bladeStates.SingleOrDefault(x => x.bladeIP == nodeIp);
                if (reqBlade == null)
                    return null;

                lock (reqBlade)
                {
                    // Clone it, since it might be modified after we release the lock otherwise.
                    return reqBlade.clone();
                }
            }
        }

        public static string getCurrentSnapshotForBlade(string nodeIp)
        {
            lock (bladeStates)
            {
                bladeOwnership reqBlade = bladeStates.SingleOrDefault(x => x.bladeIP == nodeIp);
                if (reqBlade == null)
                    return null;

                lock (reqBlade)
                {
                    return reqBlade.currentSnapshotName;
                }
            }
        }

        public static resultCodeAndBladeName RequestAnySingleNode(string requestorIP)
        {
            lock (bladeStates)
            {
                foreach (bladeOwnership blade in bladeStates)
                    checkKeepAlives(blade);
                // Put blades in an order of preference. First come unused blades, then used blades with an empty queue.
                IEnumerable<bladeOwnership> unusedBlades = bladeStates.Where(x => x.currentOwner == null);
                IEnumerable<bladeOwnership> emptyQueueBlades = bladeStates.Where(x => x.currentOwner != null && x.nextOwner == null);
                IEnumerable<bladeOwnership> orderedBlades = unusedBlades.Concat(emptyQueueBlades);

                foreach (bladeOwnership reqBlade in orderedBlades)
                {
                    resultCode res = tryRequestNode(reqBlade.bladeIP, requestorIP);
                    if (res == resultCode.success || res == resultCode.pending)
                    {
                        return new resultCodeAndBladeName() { bladeName = reqBlade.bladeIP, code = res };
                    }
                }
            }
            // Otherwise, all blades have full queues.
            return new resultCodeAndBladeName() { bladeName = null, code = resultCode.bladeQueueFull };
        }

        public static void keepAlive(string requestorIP)
        {
            lock (bladeStates)
            {
                foreach (bladeOwnership blade in bladeStates.Where(x => x.currentOwner == requestorIP))
                {
                    lock (blade)
                    {
                        blade.lastKeepAlive = DateTime.Now;
                    }
                }
            }
        }

        public static bool isBladeMine(string nodeIp, string requestorIp)
        {
            lock (bladeStates)
            {
                foreach (bladeOwnership blade in bladeStates)
                    checkKeepAlives(blade);

                bladeOwnership reqBlade = bladeStates.SingleOrDefault(x => x.bladeIP == nodeIp);
                if (reqBlade == null)
                    return false;

                lock (reqBlade)
                {
                    if (reqBlade.currentOwner == requestorIp)
                        return true;
                }
                return false;
            }
        }

        public static List<string> getLogEvents()
        {
            lock (logEvents)
            {
                List<string> toRet = new List<string>(logEvents);
                return toRet;
            }
        }
    }

    public class bladeSpec
    {
        // If you add fields, don't forget to add them to the Equals() override too.
        public string iscsiIP;
        public string bladeIP;
        public string iLOIP;
        public ushort iLOPort;

        public bladeSpec()
        {
            // For XML serialisation
        }

        public bladeSpec(string newBladeIP, string newISCSIIP, string newILOIP, ushort newILOPort)
        {
            iscsiIP = newISCSIIP;
            bladeIP = newBladeIP;
            iLOPort = newILOPort;
            iLOIP = newILOIP;
        }

        public bladeSpec clone()
        {
            return new bladeSpec(bladeIP, iscsiIP, iLOIP, iLOPort);
        }

        public override bool Equals(object obj)
        {
            bladeSpec compareTo = obj as bladeSpec;
            if (compareTo == null)
                return false;

            if (iscsiIP != compareTo.iscsiIP)
                return false;
            if (bladeIP != compareTo.bladeIP)
                return false;
            if (iLOIP != compareTo.iLOIP)
                return false;
            if (iLOPort != compareTo.iLOPort)
                return false;

            return true;
        }
    }

    public class bladeOwnership : bladeSpec
    {
        public bladeStatus state = bladeStatus.unused;
        public string currentOwner = null;
        public string nextOwner = null;

        public string currentSnapshotName { get { return bladeIP + "-" + currentOwner; }}
        public DateTime lastKeepAlive;

        public bladeOwnership(bladeSpec spec)
            : base(spec.bladeIP, spec.iscsiIP, spec.iLOIP, spec.iLOPort)
        {
            
        }

        public bladeOwnership(string newIPAddress, string newICSIIP, string newILOIP, ushort newILOPort)
            : base(newIPAddress, newICSIIP, newILOIP, newILOPort)
        {
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

    public class resultCodeAndBladeName
    {
        public resultCode code;
        public string bladeName;
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