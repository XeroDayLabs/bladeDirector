using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Threading;
using System.Web;
using System.Xml.Serialization;

namespace bladeDirector
{
    public static class hostStateDB
    {
        private static List<string> logEvents = new List<string>(); 
        private static TimeSpan keepAliveTimeout = TimeSpan.FromMinutes(1);

        private static Object connLock = new object();
        private static SQLiteConnection conn = null;
        public static string dbFilename;

        //public static List<bladeOwnership> bladeStates;

        public static void init(string basePath)
        {
            lock (connLock)
            {
                dbFilename = Path.Combine(basePath, "hoststate.sqlite");

                // If we're making a new file, remember that, since we'll have to create a new schema.
                bool needToCreateSchema = !File.Exists(dbFilename);

                conn = new SQLiteConnection("Data Source=" + dbFilename);
                conn.Open();

                if (needToCreateSchema)
                    createTables();
            }
        }

        public static void resetAll()
        {
            lock (connLock)
            {
                dropDB();
                createTables();
            }
        }

        private static void dropDB()
        {
            lock (connLock)
            {
                if (conn != null)
                {
                    conn.Close();
                    conn.Dispose();
                }

                if (dbFilename != ":memory:")
                {
                    DateTime deadline = DateTime.Now + TimeSpan.FromMinutes(1);
                    while (true)
                    {
                        try
                        {
                            File.Delete(dbFilename);
                            break;
                        }
                        catch (UnauthorizedAccessException)
                        {
                            if (deadline < DateTime.Now)
                                throw;
                            Thread.Sleep(TimeSpan.FromSeconds(2));
                        }
                    }
                }

                conn = new SQLiteConnection("Data Source=" + dbFilename);
                conn.Open();
            }
        }

        private static void createTables()
        {
            lock (connLock)
            {
                string[] sqlCommands = Properties.Resources.DBCreation.Split(';');

                foreach (string sqlCommand in sqlCommands)
                {
                    using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }

        public static void setKeepAliveTimeout(TimeSpan newTimeout)
        {
            keepAliveTimeout = newTimeout;
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
            lock (connLock)
            {
                string sqlCommand = "select bladeIP from bladeConfiguration";
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    using (SQLiteDataReader reader = cmd.ExecuteReader())
                    {
                        List<string> toRet = new List<string>();
                        while (reader.Read())
                            toRet.Add((string) reader[0]);
                        return toRet.ToArray();
                    }
                }
            }
        }

        public static List<bladeOwnership> getAllBladeInfo()
        {
            lock (connLock)
            {
                string sqlCommand = "select * from bladeOwnership " +
                                    "join bladeConfiguration on bladeOwnership.bladeConfigID = bladeConfiguration.id ";
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    using (SQLiteDataReader reader = cmd.ExecuteReader())
                    {
                        List<bladeOwnership> toRet = new List<bladeOwnership>();

                        while (reader.Read())
                            toRet.Add(new bladeOwnership(reader));

                        return toRet.ToList();
                    }
                }
            }
        }

        public static bladeOwnership getBladeByIP(string IP)
        {
            lock (connLock)
            {
                string sqlCommand = "select * from bladeOwnership " +
                                    "join bladeConfiguration on bladeOwnership.bladeConfigID = bladeConfiguration.id " +
                                    "where bladeConfiguration.bladeIP = $bladeIP";
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    cmd.Parameters.AddWithValue("$bladeIP", IP);
                    using (SQLiteDataReader reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            return new bladeOwnership(reader);
                        }
                        else
                        {
                            // No records returned.
                            return null;
                        }
                    }
                }
            }
        }

        public static resultCode tryRequestNode(string bladeIP, string requestorID)
        {
            lock (connLock)
            {
                bladeOwnership reqBlade = getBladeByIP(bladeIP);
                if (reqBlade == null)
                {
                    addLogEvent("Blade " + requestorID + " requested blade " + bladeIP + "(not found)");
                    return resultCode.bladeNotFound;
                }

                reqBlade = checkKeepAlives(reqBlade);

                // If the blade is currently unused, we can just take it.
                if (reqBlade.state == bladeStatus.unused)
                {
                    reqBlade.currentOwner = requestorID;
                    reqBlade.state = bladeStatus.inUse;
                    reqBlade.lastKeepAlive = DateTime.Now;
                    reqBlade.updateInDB(conn);

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
                reqBlade.updateInDB(conn);

                addLogEvent("Blade " + requestorID + " requested blade " + bladeIP + "(success, requestor added to blade queue)");
                return resultCode.pending;
            }
        }

        private static void checkKeepAlives(string reqBladeIP)
        {
            lock (connLock)
            {
                string sqlCommand = "select * from bladeOwnership " +
                                    "join bladeConfiguration on bladeOwnership.bladeConfigID = bladeConfiguration.id " +
                                    "where bladeOwnership.currentOwner = $bladeIP";
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    cmd.Parameters.AddWithValue("$bladeIP", reqBladeIP);
                    using (SQLiteDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            checkKeepAlives(new bladeOwnership(reader));
                        }
                    }
                }
            }
        }

        private static bladeOwnership checkKeepAlives(bladeOwnership reqBlade)
        {
            lock (connLock)
            {
                if (reqBlade.state != bladeStatus.unused)
                {
                    if (reqBlade.lastKeepAlive + keepAliveTimeout < DateTime.Now)
                    {
                        // Oh no, the blade owner failed to send a keepalive in time!
                        addLogEvent("Requestor " + reqBlade.currentOwner + " failed to keepalive for " + reqBlade.bladeIP + ", releasing blade");
                        releaseBlade(reqBlade.bladeIP, reqBlade.currentOwner);

                        return getBladeByIP(reqBlade.bladeIP);
                    }
                }
            }
            return reqBlade;
        }

        public static string[] getBladesByAllocatedServer(string NodeIP)
        {
            lock (connLock)
            {
                checkKeepAlives(NodeIP);

                string sqlCommand = "select bladeIP from bladeOwnership " +
                                    "join bladeConfiguration on bladeOwnership.bladeConfigID = bladeConfiguration.id " +
                                    "where bladeOwnership.currentOwner = $bladeOwner";
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    cmd.Parameters.AddWithValue("$bladeOwner", NodeIP);
                    using (SQLiteDataReader reader = cmd.ExecuteReader())
                    {
                        List<string> toRet = new List<string>(10);
                        while (reader.Read())
                            toRet.Add((string)reader["bladeIP"]);
                        return toRet.ToArray();
                    }
                }
            }
        }

        public static void initWithBlades(string[] bladeIPs)
        {
            bladeSpec[] specs = new bladeSpec[bladeIPs.Length];
            int n = 0;
            foreach (string bladeIP in bladeIPs)
                specs[n++] = new bladeOwnership(bladeIP, n.ToString(), n.ToString(), (ushort)n);

            initWithBlades(specs);
        }

        public static void initWithBlades(bladeSpec[] bladeSpecs)
        {
            lock (connLock)
            {
                dropDB();
                createTables();

                foreach (bladeSpec spec in bladeSpecs)
                    addNode(spec);
            }
        }

        public static void addNode(bladeSpec spec)
        {
            bladeOwnership newOwnership = new bladeOwnership(spec.bladeIP, spec.iscsiIP, spec.iLOIP, spec.iLOPort);
            newOwnership.currentOwner = null;
            newOwnership.nextOwner = null;
            newOwnership.lastKeepAlive = DateTime.Now;
            newOwnership.state = bladeStatus.unused;

            newOwnership.createInDB(conn);
        }

        public static GetBladeStatusResult getBladeStatus(string nodeIp, string requestorIp)
        {
            lock (connLock)
            {
                checkKeepAlives(requestorIp);

                string sqlCommand = "select * from bladeOwnership " +
                                    "join bladeConfiguration on bladeOwnership.bladeConfigID = bladeConfiguration.id " +
                                    "where bladeConfiguration.bladeIP = $bladeIP";
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    cmd.Parameters.AddWithValue("$bladeIP", nodeIp);
                    using (SQLiteDataReader reader = cmd.ExecuteReader())
                    {
                        if (!reader.Read())
                            return GetBladeStatusResult.bladeNotFound;

                        bladeOwnership reqBlade = new bladeOwnership(reader);

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
        }

        public static resultCode releaseBlade(string NodeIP, string RequestorIP)
        {
            lock (connLock)
            {
                string sqlCommand = "select * from bladeOwnership " +
                                    "join bladeConfiguration on bladeOwnership.bladeConfigID = bladeConfiguration.id " +
                                    "where bladeConfiguration.bladeIP = $bladeIP";
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    cmd.Parameters.AddWithValue("$bladeIP", NodeIP);
                    using (SQLiteDataReader reader = cmd.ExecuteReader())
                    {
                        if (!reader.Read())
                        {
                            addLogEvent("Requestor " + RequestorIP + " attempted to release blade " + NodeIP + " (blade not found)");
                            return resultCode.bladeNotFound;
                        }

                        bladeOwnership reqBlade = new bladeOwnership(reader);

                        if (reqBlade.currentOwner != RequestorIP)
                        {
                            addLogEvent("Requestor " + RequestorIP + " attempted to release blade " + NodeIP + " (failure: blade is not owned by requestor)");
                            return resultCode.bladeInUse;
                        }

                        // If there's no-one waiting, just set it to idle.
                        if (reqBlade.state == bladeStatus.inUse)
                        {
                            reqBlade.state = bladeStatus.unused;
                            reqBlade.currentOwner = null;
                            reqBlade.updateInDB(conn);
                            addLogEvent("Requestor " + RequestorIP + " attempted to release blade " + NodeIP + " (success, blade is now idle)");
                            return resultCode.success;
                        }
                        // If there's someone waiting, allocate it to that blade.
                        if (reqBlade.state == bladeStatus.releaseRequested)
                        {
                            addLogEvent("Requestor " + RequestorIP + " attempted to release blade " + NodeIP + " (success, blade is now owned by queue entry " + reqBlade.nextOwner + ")");

                            reqBlade.state = bladeStatus.inUse;
                            reqBlade.currentOwner = reqBlade.nextOwner;
                            reqBlade.nextOwner = null;
                            reqBlade.lastKeepAlive = DateTime.Now;
                            reqBlade.updateInDB(conn);

                            return resultCode.success;
                        }
                    }
                }
                addLogEvent("Requestor " + RequestorIP + " attempted to release blade " + NodeIP + " (generic failure)");
                return resultCode.genericFail;
            }
        }

        public static resultCode forceBladeAllocation(string nodeIp, string newOwner)
        {
            lock (connLock)
            {
                bladeOwnership toAlloc = getBladeByIP(nodeIp);
                toAlloc.state = bladeStatus.inUse;
                toAlloc.currentOwner = newOwner;
                toAlloc.nextOwner = null;
                toAlloc.updateInDB(conn);

                return resultCode.success;
            }
        }

        public static bladeSpec getConfigurationOfBlade(string nodeIp)
        {
            lock (connLock)
            {
                string sqlCommand = "select * from bladeOwnership " +
                                    "join bladeConfiguration on bladeOwnership.bladeConfigID = bladeConfiguration.id " +
                                    "where bladeConfiguration.bladeIP = $bladeIP";
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    cmd.Parameters.AddWithValue("$bladeIP", nodeIp);
                    SQLiteDataReader reader = cmd.ExecuteReader();

                    if (!reader.Read())
                    {
                        addLogEvent("getConfigurationOfBlade failed " + nodeIp + " (blade not found)");
                        return null;
                    }

                    return new bladeOwnership(reader);
                }
            }
        }

        public static string getCurrentSnapshotForBlade(string nodeIp)
        {
            lock (connLock)
            {
                bladeOwnership reqBlade = getBladeByIP(nodeIp);
                if (reqBlade == null)
                    return null;

                return reqBlade.currentSnapshotName;
            }
        }

        public static resultCodeAndBladeName RequestAnySingleNode(string requestorIP)
        {
            lock (connLock)
            {
                List<bladeOwnership> bladeStates = new List<bladeOwnership>();

                string sqlCommand = "select * from bladeOwnership " +
                                    "join bladeConfiguration on bladeOwnership.bladeConfigID = bladeConfiguration.id ";
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    using (SQLiteDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            bladeOwnership newBlade = new bladeOwnership(reader);
                            newBlade = checkKeepAlives(newBlade);
                            bladeStates.Add(newBlade);
                        }
                    }
                }

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
            lock (connLock)
            {
                string sqlCommand = "update bladeOwnership set lastKeepAlive=$NOW " +
                                    "where currentOwner = $ownerIP";
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    cmd.Parameters.AddWithValue("$ownerIP", requestorIP);
                    cmd.Parameters.AddWithValue("$NOW", DateTime.Now.ToString());
                    cmd.ExecuteNonQuery();
                }
            }
        }

        public static bool isBladeMine(string nodeIp, string requestorIp)
        {
            lock (connLock)
            {
                checkKeepAlives(requestorIp);

                string sqlCommand = "select count(*) from bladeOwnership " +
                                    "join bladeConfiguration on bladeOwnership.bladeConfigID = bladeConfiguration.id " +
                                    "where bladeOwnership.currentOwner = $requestorIp " + 
                                    "and   bladeConfiguration.bladeIP = $bladeIP ";
                using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
                {
                    cmd.Parameters.AddWithValue("$bladeIP", nodeIp);
                    cmd.Parameters.AddWithValue("$requestorIp", requestorIp);
                    return (long)cmd.ExecuteScalar() > 0;
                }
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

    [XmlInclude(typeof(bladeOwnership))]
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

    [XmlInclude(typeof (bladeSpec))]
    public class bladeOwnership : bladeSpec
    {
        public long bladeID;
        public bladeStatus state = bladeStatus.unused;
        public string currentOwner = null;
        public string nextOwner = null;
        public DateTime lastKeepAlive;

        public string currentSnapshotName { get { return bladeIP + "-" + currentOwner; }}

        public bladeOwnership()
        {
            // For xml ser
        }

        public bladeOwnership(bladeSpec spec)
            : base(spec.bladeIP, spec.iscsiIP, spec.iLOIP, spec.iLOPort)
        {
            
        }

        public bladeOwnership(string newIPAddress, string newICSIIP, string newILOIP, ushort newILOPort)
            : base(newIPAddress, newICSIIP, newILOIP, newILOPort)
        {
        }

        public bladeOwnership(SQLiteDataReader reader)
        {
            iscsiIP = (string)reader["iscsiIP"];
            bladeID = (long)reader["id"];
            bladeIP = (string)reader["bladeIP"];
            iLOPort = ushort.Parse(reader["iLOPort"].ToString());
            iLOIP = (string)reader["iLOIP"];

            long enumIdx = (long)reader["state"];
            state = (bladeStatus) ((int)enumIdx);
            if (reader["currentOwner"] is System.DBNull)
                currentOwner = null;
            else
                currentOwner = (string)reader["currentOwner"];
            if (reader["nextOwner"] is System.DBNull)
                nextOwner = null;
            else
                nextOwner = (string)reader["nextOwner"];
            lastKeepAlive = DateTime.Parse((string)reader["lastKeepAlive"]);
        }

        public void createInDB(SQLiteConnection conn)
        {

            string cmd_bladeConfig = "insert into bladeConfiguration" +
                                "(iscsiIP, bladeIP, iLoIP, iLOPort)" +
                                " VALUES " +
                                "($iscsiIP, $bladeIP, $iLoIP, $iLOPort)";
            using (SQLiteCommand cmd = new SQLiteCommand(cmd_bladeConfig, conn))
            {
                cmd.Parameters.AddWithValue("$iscsiIP", iscsiIP);
                cmd.Parameters.AddWithValue("$bladeIP", bladeIP);
                cmd.Parameters.AddWithValue("$iLoIP", iLOIP);
                cmd.Parameters.AddWithValue("$iLOPort", iLOPort);
                cmd.ExecuteNonQuery();
                bladeID = (long)conn.LastInsertRowId;
            }
            
            string cmd_bladeOwnership = "insert into bladeOwnership " +
                                "(bladeConfigID, state, currentOwner, lastKeepAlive)" +
                                " VALUES " +
                                "($bladeConfigID, $state, $currentOwner, $lastKeepAlive)";
            using (SQLiteCommand cmd = new SQLiteCommand(cmd_bladeOwnership, conn))
            {
                cmd.Parameters.AddWithValue("$bladeConfigID", bladeID);
                cmd.Parameters.AddWithValue("$state", state);
                cmd.Parameters.AddWithValue("$currentOwner", currentOwner);
                cmd.Parameters.AddWithValue("$lastKeepAlive", lastKeepAlive);
                cmd.ExecuteNonQuery();
            }
        }

        public void updateInDB(SQLiteConnection conn)
        {
            string sqlCommand = "update bladeOwnership set " +
                                "state = $state, currentOwner = $currentOwner, nextOwner = $nextOwner, lastKeepAlive = $lastKeepAlive " +
                                "where bladeConfigID = $bladeID";
            using (SQLiteCommand cmd = new SQLiteCommand(sqlCommand, conn))
            {
                cmd.Parameters.AddWithValue("$bladeID", bladeID);
                cmd.Parameters.AddWithValue("$state", state);
                cmd.Parameters.AddWithValue("$currentOwner", currentOwner);
                cmd.Parameters.AddWithValue("$nextOwner", nextOwner);
                cmd.Parameters.AddWithValue("$lastKeepAlive", lastKeepAlive);
                cmd.ExecuteNonQuery();
            }
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