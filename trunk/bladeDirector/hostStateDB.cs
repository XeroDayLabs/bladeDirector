using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Web;
using System.Web.UI.WebControls;
using hypervisors;
using Tamir.SharpSsh;

namespace bladeDirector
{
    public static class hostStateDB
    {
        private static List<string> logEvents = new List<string>();
        public static TimeSpan keepAliveTimeout = TimeSpan.FromMinutes(1);

        private static Object connLock = new object();
        private static SQLiteConnection conn = null;
        public static string dbFilename;

        private static ConcurrentDictionary<string, biosThreadState> biosUpdateState = new ConcurrentDictionary<string, biosThreadState>();

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
                    notifyBootDirectorOfNode(reqBlade);
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
                    notifyBootDirectorOfNode(reqBlade);
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
                specs[n++] = new bladeOwnership(bladeIP, n.ToString(), n.ToString(), (ushort)n, null, null);

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
            bladeOwnership newOwnership = new bladeOwnership(spec.bladeIP, spec.iscsiIP, spec.iLOIP, spec.iLOPort, spec.currentSnapshot, spec.lastDeployedBIOS);
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

        public static resultCode releaseBlade(string NodeIP, string RequestorIP, bool force = false)
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

                        if (!force)
                        {
                            if (reqBlade.currentOwner != RequestorIP)
                            {
                                addLogEvent("Requestor " + RequestorIP + " attempted to release blade " + NodeIP + " (failure: blade is not owned by requestor)");
                                return resultCode.bladeInUse;
                            }
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

                return String.Format("{0}-{1}-{2}", reqBlade.bladeIP, reqBlade.currentOwner, reqBlade.currentSnapshot);
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
                        return new resultCodeAndBladeName() { bladeName = reqBlade.bladeIP, code = res };
                }
            }
            // Otherwise, all blades have full queues.
            return new resultCodeAndBladeName() { bladeName = null, code = resultCode.bladeQueueFull };
        }

        private static void notifyBootDirectorOfNode(bladeOwnership blade)
        {
            bootMenuServiceController.Program.add(IPAddress.Parse(blade.bladeIP));
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

        public static resultCode selectSnapshotForBlade(string nodeIp, string newShot)
        {
            lock (connLock)
            {
                bladeOwnership reqBlade = getBladeByIP(nodeIp);
                if (reqBlade == null)
                    return resultCode.bladeNotFound;
                reqBlade.currentSnapshot = newShot;
                return reqBlade.updateInDB(conn);
            }
        }

        public static string getLastDeployedBIOSForBlade(string nodeIp)
        {
            lock (connLock)
            {
                bladeOwnership reqBlade = getBladeByIP(nodeIp);
                if (reqBlade == null)
                    return null;

                return reqBlade.lastDeployedBIOS;
            }
        }

        public static resultCode rebootAndStartDeployingBIOSToBlade(string nodeIp, string requestorIP, string biosxml)
        {
            lock (connLock)
            {
                bladeOwnership reqBlade = getBladeByIP(nodeIp);

                if (reqBlade.currentOwner != requestorIP)
                    return resultCode.bladeInUse;
                // First, check if this BIOS is already deployed.
                if (reqBlade.lastDeployedBIOS == biosxml)
                    return resultCode.noNeedLah;
                //  We need to:
                //  1) set this blade to boot into LTSP
                //  2) start the blade
                //  3) wait for it to boot
                //  4) SSH into it, and run conrep to configure the BIOS.
                
                // Mark the blade as BIOS-flashing. This will mean that, next time it boots, it will be served the LTSP image.
                reqBlade.currentlyHavingBIOSDeployed = true;
                reqBlade.updateInDB(conn);

                biosThreadState newState = new biosThreadState(nodeIp, reqBlade.iLOIP, biosxml);
                //lock (newState)
                {
                    if (biosUpdateState.TryAdd(nodeIp, newState) == false)
                    {
                        // the key was already present. Maybe it's finished a previous request.
                        if (checkBIOSWriteProgress(nodeIp) != resultCode.pending)
                        {
                            biosThreadState tmp;
                            biosUpdateState.TryRemove(nodeIp, out tmp);
                            if (biosUpdateState.TryAdd(nodeIp, newState) == false)
                                return resultCode.genericFail;
                        }
                        else
                        {
                            return resultCode.alreadyInProgress;
                        }
                    }

                    // otherwise, go ahead and spin up a new thread to handle this update.
                    newState.onBootFinish = SetBIOS;
                    newState.rebootThread = new Thread(ltspBootThread);
                    newState.rebootThread.Name = "Booting " + nodeIp + " to LTSP";
                    newState.rebootThread.Start(newState);
                }

                return resultCode.pending;
            }
        }

        public static resultCode rebootAndStartReadingBIOSConfiguration(string nodeIp, string requestorIP)
        {
            lock (connLock)
            {
                bladeOwnership reqBlade = getBladeByIP(nodeIp);

                if (reqBlade.currentOwner != requestorIP)
                    return resultCode.bladeInUse;

                // Mark the blade as BIOS-flashing. This will mean that, next time it boots, it will be served the LTSP image.
                reqBlade.currentlyHavingBIOSDeployed = true;
                reqBlade.updateInDB(conn);

                biosThreadState newState = new biosThreadState(nodeIp, reqBlade.iLOIP, null);
                if (biosUpdateState.TryAdd(nodeIp, newState) == false)
                {
                    // the key was already present. Maybe it's finished a previous request.
                    if (checkBIOSWriteProgress(nodeIp) != resultCode.pending)
                    {
                        biosThreadState tmp;
                        biosUpdateState.TryRemove(nodeIp, out tmp);
                        if (biosUpdateState.TryAdd(nodeIp, newState) == false)
                            return resultCode.genericFail;
                    }
                    else
                    {
                        return resultCode.alreadyInProgress;
                    }
                }

                // otherwise, go ahead and spin up a new thread to handle this update.
                newState.onBootFinish = GetBIOS;
                newState.rebootThread = new Thread(ltspBootThread);
                newState.rebootThread.Name = "Booting " + nodeIp + " to LTSP";
                newState.rebootThread.Start(newState);

                return resultCode.pending;
            }
        }

        public static resultCode checkBIOSWriteProgress(string nodeIp)
        {
            lock (connLock)
            {
                biosThreadState newState;
                if (biosUpdateState.TryGetValue(nodeIp, out newState) == false)
                    return resultCode.bladeNotFound;

                if (!newState.isFinished)
                    return resultCode.pending;
                
                if (newState.result == resultCode.success)
                {
                    bladeOwnership bladeStatus = getBladeByIP(nodeIp);
                }
                return newState.result;
            }
        }

        public static resultCodeAndBIOSConfig checkBIOSReadProgress(string nodeIp)
        {
            lock (connLock)
            {
                biosThreadState newState;
                if (biosUpdateState.TryGetValue(nodeIp, out newState) == false)
                    return new resultCodeAndBIOSConfig( resultCode.bladeNotFound );

                if (!newState.isFinished)
                    return new resultCodeAndBIOSConfig(resultCode.pending );

                if (newState.result == resultCode.success)
                {
                    bladeOwnership bladeStatus = getBladeByIP(nodeIp);
                }
                return new resultCodeAndBIOSConfig(newState);
            }
        }

        private static void ltspBootThread(Object o)
        {
            biosThreadState param = (biosThreadState)o;
            param.result = resultCode.genericFail;
            _ltspBootThreadStart(param);
        }

        private static void _ltspBootThreadStart(biosThreadState param)
        {
            // Power cycle it
            hypervisor_iLo_HTTP hyp = new hypervisor_iLo_HTTP(param.iLoIP, Properties.Settings.Default.iloUsername, Properties.Settings.Default.iloPassword);
            hyp.connect();
            hyp.powerOff();
            hyp.powerOn();
            hyp.logout();

            // Wait for it to boot.  Note that we don't ping the client repeatedly here - since the Ping class can cause 
            // a BSoD.. ;_; Instead, we wait for port 22 (SSH) to be open.
            param.connectDeadline = DateTime.Now + TimeSpan.FromMinutes(5);
            param.biosUpdateSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            param.biosUpdateSocket.BeginConnect(new IPEndPoint(IPAddress.Parse(param.nodeIp), 22), param.onBootFinish, param);
        }

        private static List<string> copyDeploymentFilesToBlade(string nodeIP, string biosConfigFile)
        {
            List<string> toRet = new List<string>(5);

            string applyBIOSFile = null;
            string getBIOSFile = null;
            string conrepFile = null;
            string conrepXmlFile = null;
            string newBiosFile = null;

            try
            {
                Scp scp = new Tamir.SharpSsh.Scp(nodeIP, Properties.Settings.Default.ltspUsername, Properties.Settings.Default.ltspPassword);
                scp.Connect();
                applyBIOSFile = writeTempFile(Properties.Resources.applyBIOS, true);
                scp.Put(applyBIOSFile, "applyBIOS.sh");
                getBIOSFile = writeTempFile(Properties.Resources.getBIOS, true);
                scp.Put(getBIOSFile, "getBIOS.sh");
                conrepFile = writeTempFile(Properties.Resources.conrep);
                scp.Put(conrepFile, "conrep");
                conrepXmlFile = writeTempFile(Properties.Resources.conrep_xml, true);
                scp.Put(conrepXmlFile, "conrep.xml");
                if (biosConfigFile != null)
                {
                    newBiosFile = writeTempFile(Encoding.ASCII.GetBytes(biosConfigFile));
                    scp.Put(newBiosFile, "newbios.xml");
                }
            }
            finally
            {
                foreach (string filename in new[] {applyBIOSFile, getBIOSFile, conrepFile, conrepXmlFile, newBiosFile})
                {
                    if (filename != null)
                        toRet.Add(filename);
                }

            }
            return toRet;
        }

        private static void GetBIOS(IAsyncResult ar)
        {
            biosThreadState state = (biosThreadState)ar.AsyncState;
            if (retryIfFailedConnect(ar, state))
                return;

            List<string> tempFilesToCleanUp = null;
            try
            {
                tempFilesToCleanUp = copyDeploymentFilesToBlade(state.nodeIp, null);

                SshExec exec = new SshExec(state.nodeIp, Properties.Settings.Default.ltspUsername, Properties.Settings.Default.ltspPassword);
                exec.Connect();
                string stderr = String.Empty;
                string stdout = String.Empty;
                int returnCode = exec.RunCommand("bash ~/getBIOS.sh", ref stdout, ref stderr);

                if (returnCode != 0)
                {
                    addLogEvent(string.Format("Reading BIOS on {0} resulted in error code {1}", state.nodeIp, returnCode));
                    Debug.WriteLine("Faied bios deploy, error code " + returnCode);
                    Debug.WriteLine("stdout " + stdout);
                    Debug.WriteLine("stderr " + stderr);
                    state.result = resultCode.genericFail;
                }
                else
                {
                    addLogEvent(string.Format("Deployed BIOS successfully to {0}", state.nodeIp));
                    state.result = resultCode.success;
                }

                // Retrieve the output
                Scp scp = new Tamir.SharpSsh.Scp(state.nodeIp, Properties.Settings.Default.ltspUsername, Properties.Settings.Default.ltspPassword);
                scp.Connect();
                string existingConfig = Path.GetTempFileName();
                state.biosxml = null;
                try
                {
                    scp.Get("currentbios.xml", existingConfig);
                    state.biosxml = File.ReadAllText(existingConfig);
                }
                finally 
                {
                    deleteWithRetry(existingConfig);
                }

                // All done, now we can power off and return.
                hypervisor_iLo_HTTP hyp = new hypervisor_iLo_HTTP(state.iLoIP, Properties.Settings.Default.iloUsername, Properties.Settings.Default.iloPassword);
                hyp.connect();
                hyp.powerOff();
                hyp.logout();
                lock (connLock)
                {
                    bladeOwnership reqBlade = getBladeByIP(state.nodeIp);
                    reqBlade.currentlyHavingBIOSDeployed = false;
                    reqBlade.lastDeployedBIOS = state.biosxml;
                    reqBlade.updateInDB(conn);
                }
                state.isFinished = true;

            }
            finally
            {
                foreach (string filename in tempFilesToCleanUp)
                    deleteWithRetry(filename);
            }
        }

        private static void SetBIOS(IAsyncResult ar)
        {
            biosThreadState state = (biosThreadState) ar.AsyncState;
            if (retryIfFailedConnect(ar, state))
                return;

            List<string> tempFilesToCleanUp = null;
            try
            {
                // Okay, now the box is up :)
                // SCP some needed files to it.
                tempFilesToCleanUp = copyDeploymentFilesToBlade(state.nodeIp, state.biosxml);

                // And execute the command to deploy the BIOS via SSH.
                SshExec exec = new SshExec(state.nodeIp, Properties.Settings.Default.ltspUsername, Properties.Settings.Default.ltspPassword);
                exec.Connect();
                string stderr = String.Empty;
                string stdout = String.Empty;
                int returnCode = exec.RunCommand("bash ~/applyBIOS.sh", ref stdout, ref stderr);

                if (returnCode != 0)
                {
                    addLogEvent(string.Format("Deploying BIOS on {0} resulted in error code {1}", state.nodeIp, returnCode));
                    Debug.WriteLine("Faied bios deploy, error code " + returnCode);
                    Debug.WriteLine("stdout " + stdout);
                    Debug.WriteLine("stderr " + stderr);
                    state.result = resultCode.genericFail;
                }
                else
                {
                    addLogEvent(string.Format("Deployed BIOS successfully to {0}", state.nodeIp));
                    state.result = resultCode.success;
                }

                // All done, now we can power off and return.
                hypervisor_iLo_HTTP hyp = new hypervisor_iLo_HTTP(state.iLoIP, Properties.Settings.Default.iloUsername, Properties.Settings.Default.iloPassword);
                hyp.connect();
                hyp.powerOff();
                hyp.logout();
                lock (connLock)
                {
                    bladeOwnership reqBlade = getBladeByIP(state.nodeIp);
                    reqBlade.currentlyHavingBIOSDeployed = false;
                    reqBlade.lastDeployedBIOS = state.biosxml;
                    reqBlade.updateInDB(conn);
                }
                state.isFinished = true;
            }
            catch (Exception e)
            {
                addLogEvent(string.Format("Deploying BIOS on {0} resulted in exception {1}", state.nodeIp, e.ToString()));
                state.result = resultCode.genericFail;
                state.isFinished = true;
            }
            finally
            {
                // Clean up our temp files (with a retry in case they are in use right now)
                if (tempFilesToCleanUp != null)
                {
                    foreach (string filename in tempFilesToCleanUp)
                        deleteWithRetry(filename);
                }
            }
        }

        private static void deleteWithRetry(string filename)
        {
            while (File.Exists(filename))
            {
                DateTime deadline = DateTime.Now + TimeSpan.FromMinutes(1);
                try
                {
                    File.Delete(filename);
                }
                catch (InvalidOperationException)
                {
                    if (DateTime.Now > deadline)
                        throw;
                    Thread.Sleep(100);
                }
            }
        }

        private static bool retryIfFailedConnect(IAsyncResult ar, biosThreadState state)
        {
            try
            {
                state.biosUpdateSocket.EndConnect(ar);
            }
            catch (SocketException)
            {
                // We failed to connect. Either report failure (if our timeout has expired), or start another connection attempt.
                if (DateTime.Now > state.connectDeadline)
                {
                    state.result = resultCode.genericFail;
                    state.isFinished = true;
                    lock (connLock)
                    {
                        bladeOwnership reqBlade = getBladeByIP(state.nodeIp);
                        reqBlade.currentlyHavingBIOSDeployed = false;
                        reqBlade.updateInDB(conn);

                        state.result = resultCode.genericFail;
                        state.isFinished = true;
                    }
                }
                // Otherwise, just keep retrying.
                state.biosUpdateSocket.BeginConnect(new IPEndPoint(IPAddress.Parse(state.nodeIp), 22), state.onBootFinish, state);
                return true;
            }
            return false;
        }

        private static string writeTempFile(byte[] fileContents, bool convertNewlines = false)
        {
            string filename = Path.GetTempFileName();
            using (FileStream f = File.OpenWrite(filename))
            {
                byte[] biosAsBytes = fileContents;
                if (convertNewlines)
                {
                    string fileASCIIStr = Encoding.ASCII.GetString(fileContents);
                    biosAsBytes = Encoding.ASCII.GetBytes(fileASCIIStr.Replace("\r\n", "\n"));
                }
                f.Write(biosAsBytes, 0, biosAsBytes.Length);
            }
            return filename;
        }
    }

    public class biosThreadState
    {
        public string biosxml;
        public string iLoIP;
        public string nodeIp;
        public bool isFinished;
        public resultCode result;
        public Socket biosUpdateSocket;
        public DateTime connectDeadline;
        public Thread rebootThread;
        public AsyncCallback onBootFinish;

        public biosThreadState(string nodeIp, string iLoIP, string biosxml)
        {
            this.nodeIp = nodeIp;
            this.iLoIP = iLoIP;
            this.biosxml = biosxml;
            isFinished = false;
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

    public class resultCodeAndBIOSConfig
    {
        public resultCode code;
        public string BIOSConfig;

        // For XML de/ser
        // ReSharper disable once UnusedMember.Global
        public resultCodeAndBIOSConfig()
        {
        }

        public resultCodeAndBIOSConfig(resultCode newState)
        {
            this.code = newState;
            this.BIOSConfig = null;
        }

        public resultCodeAndBIOSConfig(biosThreadState state)
        {
            this.code = state.result;
            this.BIOSConfig = state.biosxml;
        }
    }

    public enum resultCode
    {
        success,
        bladeNotFound,
        bladeInUse,
        bladeQueueFull,
        pending,
        alreadyInProgress,
        genericFail,
        noNeedLah
    }
}