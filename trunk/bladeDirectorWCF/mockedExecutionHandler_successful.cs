using System;
using System.Collections.Generic;
using System.Threading;
using hypervisors;

namespace bladeDirectorWCF
{
    public class mockedExecutionHandler_failOnFirstTry : mockedExecutionHandler_successful
    {
        private List<string> executedCommands = new List<string>();

        public override executionResult callMockedExecutionHandler(hypervisor sender, string command, string args, string workingdir, cancellableDateTime deadline)
        {
            string commandLine = command + " " + args;

            if (executedCommands.Contains(commandLine))
                return base.callMockedExecutionHandler(sender, commandLine, args, workingdir, deadline);

            throw new hypervisorExecutionException("Injected fault");
        }        
    }

    public class mockedExecutionHandler_successfulButSlow : mockedExecutionHandler_successful
    {
        public override executionResult callMockedExecutionHandler(hypervisor sender, string command, string args, string workingdir, cancellableDateTime deadline)
        {
            Thread.Sleep(TimeSpan.FromSeconds(10));
            return base.callMockedExecutionHandler(sender, command, args, workingdir, deadline);
        }
    }

    public class mockedExecutionHandler_successful : mockedExecutionHandler
    {
        public override executionResult callMockedExecutionHandler(hypervisor sender, string command, string args, string workingdir, cancellableDateTime deadline)
        {
            string commandLine = command + " " + args;
            switch (commandLine)
            {
                case "bash ~/applyBIOS.sh":
                    return new executionResult("bios stuff", "", 0);
                case "esxcfg-nas -l":
                    return new executionResult("esxivms is /mnt/SSDs/esxivms from store.xd.lan mounted available", null, 0);
                case @"C:\windows\system32\cmd /c shutdown -s -f -t 01":
                    sender.powerOff();
                    return new executionResult("", "", 0);
            }

            if (commandLine.StartsWith("vim-cmd vmsvc/power.off `vim-cmd vmsvc/getallvms | grep"))
                return new executionResult("", null, 0);
            if (commandLine.StartsWith("vim-cmd vmsvc/unregister `vim-cmd vmsvc/getallvms"))
                return new executionResult("", null, 0);
            if (commandLine.StartsWith("rm  -rf /vmfs/volumes/esxivms/"))
                return new executionResult("", null, 0);
            if (commandLine.StartsWith("cp  -R /vmfs/volumes/esxivms/PXETemplate /vmfs/volumes/esxivms/"))
                return new executionResult("", null, 0);
            if (commandLine.StartsWith("sed  -e "))
                return new executionResult("", null, 0);
            if (commandLine.StartsWith("vim-cmd  solo/registervm /vmfs/volumes/esxivms/"))
                return new executionResult("", null, 0);
            if (commandLine.StartsWith("cmd.exe /c c:\\deployed.bat "))
                return new executionResult("", null, 0);

            throw new Exception("executed unexpected command " + commandLine);
        }
    }
}