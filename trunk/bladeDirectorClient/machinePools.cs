namespace bladeDirectorClient
{
    public static class machinePools
    {
        public static string bladeDirectorURL = Properties.Settings.Default.bladeDirectorURL;

        public static iLoHypervisorPool ilo = new iLoHypervisorPool();
        public static VMWareHypervisorPool vmware = new VMWareHypervisorPool();

        public static void reinit()
        {
            bladeDirectorURL = Properties.Settings.Default.bladeDirectorURL;
            ilo = new iLoHypervisorPool();
            vmware = new VMWareHypervisorPool();
        }
    }
}