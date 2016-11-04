namespace bladeDirectorClient
{
    public static class machinePools
    {
        public static readonly iLoHypervisorPool ilo = new iLoHypervisorPool();
        public static readonly VMWareHypervisorPool vmware = new VMWareHypervisorPool();
    }
}