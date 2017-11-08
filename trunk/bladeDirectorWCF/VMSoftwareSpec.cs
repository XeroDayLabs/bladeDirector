namespace bladeDirectorWCF
{
    public class VMSoftwareSpec
    {
        /// <summary>
        /// Set to the address you want the kernel debugger configured to, or NULL for none
        /// </summary>
        public string debuggerHost = null;

        /// <summary>
        /// Set to the UDP port you want the kernel debugger configured to use, or zero for auto
        /// </summary>
        public ushort debuggerPort = 0;

        /// <summary>
        /// Set to the key you want the kernel debugger configured to use, or NULL for none
        /// </summary>
        public string debuggerKey;
        
        /// <summary>
        /// Set this if you want to add windows user accounts to the system
        /// </summary>
        public userAddRequest[] usersToAdd = null;
    }

}