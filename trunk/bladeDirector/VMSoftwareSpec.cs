namespace bladeDirector
{
    public class VMSoftwareSpec
    {
        /// <summary>
        /// Set to the address you want the kernel debugger configured to, or NULL for none
        /// </summary>
        public string debuggerHost = null;

        /// <summary>
        /// Set to the UDP port you want the kernel debugger configured to use, or zero for none
        /// </summary>
        public ushort debuggerPort = 0;

        /// <summary>
        /// Set to the key you want the kernel debugger configured to use, or NULL for none
        /// </summary>
        public string debuggerKey;

        /// <summary>
        /// Set this to delete any existing iscsi images and re-create them before a system boots
        /// </summary>
        public bool forceRecreate = false;
    }
}