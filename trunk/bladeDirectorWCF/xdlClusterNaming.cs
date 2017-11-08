using System.Net;

namespace bladeDirectorWCF
{
    public static class xdlClusterNaming
    {
        public static string makeVMIP(string bladeIP, vmSpec newVM)
        {
            byte[] VMServerIPBytes = IPAddress.Parse(bladeIP).GetAddressBytes();
            return "172.17." + (28 + VMServerIPBytes[3]) + "." + newVM.indexOnServer;
        }

        public static string makeiSCSIIP(string bladeIP, vmSpec newVM)
        {
            byte[] VMServerIPBytes = IPAddress.Parse(bladeIP).GetAddressBytes();
            return "10.0." + (28 + VMServerIPBytes[3]) + "." + newVM.indexOnServer;
        }

        public static string makeEth0MAC(string bladeIP, vmSpec newVM)
        {
            byte[] VMServerIPBytes = IPAddress.Parse(bladeIP).GetAddressBytes();
            return newVM.eth0MAC = "00:50:56:00:" + (VMServerIPBytes[3] - 100).ToString("D2") + ":" + newVM.indexOnServer.ToString("D2");
        }

        public static string makeEth1MAC(string bladeIP, vmSpec newVM)
        {
            byte[] VMServerIPBytes = IPAddress.Parse(bladeIP).GetAddressBytes();
            return "00:50:56:01:" + (VMServerIPBytes[3] - 100).ToString("D2") + ":" + newVM.indexOnServer.ToString("D2");
        }

        public static string makeVMName(string bladeIP, int VMIndexOnServer)
        {
            byte[] VMServerIPBytes = IPAddress.Parse(bladeIP).GetAddressBytes();
            return "VM_" + (VMServerIPBytes[3] - 100).ToString("D2") + "_" + VMIndexOnServer.ToString("D2");            
        }

        public static ushort makeVMKernelDebugPort(string bladeIP, int VMIndexOnServer)
        {
            byte[] VMServerIPBytes = IPAddress.Parse(bladeIP).GetAddressBytes();
            return (ushort)(50000 + ((VMServerIPBytes[3] - 100) * 100) + VMIndexOnServer);
        }

        public static string makeBladeIP(int nodeIndex)
        {
            return "172.17.129." + (nodeIndex + 100);
        }

        public static string makeBladeISCSIIP(int nodeIndex)
        {
            return "10.0.0." + (nodeIndex + 100);
        }

        public static string makeBladeILOIP(int nodeIndex)
        {
            return "172.17.2." + (nodeIndex + 100);
        }

        public static ushort makeBladeKernelDebugPort(int nodeIndex)
        {
            // physical blades debug port is 599xx.
            return (ushort)(59900 + nodeIndex);
        }

        public static string makeBladeFriendlyName(int nodeIndex)
        {
            return "blade_" + nodeIndex.ToString("D2");
        }
    }
}