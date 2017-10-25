using System;
using System.ComponentModel;
using System.Net;
using System.Runtime.InteropServices;

namespace bladeDirectorWCF
{
    public class ipUtils
    {
        [DllImport("iphlpapi.dll", SetLastError = true, CallingConvention = CallingConvention.StdCall)]
        private static extern int GetBestRoute2(IntPtr InterfaceLUID, int InterfaceIndex, IntPtr SourceAddress,
            sockaddr_inet destAddress, UInt32 AddresssortOptions, ref mib_ipforward_row2 bestRoute,
            ref sockaddr_inet bestSourceAddress);

        public static IPAddress getBestRouteTo(IPAddress destIPAddress)
        {
            sockaddr_inet dest = new sockaddr_inet();
            dest.sin_family = (ushort) destIPAddress.AddressFamily;
            dest.s_addr = (uint) byteswap(destIPAddress.Address);
            dest.sin_zero = 0;

            mib_ipforward_row2 bestRoute = new mib_ipforward_row2();
            sockaddr_inet bestSrc = new sockaddr_inet();
            int res = GetBestRoute2(IntPtr.Zero, 0, IntPtr.Zero, dest, 0, ref bestRoute, ref bestSrc);
            if (res != 0)
                throw new Win32Exception(res);

            return new IPAddress(bestSrc.s_addr);
        }

        private static long byteswap(long src)
        {
            long dst = 0;

            dst |= (src >> 24);
            dst |= (src >> 16) << 8;
            dst |= (src >> 4) << 16;
            dst |= (src >> 8) << 24;

            return dst;
        }


        [StructLayout(LayoutKind.Explicit)]
        public struct mib_ipforward_row2
        {
            [FieldOffset(0x00)] public UInt64 InterfaceLuid;
            [FieldOffset(0x08)] public UInt32 InterfaceIndex;
            [FieldOffset(0x0c)] public UInt16 DestinationPrefix_family;
            [FieldOffset(0x0e)] public UInt16 DestinationPrefix_port;
            [FieldOffset(0x10)] public UInt32 DestinationPrefix_addr;
            [FieldOffset(0x14)] public UInt64 DestinationPrefix_zeros;
            [FieldOffset(0x20)] public Byte DestinationPrefix_prefixLen;
            [FieldOffset(0x2c)] public UInt16 NextHop_family;
            [FieldOffset(0x2e)] public UInt16 NextHop_port;
            [FieldOffset(0x31)] public UInt32 NextHop_addr;
            [FieldOffset(0x29)] public UInt64 NextHop_zero;
            [FieldOffset(0x48)] public UInt32 SitePrefixLength;
            [FieldOffset(0x4C)] public UInt32 ValidLifetime;
            [FieldOffset(0x50)] public UInt32 PreferredLifetime;
            [FieldOffset(0x54)] public UInt32 Metric;
            [FieldOffset(0x58)] public UInt32 Protocol;
            [FieldOffset(0x5C)] public Byte Loopback;
            [FieldOffset(0x5D)] public Byte AutoconfigureAddress;
            [FieldOffset(0x5E)] public Byte Publish;
            [FieldOffset(0x5F)] public Byte Immortal;
            [FieldOffset(0x60)] public UInt32 Age;
            [FieldOffset(0x63)] public UInt32 Origin;
        }

        public struct IP_ADDRESS_PREFIX
        {
            public sockaddr_inet Prefix;
            public Byte PrefixLength;
        };

        [StructLayout(LayoutKind.Explicit)]
        public struct sockaddr_inet
        {
            [FieldOffset(0)] public UInt16 sin_family;

            [FieldOffset(2)] public UInt16 sin_port;

            [FieldOffset(4)] public UInt32 s_addr;

            [FieldOffset(8)] public UInt64 sin_zero;
        }
    }
}