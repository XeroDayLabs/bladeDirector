using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;

namespace bladeDirectorWCF
{
    public class miniDumpUtils
    {
        public static bool dumpSelf(string dumpFile)
        {
            IntPtr hProcess = Process.GetCurrentProcess().Handle;
            uint PID = (uint)Process.GetCurrentProcess().Id;
            // Create a crash dump, which we will then pass to windbg for analysis.
            using (FileStream foo = File.OpenWrite(dumpFile))
            {
                MiniDumpExceptionInformation exp = new MiniDumpExceptionInformation();

                exp.ThreadId = 0xffffffff;
                exp.ClientPointers = false;

                if (!MiniDumpWriteDump(hProcess, PID, foo.SafeFileHandle, Option.WithFullMemory, IntPtr.Zero, IntPtr.Zero, IntPtr.Zero))
                {
                    throw new Exception("unable to dump - GLE " + Marshal.GetLastWin32Error());
                }
            }

            return true;
        }

        // All this p/invoke stuff nabbed from The Internet and modified.
        // http://blogs.msdn.com/b/dondu/archive/2010/10/24/writing-minidumps-in-c.aspx
        [DllImport("dbgcore.dll", EntryPoint = "MiniDumpWriteDump", CallingConvention = CallingConvention.StdCall, CharSet = CharSet.Unicode, ExactSpelling = true, SetLastError = true)]
        public static extern bool MiniDumpWriteDump(IntPtr hProcess, uint processId, SafeHandle hFile, Option dumpType, ref MiniDumpExceptionInformation expParam, IntPtr userStreamParam, IntPtr callbackParam);

        [DllImport("dbgcore.dll", EntryPoint = "MiniDumpWriteDump", CallingConvention = CallingConvention.StdCall, CharSet = CharSet.Unicode, ExactSpelling = true, SetLastError = true)]
        public static extern bool MiniDumpWriteDump(IntPtr hProcess, uint processId, SafeHandle hFile, Option dumpType, IntPtr expParam, IntPtr userStreamParam, IntPtr callbackParam);

        [Flags]
        public enum Option : uint
        {
            Normal = 0x00000000,
            WithDataSegs = 0x00000001,
            WithFullMemory = 0x00000002,
            WithHandleData = 0x00000004,
            FilterMemory = 0x00000008,
            ScanMemory = 0x00000010,
            WithUnloadedModules = 0x00000020,
            WithIndirectlyReferencedMemory = 0x00000040,
            FilterModulePaths = 0x00000080,
            WithProcessThreadData = 0x00000100,
            WithPrivateReadWriteMemory = 0x00000200,
            WithoutOptionalData = 0x00000400,
            WithFullMemoryInfo = 0x00000800,
            WithThreadInfo = 0x00001000,
            WithCodeSegs = 0x00002000,
            WithoutAuxiliaryState = 0x00004000,
            WithFullAuxiliaryState = 0x00008000,
            WithPrivateWriteCopyMemory = 0x00010000,
            IgnoreInaccessibleMemory = 0x00020000,
            ValidTypeFlags = 0x0003ffff,
        };

        [StructLayout(LayoutKind.Sequential, Pack = 4)] // Pack=4 is important! So it works also for x64!
        public struct MiniDumpExceptionInformation
        {
            public uint ThreadId;
            public IntPtr ExceptionPointers;

            [MarshalAs(UnmanagedType.Bool)]
            public bool ClientPointers;
        }
    }
}