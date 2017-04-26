using System;
using System.Data.SQLite;

namespace bladeDirector
{
    public class VMHardwareSpec
    {
        public int memoryMB;
        public int cpuCount;

        public VMHardwareSpec()
        {
            // For XML de/ser
        }

        public VMHardwareSpec(int newMemoryMB, int newCPUCount)
        {
            memoryMB = newMemoryMB;
            cpuCount = newCPUCount;
        }

        public VMHardwareSpec(SQLiteDataReader reader)
        {
            if (!(reader["memoryMB"] is System.DBNull))
                memoryMB = Convert.ToInt32((long) reader["memoryMB"]);
            if (!(reader["cpuCount"] is System.DBNull))
                cpuCount = Convert.ToInt32((long)reader["cpuCount"]);
        }
    }
}