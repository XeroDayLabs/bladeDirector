using System;
using System.Collections.Generic;

namespace bladeDirectorWCF
{
    public class disposingList<T> : List<T>, IDisposable 
        where T: IDisposable
    {
        private bool isDisposed = false;
        private string allocationStack;

        public disposingList(List<T> cts)
            : base(cts)
        {
            allocationStack = Environment.StackTrace;
        }

        public disposingList()
        {
            allocationStack = Environment.StackTrace;
        }

        public disposingList(int count)
            : base(count)
        {
            allocationStack = Environment.StackTrace;
        }

        public void Dispose()
        {
            foreach (T element in this)
            {
                element.Dispose();
            }
            isDisposed = true;
        }

        ~disposingList()
        {
            if (!isDisposed)
                throw new Exception("disposingList was leaked! Allocation stack was " + allocationStack);
        }
    }
}