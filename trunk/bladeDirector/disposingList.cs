using System;
using System.Collections.Generic;

namespace bladeDirector
{
    public class disposingList<T> : List<T>, IDisposable 
        where T: IDisposable
    {
        public disposingList(List<T> cts)
            : base(cts)
        {
            
        }

        public disposingList()
        {
        }

        public disposingList(int count)
            : base(count)
        {
            
        }

        public void Dispose()
        {
            foreach (T element in this)
            {
                element.Dispose();
            }
        }
    }
}