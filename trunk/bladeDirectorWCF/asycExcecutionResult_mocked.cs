using hypervisors;

namespace bladeDirectorWCF
{
    public class asycExcecutionResult_mocked : IAsyncExecutionResult
    {
        private executionResult _res;

        public asycExcecutionResult_mocked(executionResult res)
        {
            _res = res;
        }

        public executionResult getResultIfComplete()
        {
            return _res;
        }

        public void Dispose()
        {

        }
    }
}