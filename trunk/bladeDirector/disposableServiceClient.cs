using System;
using System.ServiceModel;

namespace bladeDirector
{
    public class disposableServiceClient<T> : IDisposable
        where T : ICommunicationObject, new()
    {
        public readonly T commObj;

        public disposableServiceClient()
        {
            commObj = new T();
            commObj.Open();
        }

        public void Dispose()
        {
            // This is MS's recommended pattern for closing ICommunicationObjects.
            try
            {
                commObj.Close();
            }
            catch (CommunicationException)
            {
                commObj.Abort();
            }
            catch (TimeoutException)
            {
                commObj.Abort();
            }
            catch (Exception)
            {
                commObj.Abort();
                throw;
            }
            ((IDisposable)commObj).Dispose();
        }
    }
}