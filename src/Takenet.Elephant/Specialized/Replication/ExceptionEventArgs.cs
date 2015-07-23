using System;

namespace Takenet.Elephant.Specialized.Replication
{
    public class ExceptionEventArgs : EventArgs
    {
        public ExceptionEventArgs(Exception exception)
        {
            if (exception == null) throw new ArgumentNullException(nameof(exception));
            Exception = exception;
        }

        public Exception Exception { get; }
    }
}