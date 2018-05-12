using System;

namespace Take.Elephant.Specialized
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