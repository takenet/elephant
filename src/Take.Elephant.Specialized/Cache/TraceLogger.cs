using System;
using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Take.Elephant.Specialized.Cache
{
    public class TraceLogger : ILogger
    {
        public IDisposable BeginScope<TState>(TState state) => throw new NotImplementedException();

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            switch (logLevel)
            {
                case LogLevel.Error:
                    Trace.TraceError(formatter(state, exception));
                    break;

                case LogLevel.Warning:
                    Trace.TraceWarning(formatter(state, exception));
                    break;

                default:
                    Trace.TraceInformation(formatter(state, exception));
                    break;

            }
        }
    }
}