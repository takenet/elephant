using System;

namespace Take.Elephant.Specialized.Cache
{
    public class CacheOptions
    {
        public TimeSpan CacheExpiration { get; set; }

        public TimeSpan CacheFaultTolerance { get; set; }

        public bool ThrowOnCacheWritingExceptions { get; set; } = true;
    }
}