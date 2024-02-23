using System;

namespace Take.Elephant.Specialized.Cache
{
    public class CacheOptions
    {
        public TimeSpan CacheExpiration { get; set; }

        public TimeSpan CacheFaultTolerance { get; set; }

        public bool ThrowOnCacheWritingExceptions { get; set; } = true;

        // To cache null values when the source returns null or default type value
        // Default is false
        public bool CacheMissingValues { get; set; }
    }
}