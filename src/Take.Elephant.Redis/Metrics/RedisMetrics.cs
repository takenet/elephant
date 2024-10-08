using System.Diagnostics.Metrics;

namespace Take.Elephant.Redis.Metrics
{
    /// <summary>
    /// Metrics for the Redis implementation.
    /// </summary>
    public static class RedisMetrics
    {
        private static readonly Meter _meter = new Meter("Take.Elephant.Redis");

        public static readonly Counter<int> StringMapAddCounter =
            _meter.CreateCounter<int>("elephant.redis.string_map.add", "operations",
                "Number of adds to redis with expiration in the string map");
    }
}