using System.Collections.Generic;
using StackExchange.Redis;

namespace Takenet.SimplePersistence.Redis.Converters
{
    public sealed class LongRedisValueDictionaryConverter : IDictionaryConverter<long>
    {
        public const string VALUE_KEY = "value";

        public IEnumerable<string> Properties => new[] { VALUE_KEY };

        public long FromDictionary(IDictionary<string, object> dictionary)
        {
            return (long)(RedisValue)dictionary[VALUE_KEY];
        }

        public IDictionary<string, object> ToDictionary(long value)
        {
            return new Dictionary<string, object>()
            {
                {VALUE_KEY, value}
            };
        }
    }
}