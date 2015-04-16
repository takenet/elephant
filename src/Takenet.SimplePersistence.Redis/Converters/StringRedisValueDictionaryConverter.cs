using System.Collections.Generic;
using StackExchange.Redis;

namespace Takenet.SimplePersistence.Redis.Converters
{
    public sealed class StringRedisValueDictionaryConverter : IDictionaryConverter<string>
    {
        public const string VALUE_KEY = "value";

        public IEnumerable<string> Properties => new[] { VALUE_KEY };

        public string FromDictionary(IDictionary<string, object> dictionary)
        {
            return (RedisValue)dictionary[VALUE_KEY];
        }

        public IDictionary<string, object> ToDictionary(string value)
        {
            return new Dictionary<string, object>()
            {
                {VALUE_KEY, value}
            };
        }
    }
}