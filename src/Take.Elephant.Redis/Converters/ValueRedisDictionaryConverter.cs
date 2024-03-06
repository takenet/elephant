using System;
using System.Collections.Generic;
using StackExchange.Redis;

namespace Take.Elephant.Redis.Converters
{
    public class ValueRedisDictionaryConverter<T> : IRedisDictionaryConverter<T>
    {
        public const string DEFAULT_VALUE_KEY = "value";
        private readonly string _valueKey;

        public ValueRedisDictionaryConverter()
            : this(DEFAULT_VALUE_KEY)
        {
            
        }

        public ValueRedisDictionaryConverter(string valueKey)
        {
            if (string.IsNullOrWhiteSpace(valueKey)) throw new ArgumentNullException(nameof(valueKey));
            _valueKey = valueKey;
        }

        public IEnumerable<string> Properties  => new[] { _valueKey };

        public T FromDictionary(IDictionary<string, RedisValue> dictionary)
        {
            return dictionary[_valueKey].Cast<T>();
        }

        public IDictionary<string, RedisValue> ToDictionary(T value)
        {
            if (value == null || value.Equals(default(T))) return new Dictionary<string, RedisValue>();

            return new Dictionary<string, RedisValue>()
            {
                {_valueKey, value.ToRedisValue()}
            };
        }
    }
}

