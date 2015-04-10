using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Takenet.SimplePersistence.Redis
{
    public static class RedisValueExtensions
    {
        public static object Cast(this RedisValue value, Type type)
        {
            if (type == null) throw new ArgumentNullException(nameof(type));
            if (type == typeof(int)) return (int)value;            
            if (type == typeof(int?)) return (int?)value;            
            if (type == typeof(long)) return (long)value;            
            if (type == typeof(long?)) return (long?)value;
            if (type == typeof(bool)) return (bool)value;            
            if (type == typeof(bool?)) return (bool?)value;            
            if (type == typeof(byte[])) return (byte[])value;            
            if (type == typeof(string)) return (string)value;            
            throw new NotSupportedException($"The property type '{value.GetType()}'  is not supported");
        }

        public static T Cast<T>(this RedisValue value)
        {
            return (T)Cast(value, typeof(T));
        }

        public static RedisValue ToRedisValue(this object value)
        {
            RedisValue redisValue;

            if (value is int)
            {
                redisValue = (int)value;
            }
            else if (value is long)
            {
                redisValue = (long)value;
            }
            else if (value is bool)
            {
                redisValue = (bool)value;
            }
            else if (value is byte[])
            {
                redisValue = (byte[])value;
            }
            else if (value is string)
            {
                redisValue = (string)value;
            }
            else
            {
                throw new NotSupportedException(string.Format("The property type '{0}'  is not supported", value.GetType()));
            }

            return redisValue;

        }

    }
}
