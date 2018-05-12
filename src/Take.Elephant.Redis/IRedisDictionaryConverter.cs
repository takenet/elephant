using System.Collections.Generic;
using StackExchange.Redis;

namespace Take.Elephant.Redis
{
    /// <summary>
    /// Defines a dictionary converter service.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IRedisDictionaryConverter<T>
    {
        /// <summary>
        /// Gets the names of the dictionary properties for the type.
        /// </summary>
        /// <value>
        /// The properties.
        /// </value>
        IEnumerable<string> Properties { get; }

        /// <summary>
        /// Creates an instance of <see cref="T"/> from the specified dictionary instance.
        /// </summary>
        /// <param name="dictionary"></param>
        /// <returns></returns>
        T FromDictionary(IDictionary<string, RedisValue> dictionary);

        /// <summary>
        /// Creates a dictionary from the specified <see cref="T"/> instance.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        IDictionary<string, RedisValue> ToDictionary(T value);
    }
}
