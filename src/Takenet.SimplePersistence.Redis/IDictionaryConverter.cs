using System.Collections.Generic;

namespace Takenet.SimplePersistence.Redis
{
    /// <summary>
    /// Defines a dictionary converter service.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IDictionaryConverter<T>
    {
        /// <summary>
        /// Creates an instance of <see cref="T"/> from the specified dictionary instance.
        /// </summary>
        /// <param name="dictionary"></param>
        /// <returns></returns>
        T FromDictionary(IDictionary<string, object> dictionary);

        /// <summary>
        /// Creates a dicitonary from the specified <see cref="T"/> instance.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        IDictionary<string, object> ToDictionary(T value);
    }
}
