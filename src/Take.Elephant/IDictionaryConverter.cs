using System.Collections.Generic;

namespace Take.Elephant
{
    /// <summary>
    /// Defines a dictionary converter service.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IDictionaryConverter<T>
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
        T FromDictionary(IDictionary<string, object> dictionary);

        /// <summary>
        /// Creates a dictionary from the specified <see cref="T"/> instance.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        IDictionary<string, object> ToDictionary(T value);
    }
}
