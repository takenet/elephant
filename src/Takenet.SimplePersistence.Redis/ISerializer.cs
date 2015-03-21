namespace Takenet.SimplePersistence.Redis
{
    /// <summary>
    /// Defines a type serialization service.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface ISerializer<T>
    {
        /// <summary>
        /// Serializes an instance of <see cref="T"/> into a string.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        string Serialize(T value);

        /// <summary>
        /// Creates an instance of <see cref="T"/> from a string.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        T Deserialize(string value);
    }
}
