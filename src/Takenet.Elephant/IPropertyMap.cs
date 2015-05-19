using System.Threading.Tasks;

namespace Takenet.Elephant
{
    /// <summary>
    /// Defines a map that allows the insertion and update of specific properties of the value document.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public interface IPropertyMap<TKey, TValue> : IMap<TKey, TValue>
    {
        /// <summary>
        /// Adds a property to a map item.
        /// If the map item doesn't exists, it will be created.
        /// If the property already exists, it will be overwritten.
        /// </summary>
        /// <typeparam name="TProperty"></typeparam>
        /// <param name="key"></param>
        /// <param name="propertyName"></param>
        /// <param name="propertyValue"></param>
        /// <returns></returns>
        Task SetPropertyValueAsync<TProperty>(TKey key, string propertyName, TProperty propertyValue);

        /// <summary>
        /// Gets a property value for the item in the specific key. 
        /// If the item doesn't exists, returns the default property type value.
        /// </summary>
        /// <typeparam name="TProperty"></typeparam>
        /// <param name="key"></param>
        /// <param name="propertyName"></param>
        /// <returns></returns>
        Task<TProperty> GetPropertyValueOrDefaultAsync<TProperty>(TKey key, string propertyName);

        /// <summary>
        /// Merge the properties of the map item.
        /// Properties with the default values of the property type will be ignored.
        /// If the map item doesn't exists, it will be created.
        /// </summary>
        /// <param name="key">The map item key.</param>
        /// <param name="value">The partial or complete item value.</param>
        /// <returns></returns>
        Task MergeAsync(TKey key, TValue value);
    }
}
