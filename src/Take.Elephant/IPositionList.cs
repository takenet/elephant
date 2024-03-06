using System.Threading.Tasks;

namespace Take.Elephant
{
    /// <summary>
    /// Defines a list of items that allows adding an item to start.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IPositionList<T> : IList<T>
    {
        /// <summary>
        /// Adds an item to beginning of the list.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <param name="position">Index where value should be inserted</param>
        /// <returns></returns>
        Task AddToPositionAsync(T value, int position);
    }
}