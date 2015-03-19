using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence
{
    /// <summary>
    /// Object util extensions.
    /// </summary>
    public static class ObjectExtensions
    {
        /// <summary>
        /// Creates a completed task for the value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value"></param>
        /// <returns></returns>
        public static Task<T> AsCompletedTask<T>(this T value)
        {
            return Task.FromResult(value);
        }
    }
}
