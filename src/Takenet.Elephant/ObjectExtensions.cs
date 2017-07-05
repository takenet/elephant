using System;
using System.Reflection;
using System.Threading.Tasks;

namespace Takenet.Elephant
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

        /// <summary>
        /// Determines if the value is a default value of the specified type.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <param name="type">The type.</param>
        /// <param name="ignoreIfEnum">if set to <c>true</c> [ignore if enum].</param>
        /// <returns></returns>
        public static bool IsDefaultValueOfType(this object value, Type type, bool ignoreIfEnum = true)
        {
            if (value == null) return true;
            if (ignoreIfEnum && type.GetTypeInfo().IsEnum) return false;
            return value.Equals(type.GetDefaultValue());
        }
    }
}
