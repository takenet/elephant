using System;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using SmartFormat;

namespace Take.Elephant
{
    public static class StringExtensions
    {
        public static string ToStringOfCulture(this object obj, CultureInfo culture)
        {
            return
                (obj as IConvertible)?.ToString(culture) ??
                ((obj as IFormattable)?.ToString(null, culture) ??
                obj.ToString());
        }

        public static string ToStringInvariant(this object obj)
            => ToStringOfCulture(obj, CultureInfo.InvariantCulture);

        /// <summary>
        /// Returns the first few characters of the string with a length
        /// specified by the given parameter. If the string's length is less than the 
        /// given length the complete string is returned. If length is zero or 
        /// less an empty string is returned
        /// </summary>
        /// <param name="s">the string to process</param>
        /// <param name="length">Number of characters to return</param>
        /// <returns></returns>
        public static string Left(this string s, int length)
        {
            length = Math.Max(length, 0);
            return s.Length > length ? s.Substring(0, length) : s;
        }

        /// <summary>
        /// Format the string using the source object to populate the named formats.
        /// </summary>
        /// <param name="format"></param>
        /// <param name="source"></param>
        /// <returns></returns>
        public static string Format(this string format, object source) => Smart.Format(format, source);

        /// <summary>
        /// Check if the strings are equals ignoring the casing.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        public static bool EqualsOrdinalIgnoreCase(this string a, string b)
        {
            return string.Equals(a, b, StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Returns the DataMember name of the property that matches the provided propertyname
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="propertyName"></param>
        /// <returns></returns>
        public static string GetPropertyDataMemberName<T>(this string propertyName)
        {
            var property = typeof(T).GetProperties().SingleOrDefault(x =>
                x.Name.EqualsOrdinalIgnoreCase(propertyName));

            if (property == null)
            {
                return String.Empty;
            }

            var dataMemberAttribute = (DataMemberAttribute)property
                .GetCustomAttributes(typeof(DataMemberAttribute), true)
                .FirstOrDefault();

            if (dataMemberAttribute == null)
            {
                return String.Empty;
            }

            return dataMemberAttribute.Name;
        }
    }
}

