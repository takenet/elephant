using System;
using System.Globalization;
using SmartFormat;

namespace Takenet.Elephant
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
    }
}

