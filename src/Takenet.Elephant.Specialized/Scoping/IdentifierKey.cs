using System;

namespace Takenet.Elephant.Specialized.Scoping
{
    /// <summary>
    /// Represents an scoped map identifier and a key pair.
    /// </summary>
    public class IdentifierKey
    {
        private string _identifier;
        public const char SEPARATOR = ':';

        /// <summary>
        /// Gets or sets the scoped map identifier.
        /// </summary>
        /// <value>
        /// The identifier.
        /// </value>
        public string Identifier
        {
            get { return _identifier; }
            set
            {
                if (value == null) throw new ArgumentNullException(nameof(value));                
                if (value.IndexOf(SEPARATOR) >= 0) throw new ArgumentException($"The identifier cannot contain the character '{SEPARATOR}'", nameof(value));
                _identifier = value;
            }
        }

        /// <summary>
        /// Gets or sets the key.
        /// </summary>
        /// <value>
        /// The key.
        /// </value>
        public string Key { get; set; }

        /// <summary>
        /// Returns a <see cref="System.String" /> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String" /> that represents this instance.
        /// </returns>
        public override string ToString() => $"{Identifier}{SEPARATOR}{Key}";

        /// <summary>
        /// Parses the specified value.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns></returns>
        /// <exception cref="System.ArgumentNullException"></exception>
        /// <exception cref="System.ArgumentException"></exception>
        public static IdentifierKey Parse(string value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            var separatorIndex = value.IndexOf(SEPARATOR);
            if (separatorIndex < 0) throw new ArgumentException($"The separator '{SEPARATOR}' was not found in the string", nameof(value));

            return new IdentifierKey()
            {
                Identifier = value.Substring(0, separatorIndex),
                Key = separatorIndex + 1 >= value.Length ? 
                        string.Empty : 
                        value.Substring(separatorIndex + 1, value.Length - (separatorIndex + 1))
            };
        }

        /// <summary>
        /// Returns a hash code for this instance.
        /// </summary>
        /// <returns>
        /// A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table. 
        /// </returns>
        public override int GetHashCode() => ToString().GetHashCode();

        /// <summary>
        /// Determines whether the specified <see cref="System.Object" />, is equal to this instance.
        /// </summary>
        /// <param name="obj">The <see cref="System.Object" /> to compare with this instance.</param>
        /// <returns>
        ///   <c>true</c> if the specified <see cref="System.Object" /> is equal to this instance; otherwise, <c>false</c>.
        /// </returns>
        public override bool Equals(object obj)
        {
            if (obj == null) return false;
            return ToString().Equals(obj.ToString());
        }
    }
}