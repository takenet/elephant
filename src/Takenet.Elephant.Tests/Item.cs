using System;
using System.Globalization;

namespace Takenet.Elephant.Tests
{
    public class Item
    {
        public const string COMPARISON_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

        public string StringProperty { get; set; }

        public int IntegerProperty { get; set; }

        public Guid GuidProperty { get; set; }

        public Uri UriProperty { get; set; }

        private DateTimeOffset _dateProperty;
        public DateTimeOffset DateProperty
        {
            get { return _dateProperty; }
            set
            {
                var utcValue = value.ToUniversalTime();
                _dateProperty = new DateTimeOffset(utcValue.Year, utcValue.Month, utcValue.Day, utcValue.Hour, utcValue.Minute, utcValue.Second, utcValue.Offset);
            }
        }

        public ItemOptions Select { get; set; }

        public override string ToString()
        {
            return $"{StringProperty};{IntegerProperty};{GuidProperty};{UriProperty};{DateProperty.ToString(COMPARISON_DATE_FORMAT, CultureInfo.InvariantCulture)};{Select}";
        }

        public static Item Parse(string s)
        {
            if (s == null) throw new ArgumentNullException(nameof(s));

            var values = s.Split(';');
            return new Item
            {
                StringProperty = values[0],
                IntegerProperty = int.Parse(values[1]),
                GuidProperty = Guid.Parse(values[2]),
                UriProperty = new Uri(values[3]),
                DateProperty = DateTimeOffset.Parse(values[4], CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal),
                Select = (ItemOptions)Enum.Parse(typeof(ItemOptions), values[5])
            };
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((Item) obj);
        }

        protected bool Equals(Item other)
        {
            return string.Equals(StringProperty, other.StringProperty) &&
                   IntegerProperty == other.IntegerProperty &&
                   GuidProperty.Equals(other.GuidProperty) &&
                   UriProperty.Equals(other.UriProperty) &&
                   DateProperty.ToUniversalTime()
                       .ToString(COMPARISON_DATE_FORMAT, CultureInfo.InvariantCulture)
                       .Equals(other.DateProperty.ToString(COMPARISON_DATE_FORMAT, CultureInfo.InvariantCulture)) &&
                   Select.Equals(other.Select);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = StringProperty?.GetHashCode() ?? 0;
                hashCode = (hashCode * 397) ^ IntegerProperty;
                hashCode = (hashCode * 397) ^ GuidProperty.GetHashCode();
                hashCode = (hashCode * 397) ^ UriProperty.GetHashCode();
                hashCode = (hashCode * 397) ^ DateProperty.ToString(COMPARISON_DATE_FORMAT, CultureInfo.InvariantCulture).GetHashCode();
                hashCode = (hashCode * 397) ^ Select.GetHashCode();
                return hashCode;
            }
        }
    }

    public enum ItemOptions
    {
        Option1,
        Option2,
        Option3
    }
}