using System;

namespace Takenet.Elephant.Tests
{
    public class Item
    {
        public string StringProperty { get; set; }

        public int IntegerProperty { get; set; }

        public Guid GuidProperty { get; set; }

        public Uri UriProperty { get; set; }

        public DateTime DateProperty { get; set; }

        public override string ToString()
        {
            return $"{StringProperty};{IntegerProperty};{GuidProperty};{UriProperty};{DateProperty}";
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
                DateProperty = DateTime.Parse(values[4])
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
                   DateProperty.ToLongDateString().Equals(other.DateProperty.ToLongDateString());
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (StringProperty != null ? StringProperty.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ IntegerProperty;
                hashCode = (hashCode * 397) ^ GuidProperty.GetHashCode();
                hashCode = (hashCode * 397) ^ UriProperty.GetHashCode();
                hashCode = (hashCode * 397) ^ DateProperty.ToLongDateString().GetHashCode();
                return hashCode;
            }
        }
    }
}