using System;
using System.Globalization;

namespace Take.Elephant.Tests
{
    public class Item
    {
        public const string COMPARISON_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

        public string StringProperty { get; set; }

        public int IntegerProperty { get; set; }

        public Guid GuidProperty { get; set; }

        public Uri UriProperty { get; set; }

        public decimal DecimalProperty { get; set; }

        public float FloatProperty { get; set; }

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

        public bool BooleanProperty { get; set; }

        public string RandomProperty { get; set; }

        public bool? BooleanNullProperty { get; set; }

        public override string ToString()
        {
            return $"{StringProperty ?? "<null>"};{IntegerProperty};{GuidProperty};{UriProperty};{DateProperty.ToString(COMPARISON_DATE_FORMAT, CultureInfo.InvariantCulture)};{Select};{BooleanProperty};{string.Format("{0:0.000}", DecimalProperty)};{FloatProperty};{RandomProperty ?? "<null>"}";
        }

        public static Item Parse(string s)
        {
            if (s == null)
                throw new ArgumentNullException(nameof(s));

            var values = s.Split(';');
            return new Item
            {
                StringProperty = values[0] == "<null>" ? null : values[0],
                IntegerProperty = int.Parse(values[1]),
                GuidProperty = Guid.Parse(values[2]),
                UriProperty = new Uri(values[3]),
                DateProperty = DateTimeOffset.Parse(values[4], CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal),
                Select = (ItemOptions)Enum.Parse(typeof(ItemOptions), values[5]),
                BooleanProperty = bool.Parse(values[6]),
                DecimalProperty = decimal.Parse(values[7]),
                FloatProperty = float.Parse(values[8]),
                RandomProperty = values[9] == "<null>" ? null : values[9],
            };
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj.GetType() != this.GetType())
                return false;
            return Equals((Item)obj);
        }

        protected bool Equals(Item other) => ToString().Equals(other.ToString());

        public override int GetHashCode() => ToString().GetHashCode();
    }

    public enum ItemOptions
    {
        Option1,
        Option2,
        Option3
    }
}