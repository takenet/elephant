using System;

namespace Takenet.Elephant.Samples
{
    public class Data
    {

        public string Name { get; set; }

        public int Value { get; set; }


        protected bool Equals(Data other)
        {
            return string.Equals(Name, other.Name) && Value == other.Value;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Name?.GetHashCode() ?? 0)*397) ^ Value;
            }
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((Data) obj);
        }

        public override string ToString()
        {
            return $"{Name}:{Value}";
        }

        public static Data Parse(string value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            var values = value.Split(':');
            if (values.Length < 2) throw new ArgumentException(@"Invalid data value", nameof(value));

            return new Data()
            {
                Name = values[0],
                Value = int.Parse(values[1])
            };
        }
    }
}