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
    }
}