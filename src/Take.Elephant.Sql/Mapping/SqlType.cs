using System.Data;

namespace Take.Elephant.Sql.Mapping
{
    /// <summary>
    /// Provides information about a SQL type.
    /// </summary>
    public sealed class SqlType
    { 
        public const int DEFAULT_STRING_LENGTH = 250;
        private readonly int? _length;
  
        public SqlType(DbType type, bool isIdentity = false)
        {
            Type = type;
            IsIdentity = isIdentity;
        }

        public SqlType(DbType type, int length)
            : this(type, false)
        {
            _length = length;
        }

        public SqlType(DbType type, int precision, int scale)
            : this(type, false)
        {
            Precision = precision;
            Scale = scale;
        }

        public DbType Type { get; }

        public int? Length
        {
            get
            {
                if (_length == null)
                {
                    if (Type == DbType.String) return DEFAULT_STRING_LENGTH;
                    if (Type == DbType.Binary) return int.MaxValue;
                }
                return _length;
            }
        }

        public int? Precision { get; }

        public int? Scale { get; }

        public bool IsIdentity { get; }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            return obj is SqlType && Equals((SqlType) obj);
        }

        private bool Equals(SqlType other)
        {
            return _length == other._length && Type == other.Type && Precision == other.Precision && Scale == other.Scale && IsIdentity == other.IsIdentity;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = _length.GetHashCode();
                hashCode = (hashCode * 397) ^ (int)Type;
                hashCode = (hashCode * 397) ^ Precision.GetHashCode();
                hashCode = (hashCode * 397) ^ Scale.GetHashCode();
                hashCode = (hashCode * 397) ^ IsIdentity.GetHashCode();
                return hashCode;
            }
        }

        public override string ToString()
        {
            if (Precision != null)
            {
                if (Scale != null) return $"{Type}({Precision},{Scale})";            
                return $"{Type}({Precision})";
            }

            return Type.ToString();
        }
    }
}
