using System.Data;

namespace Take.Elephant.Sql.Mapping
{
    /// <summary>
    /// Provides information about a SQL type.
    /// </summary>
    public sealed class SqlType
    {
        public const int DEFAULT_STRING_LENGTH = 250;
        public const int DEFAULT_DECIMAL_PRECISION = 15;
        public const int DEFAULT_DECIMAL_SCALE = 3;
        private readonly int? _length;
        private readonly int? _precision;
        private readonly int? _scale;

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
            _precision = precision;
            _scale = scale;
        }

        public DbType Type { get; }

        public int? Length
        {
            get
            {
                if (_length == null)
                {
                    if (Type == DbType.String)
                        return DEFAULT_STRING_LENGTH;
                    if (Type == DbType.Binary)
                        return int.MaxValue;
                }

                return _length;
            }
        }

        public int? Precision
        {
            get
            {
                if (_precision == null)
                {
                    if (Type == DbType.Decimal)
                        return DEFAULT_DECIMAL_PRECISION;
                }

                return _precision;
            }
        }

        public int? Scale
        {
            get
            {
                if (_scale == null)
                {
                    if (Type == DbType.Decimal)
                        return DEFAULT_DECIMAL_SCALE;
                }

                return _scale;
            }
        }

        public bool IsIdentity { get; }

        public override bool Equals(object obj)
        {
            if (obj is null)
                return false;
            if (this == obj)
                return true;

            return obj is SqlType type && Equals(type);
        }

        private bool Equals(SqlType other)
        {
            return _length == other._length
                && Type == other.Type
                && Precision == other.Precision
                && Scale == other.Scale
                && IsIdentity == other.IsIdentity;
        }

        public override int GetHashCode() => System.HashCode.Combine(_length, Type, Precision, Scale, IsIdentity);

        public override string ToString()
        {
            if (Precision != null)
            {
                if (Scale != null) 
                {
                  return $"{Type}({Precision},{Scale})";
                }

                return $"{Type}({Precision})";
            }

            return Type.ToString();
        }
    }
}