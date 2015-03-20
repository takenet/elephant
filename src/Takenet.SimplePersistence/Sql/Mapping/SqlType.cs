using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence.Sql.Mapping
{
    /// <summary>
    /// Provides information about a SQL type.
    /// </summary>
    public sealed class SqlType
    {
        public const string MAX_LENGTH = "MAX";
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
                if (_length == null && Type == DbType.String)
                {
                    return DEFAULT_STRING_LENGTH;
                }
                return _length;
            }
        }

        public int? Precision { get; }

        public int? Scale { get; }

        public bool IsIdentity { get; }
    }
}
