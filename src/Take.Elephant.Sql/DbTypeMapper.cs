using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;

namespace Take.Elephant.Sql
{
    /// <summary>
    /// Provides CLR to SQL type mapping utilities.
    /// </summary>
    public class DbTypeMapper : IDbTypeMapper
    {
        private static readonly Dictionary<Type, DbType> _typeMap;

        static DbTypeMapper()
        {
            _typeMap = new Dictionary<Type, DbType>
            {
                {typeof (byte), DbType.Byte},
                {typeof (sbyte), DbType.SByte},
                {typeof (short), DbType.Int16},
                {typeof (ushort), DbType.UInt16},
                {typeof (int), DbType.Int32},
                {typeof (uint), DbType.UInt32},
                {typeof (long), DbType.Int64},
                {typeof (ulong), DbType.UInt64},
                {typeof (float), DbType.Single},
                {typeof (double), DbType.Double},
                {typeof (decimal), DbType.Decimal},
                {typeof (bool), DbType.Boolean},
                {typeof (string), DbType.String},
                {typeof (char), DbType.StringFixedLength},
                {typeof (Guid), DbType.Guid},
                {typeof (DateTime), DbType.DateTime},
                {typeof (DateTimeOffset), DbType.DateTimeOffset},
                {typeof (TimeSpan), DbType.Time},
                {typeof (byte[]), DbType.Binary},
                {typeof (byte?), DbType.Byte},
                {typeof (sbyte?), DbType.SByte},
                {typeof (short?), DbType.Int16},
                {typeof (ushort?), DbType.UInt16},
                {typeof (int?), DbType.Int32},
                {typeof (uint?), DbType.UInt32},
                {typeof (long?), DbType.Int64},
                {typeof (ulong?), DbType.UInt64},
                {typeof (float?), DbType.Single},
                {typeof (double?), DbType.Double},
                {typeof (decimal?), DbType.Decimal},
                {typeof (bool?), DbType.Boolean},
                {typeof (char?), DbType.StringFixedLength},
                {typeof (Guid?), DbType.Guid},
                {typeof (DateTime?), DbType.DateTime},
                {typeof (DateTimeOffset?), DbType.DateTimeOffset},
                {typeof (TimeSpan?), DbType.Time}
            };
        }

        public static DbType GetDbType(Type type)
        {
            DbType dbType;

            if (!_typeMap.TryGetValue(type, out dbType))
            {
                dbType = DbType.String;
            }

            return dbType;
        }

        public static readonly DbTypeMapper Default = new DbTypeMapper();

        public object ToDbType(object value, DbType type, int? length = null)
        {
            if (value == null)
                return DBNull.Value;
            if (type == DbType.String)
            {
                if (!(value is string))
                {
                    value = value.ToString();
                }

                if (length.HasValue && length.Value < int.MaxValue)
                {
                    value = ((string)value).Left(length.Value);
                }
            }

            return value;
        }

        public object FromDbType(object dbValue, DbType type, Type propertyType)
        {
            if (propertyType.GetTypeInfo().IsGenericType &&
                propertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                propertyType = Nullable.GetUnderlyingType(propertyType);
            }

            if (dbValue.GetType() == propertyType)
            {
                return dbValue;
            }
            if (propertyType.GetTypeInfo().IsEnum)
            {
                var dbValueString = (string)dbValue;
                return Enum.Parse(propertyType, dbValueString);
            }
            if (propertyType == typeof(Uri))
            {
                var dbValueString = (string)dbValue;
                return new Uri(dbValueString);
            }
            if (propertyType == typeof(DateTime) && dbValue is DateTimeOffset)
            {
                return ((DateTimeOffset)dbValue).DateTime;
            }
            if (propertyType == typeof(DateTimeOffset) && dbValue is DateTime)
            {
                return new DateTimeOffset((DateTime)dbValue);
            }
            if (dbValue is string)
            {
                var dbValueString = (string)dbValue;
                var parseFunc = TypeUtil.GetParseFuncForType(propertyType);
                return parseFunc(dbValueString);
            }
            throw new NotSupportedException($"Property type '{propertyType.Name}' is not supported");
        }
    }
}