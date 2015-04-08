using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence.Sql
{
    /// <summary>
    /// Provides CLR to SQL type
    /// mapping utilities
    /// </summary>
    public static class TypeMapper
    {
        private static Dictionary<Type, DbType> _typeMap;

        static TypeMapper()
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

        public static object ToDbType(object value, DbType type)
        {
            if (type == DbType.String &&
                !(value is string) &&
                value != null)
            {
                return value.ToString();
            }

            return value;
        }

        public static object FromDbType(object dbValue, DbType type, Type propertyType)
        {
            if (propertyType.IsGenericType &&
                propertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                propertyType = Nullable.GetUnderlyingType(propertyType);
            }

            if (dbValue.GetType() == propertyType)
            {
                return dbValue;
            }
            else if (propertyType.IsEnum)
            {
                var dbValueString = (string)dbValue;
                return Enum.Parse(propertyType, dbValueString);
            }
            else if (dbValue is string)
            {
                var dbValueString = (string)dbValue;
                var parseFunc = TypeUtil.GetParseFuncForType(propertyType);
                return parseFunc(dbValueString);
            }
            else
            {
                throw new NotSupportedException("Property type '{type}' is not supported".Format(new { type = propertyType.Name }));
            }
        }

    }
}
