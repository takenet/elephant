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
            _typeMap = new Dictionary<Type, DbType>();
            _typeMap[typeof(byte)] = DbType.Byte;
            _typeMap[typeof(sbyte)] = DbType.SByte;
            _typeMap[typeof(short)] = DbType.Int16;
            _typeMap[typeof(ushort)] = DbType.UInt16;
            _typeMap[typeof(int)] = DbType.Int32;
            _typeMap[typeof(uint)] = DbType.UInt32;
            _typeMap[typeof(long)] = DbType.Int64;
            _typeMap[typeof(ulong)] = DbType.UInt64;
            _typeMap[typeof(float)] = DbType.Single;
            _typeMap[typeof(double)] = DbType.Double;
            _typeMap[typeof(decimal)] = DbType.Decimal;
            _typeMap[typeof(bool)] = DbType.Boolean;
            _typeMap[typeof(string)] = DbType.String;
            _typeMap[typeof(char)] = DbType.StringFixedLength;
            _typeMap[typeof(Guid)] = DbType.Guid;
            _typeMap[typeof(DateTime)] = DbType.DateTime;
            _typeMap[typeof(DateTimeOffset)] = DbType.DateTimeOffset;
            _typeMap[typeof(byte[])] = DbType.Binary;
            _typeMap[typeof(byte?)] = DbType.Byte;
            _typeMap[typeof(sbyte?)] = DbType.SByte;
            _typeMap[typeof(short?)] = DbType.Int16;
            _typeMap[typeof(ushort?)] = DbType.UInt16;
            _typeMap[typeof(int?)] = DbType.Int32;
            _typeMap[typeof(uint?)] = DbType.UInt32;
            _typeMap[typeof(long?)] = DbType.Int64;
            _typeMap[typeof(ulong?)] = DbType.UInt64;
            _typeMap[typeof(float?)] = DbType.Single;
            _typeMap[typeof(double?)] = DbType.Double;
            _typeMap[typeof(decimal?)] = DbType.Decimal;
            _typeMap[typeof(bool?)] = DbType.Boolean;
            _typeMap[typeof(char?)] = DbType.StringFixedLength;
            _typeMap[typeof(Guid?)] = DbType.Guid;
            _typeMap[typeof(DateTime?)] = DbType.DateTime;
            _typeMap[typeof(DateTimeOffset?)] = DbType.DateTimeOffset;
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
