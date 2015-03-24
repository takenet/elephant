using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Takenet.SimplePersistence.Sql.Mapping
{
    public class KeyValuePropertyTable<TKey, TValue> : KeyValuePropertyTable
    {
        public KeyValuePropertyTable()
            : this(typeof(TKey).Name + typeof(TValue).Name)
        {

        }

        public KeyValuePropertyTable(string tableName)
            : base(tableName, typeof(TKey).GetProperties(BindingFlags.Public | BindingFlags.Instance), typeof(TValue).GetProperties(BindingFlags.Public))
        {
        }
    }

    public class KeyValuePropertyTable : ITable
    {
        public KeyValuePropertyTable(string tableName, PropertyInfo[] keyProperties, PropertyInfo[] valueProperties)
        {
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (keyProperties == null || !keyProperties.Any()) throw new ArgumentNullException(nameof(keyProperties));
            if (valueProperties == null || !valueProperties.Any()) throw new ArgumentNullException(nameof(valueProperties));
            TableName = tableName;
            KeyColumns = keyProperties.Select(p => p.Name).ToArray();
            Columns = keyProperties.Concat(valueProperties).ToDictionary(p => p.Name, p => new SqlType(TypeMapper.GetDbType(p.PropertyType)));
        }

        public string TableName { get; }

        public string[] KeyColumns { get; }

        public IDictionary<string, SqlType> Columns { get; }
    }
}