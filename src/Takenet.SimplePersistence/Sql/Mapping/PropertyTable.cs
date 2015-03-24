using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Takenet.SimplePersistence.Sql.Mapping
{
    public class PropertyTable<T> : PropertyTable
    {
        public PropertyTable()
            : this(typeof(T).Name, new[] {  typeof(T).Name + "Id" })
        {

        }

        public PropertyTable(string tableName, string[] keyColumns)
            : base(tableName, keyColumns, typeof (T).GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            
        }
    }

    public class PropertyTable : ITable
    {
        public PropertyTable(string tableName, string[] keyColumns, PropertyInfo[] properties)
        {
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (keyColumns == null || !keyColumns.Any()) throw new ArgumentNullException(nameof(keyColumns));
            if (properties == null || !properties.Any()) throw new ArgumentNullException(nameof(properties));
            TableName = tableName;
            KeyColumns = keyColumns;
            Columns = properties.ToDictionary(p => p.Name, p => new SqlType(TypeMapper.GetDbType(p.PropertyType)));

            var invalidKey = KeyColumns.FirstOrDefault(k => !Columns.ContainsKey(k));
            if (invalidKey != null)
                throw new ArgumentException($"The key column '{invalidKey}' is not part of the table columns", nameof(keyColumns));
        }

        public string TableName { get; }

        public string[] KeyColumns { get; }

        public IDictionary<string, SqlType> Columns { get; }
    }
}