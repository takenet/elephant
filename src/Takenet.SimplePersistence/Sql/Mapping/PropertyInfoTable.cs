using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Takenet.SimplePersistence.Sql.Mapping
{
    public class PropertyInfoTable : ITable
    {
        public PropertyInfoTable(string name, string[] keyColumns, PropertyInfo[] properties)
        {
            if (string.IsNullOrWhiteSpace(name)) throw new ArgumentNullException(nameof(name));
            if (keyColumns == null || !keyColumns.Any()) throw new ArgumentNullException(nameof(keyColumns));
            if (properties == null || !properties.Any()) throw new ArgumentNullException(nameof(properties));
            Name = name;
            KeyColumns = keyColumns;
            Columns = properties.ToSqlColumns();

            var invalidKey = KeyColumns.FirstOrDefault(k => !Columns.ContainsKey(k));
            if (invalidKey != null)
                throw new ArgumentException($"The key column '{invalidKey}' is not part of the table columns", nameof(keyColumns));
        }

        public string Name { get; }

        public string[] KeyColumns { get; }

        public IDictionary<string, SqlType> Columns { get; }
    }
}