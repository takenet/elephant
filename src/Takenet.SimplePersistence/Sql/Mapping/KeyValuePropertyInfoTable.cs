using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Takenet.SimplePersistence.Sql.Mapping
{
    public class KeyValuePropertyInfoTable : ITable
    {
        public KeyValuePropertyInfoTable(string name, PropertyInfo[] keyProperties, PropertyInfo[] valueProperties)
        {
            if (string.IsNullOrWhiteSpace(name)) throw new ArgumentNullException(nameof(name));
            if (keyProperties == null || !keyProperties.Any()) throw new ArgumentNullException(nameof(keyProperties));
            if (valueProperties == null || !valueProperties.Any()) throw new ArgumentNullException(nameof(valueProperties));
            Name = name;
            KeyColumns = keyProperties.Select(p => p.Name).ToArray();
            Columns = keyProperties.Concat(valueProperties).ToSqlColumns();
        }

        public string Name { get; }

        public string[] KeyColumns { get; }

        public IDictionary<string, SqlType> Columns { get; }
    }
}