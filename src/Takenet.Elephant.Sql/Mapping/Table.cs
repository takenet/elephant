using System;
using System.Collections.Generic;
using System.Linq;

namespace Takenet.Elephant.Sql.Mapping
{
    public class Table : ITable
    {
        public Table(string name, string[] keyColumnsNames, IDictionary<string, SqlType> columns, string schema = null)
        {
            if (keyColumnsNames == null) throw new ArgumentNullException(nameof(keyColumnsNames));
            var repeatedKeyColumn = keyColumnsNames.GroupBy(k => k).FirstOrDefault(c => c.Count() > 1);
            if (repeatedKeyColumn != null) throw new ArgumentException($"The key column named '{repeatedKeyColumn.Key}' appears more than once", nameof(columns));
            if (columns == null) throw new ArgumentNullException(nameof(columns));
            if (columns.Count == 0) throw new ArgumentException(@"The table must define at least one column", nameof(columns));
            var repeatedColumn = columns.GroupBy(c => c.Key).FirstOrDefault(c => c.Count() > 1);
            if (repeatedColumn != null) throw new ArgumentException($"The column named '{repeatedColumn.Key}' appears more than once", nameof(columns));
            Name = name ?? throw new ArgumentNullException(nameof(name));
            KeyColumnsNames = keyColumnsNames;
            Columns = columns;
            Schema = schema;
        }

        public string Schema { get; }

        public string Name { get; }

        public string[] KeyColumnsNames { get; }

        public IDictionary<string, SqlType> Columns { get; }

        public bool SchemaChecked { get; set; }
    }
}