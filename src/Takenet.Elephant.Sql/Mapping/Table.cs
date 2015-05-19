using System;
using System.Collections.Generic;
using System.Linq;

namespace Takenet.Elephant.Sql.Mapping
{
    public class Table : ITable
    {
        public Table(string name, string[] keyColumns, IDictionary<string, SqlType> columns)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));
            if (keyColumns == null) throw new ArgumentNullException(nameof(keyColumns));
            var repeatedKeyColumn = keyColumns.GroupBy(k => k).FirstOrDefault(c => c.Count() > 1);
            if (repeatedKeyColumn != null) throw new ArgumentException($"The key column named '{repeatedKeyColumn.Key}' appears more than once", nameof(columns));
            if (columns == null) throw new ArgumentNullException(nameof(columns));
            if (columns.Count == 0) throw new ArgumentException(@"The table must define at least one column", nameof(columns));
            var repeatedColumn = columns.GroupBy(c => c.Key).FirstOrDefault(c => c.Count() > 1);
            if (repeatedColumn != null) throw new ArgumentException($"The column named '{repeatedColumn.Key}' appears more than once", nameof(columns));

            Name = name;
            KeyColumns = keyColumns;
            Columns = columns;
        }

        public string Name { get; }
        public string[] KeyColumns { get; }
        public IDictionary<string, SqlType> Columns { get; }
    }
}
