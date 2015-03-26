using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence.Sql.Mapping
{
    public class Table : ITable
    {
        public Table(string name, string[] keyColumns, IDictionary<string, SqlType> columns)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));
            if (keyColumns == null) throw new ArgumentNullException(nameof(keyColumns));
            if (columns == null) throw new ArgumentNullException(nameof(columns));
            if (columns.Count == 0) throw new ArgumentException(@"The table must define at least one column", nameof(columns));
            Name = name;
            KeyColumns = keyColumns;
            Columns = columns;
        }

        public string Name { get; }
        public string[] KeyColumns { get; }
        public IDictionary<string, SqlType> Columns { get; }
    }
}
