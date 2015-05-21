using System;
using System.Collections.Generic;
using System.Data;

namespace Takenet.Elephant.Sql.Mapping
{
    public class ValueMapper<T> : IMapper<T>
    {
        private readonly static DbType _dbType;                

        static ValueMapper()
        {
            _dbType = TypeMapper.GetDbType(typeof(T));
        }

        internal string ColumnName { get; }

        public ValueMapper(string columnName)
        {
            if (columnName == null) throw new ArgumentNullException(nameof(columnName));
            ColumnName = columnName;
        }

        public IDictionary<string, object> GetColumnValues(T value, string[] columns = null, bool emitDefaultValues = false)
        {
            return new Dictionary<string, object>()
            {
                { ColumnName, TypeMapper.ToDbType(value, _dbType) }
            };
        }

        public T Create(IDataRecord record, string[] columns)
        {
            var index = -1;

            for (var i = 0; i < columns.Length; i++)
            {
                if (columns[i].Equals(ColumnName, StringComparison.OrdinalIgnoreCase))
                {
                    index = i;
                    break;
                }
            }

            if (index < 0) throw new ArgumentException($"The column '{ColumnName}' was not found", nameof(columns));            
            return (T)TypeMapper.FromDbType(record[index], _dbType, typeof (T));
        }
    }
}