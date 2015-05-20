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

        private readonly string _columnName;

        public ValueMapper(string columnName)
        {
            if (columnName == null) throw new ArgumentNullException(nameof(columnName));
            _columnName = columnName;
        }

        public IDictionary<string, object> GetColumnValues(T value, string[] columns = null, bool returnDefaultValues = false)
        {
            return new Dictionary<string, object>()
            {
                { _columnName, TypeMapper.ToDbType(value, _dbType) }
            };
        }

        public T Create(IDataRecord record, string[] columns)
        {
            var index = -1;

            for (var i = 0; i < columns.Length; i++)
            {
                if (columns[i].Equals(_columnName, StringComparison.OrdinalIgnoreCase))
                {
                    index = i;
                    break;
                }
            }

            if (index < 0) throw new ArgumentException($"The column '{_columnName}' was not found", nameof(columns));            
            return (T)TypeMapper.FromDbType(record[index], _dbType, typeof (T));
        }
    }
}