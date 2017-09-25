using System;
using System.Collections.Generic;
using System.Data;

namespace Takenet.Elephant.Sql.Mapping
{
    public class ValueMapper<T> : IMapper<T>
    {
        private static readonly DbType _dbType = Sql.DbTypeMapper.GetDbType(typeof(T));
                
        public ValueMapper(string columnName)
            : this(columnName, Sql.DbTypeMapper.Default)
        {
            
        }

        public ValueMapper(string columnName, IDbTypeMapper dbTypeMapper)
        {            
            if (columnName == null) throw new ArgumentNullException(nameof(columnName));
            ColumnName = columnName;
            DbTypeMapper = dbTypeMapper;
        }

        internal string ColumnName { get; }

        public IDbTypeMapper DbTypeMapper { get; }

        public virtual IDictionary<string, object> GetColumnValues(T value, string[] columns = null, bool emitNullValues = false, bool includeIdentityTypes = false)
        {
            return new Dictionary<string, object>()
            {
                { ColumnName, DbTypeMapper.ToDbType(value, _dbType) }
            };
        }

        public virtual T Create(IDataRecord record, string[] columns, T value = default(T))
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
            return (T)DbTypeMapper.FromDbType(record[index], _dbType, typeof (T));
        }
    }
}