using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;

namespace Takenet.SimplePersistence.Sql.Mapping
{
    public class TypeMapper<TEntity> : IMapper<TEntity> where TEntity : class, new()
    {
        private readonly ITable _table;
        private readonly IDictionary<string, Type> _propertyDictionary;
        private readonly IDictionary<string, Func<TEntity, object>> _propertyGetFuncDictionary;
        private readonly IDictionary<string, Action<TEntity, object>> _propertySetActionDictionary;

        public TypeMapper(ITable table)
            : this(table, typeof(TEntity).GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            
        }

        protected TypeMapper(ITable table, PropertyInfo[] properties)
        {
            if (table == null) throw new ArgumentNullException(nameof(table));
            if (properties == null || properties.Length == 0) throw new ArgumentNullException(nameof(properties));
            var missingColumnProperty = properties.Select(p => p.Name).FirstOrDefault(p => !table.Columns.ContainsKey(p));
            if (missingColumnProperty != null)
            {
                throw new ArgumentException($"The table doesn't contains a column for the property '{missingColumnProperty}'");
            }

            _table = table;            
            _propertyDictionary = properties.ToDictionary(p => p.Name, p => p.PropertyType);
            _propertyGetFuncDictionary = new Dictionary<string, Func<TEntity, object>>();
            _propertySetActionDictionary = new Dictionary<string, Action<TEntity, object>>();

            foreach (var property in properties)
            {
                if (!_table.Columns[property.Name].IsIdentity)
                {
                    _propertyGetFuncDictionary.Add(
                        property.Name,
                        TypeUtil.BuildGetAccessor(property));
                }

                _propertySetActionDictionary.Add(
                    property.Name,
                    TypeUtil.BuildSetAccessor(property));
            }
        }

        public IDictionary<string, object> GetColumnValues(TEntity value, string[] columns = null, bool returnNullValues = false)
        {
            return _propertyGetFuncDictionary
                .Where(
                    p => columns == null ||
                         columns.Contains(p.Key))
                .ToDictionary(
                    p => p.Key,
                    p => p.Value(value))
                .Where(
                    p => returnNullValues ||
                         p.Value != null)
                .ToDictionary(
                    p => p.Key,
                    p => TypeMapper.ToDbType(p.Value, _table.Columns[p.Key].Type));
        }

        public TEntity Create(IDataRecord record, string[] columns)
        {
            var entity = new TEntity();

            for (var i = 0; i < columns.Length; i++)
            {
                if (record.IsDBNull(i)) continue;
                var column = columns[i];

                if (_propertyDictionary.ContainsKey(column))
                {
                    _propertySetActionDictionary[column](
                        entity,
                        TypeMapper.FromDbType(
                            record[i],
                            _table.Columns[column].Type,
                            _propertyDictionary[column]
                            ));
                }
            }

            return entity;
        }
    }
}