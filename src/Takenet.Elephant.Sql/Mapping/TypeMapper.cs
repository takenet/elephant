using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;

namespace Takenet.Elephant.Sql.Mapping
{
    public class TypeMapper<TEntity> : IMapper<TEntity> where TEntity : class, new()
    {
        private readonly ITable _table;
        private readonly IDictionary<string, Type> _propertyDictionary;
        private readonly IDictionary<string, Func<TEntity, object>> _propertyGetFuncDictionary;
        private readonly IDictionary<string, Action<TEntity, object>> _propertySetActionDictionary;

        public TypeMapper(ITable table, IDbTypeMapper dbTypeMapper = null)
            : this(table, p => true, dbTypeMapper)
        {

        }

        public TypeMapper(ITable table, Func<PropertyInfo, bool> propertyFilter, IDbTypeMapper dbTypeMapper = null)
            : this(table, typeof(TEntity).GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(propertyFilter).ToArray(), dbTypeMapper ?? Sql.DbTypeMapper.Default)
        {

        }

        protected TypeMapper(ITable table, PropertyInfo[] properties, IDbTypeMapper dbTypeMapper)
        {
            if (properties == null || properties.Length == 0) throw new ArgumentNullException(nameof(properties));
            _table = table ?? throw new ArgumentNullException(nameof(table));
            DbTypeMapper = dbTypeMapper ?? throw new ArgumentNullException(nameof(dbTypeMapper));
            _propertyDictionary = properties.ToDictionary(p => p.Name, p => p.PropertyType);
            _propertyGetFuncDictionary = new Dictionary<string, Func<TEntity, object>>();
            _propertySetActionDictionary = new Dictionary<string, Action<TEntity, object>>();

            foreach (var property in properties)
            {
                if (!_table.Columns.ContainsKey(property.Name))
                {
                    throw new ArgumentException($"The table doesn't contains a column for property '{property.Name}'");
                }

                _propertyGetFuncDictionary.Add(
                    property.Name,
                    TypeUtil.BuildGetAccessor(property));

                _propertySetActionDictionary.Add(
                    property.Name,
                    TypeUtil.BuildSetAccessor(property));
            }
        }

        public IDbTypeMapper DbTypeMapper { get; }

        public virtual IDictionary<string, object> GetColumnValues(TEntity value, string[] columns = null, bool emitNullValues = false, bool includeIdentityTypes = false)
        {
            return _propertyGetFuncDictionary
                .Where(
                    p => columns == null ||
                         columns.Contains(p.Key))
                .ToDictionary(
                    p => p.Key,
                    p => p.Value(value))
                .Where(
                    p => emitNullValues || p.Value != null)
                .Where(
                    p => includeIdentityTypes || !_table.Columns[p.Key].IsIdentity)
                .ToDictionary(
                    p => p.Key,
                    p => DbTypeMapper.ToDbType(p.Value, _table.Columns[p.Key].Type, _table.Columns[p.Key].Length));
        }

        public virtual TEntity Create(IDataRecord record, string[] columns)
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
                        DbTypeMapper.FromDbType(
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