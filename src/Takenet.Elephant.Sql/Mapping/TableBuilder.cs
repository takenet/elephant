using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;

namespace Takenet.Elephant.Sql.Mapping
{
    /// <summary>
    /// Utility class to allow build tables using fluent notation.
    /// </summary>
    public sealed class TableBuilder
    {
        private readonly string _name;
        private readonly List<KeyValuePair<string, SqlType>> _columns;
        private readonly List<string> _keyColumns;

        private TableBuilder(string name)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));
            _name = name;
            _columns = new List<KeyValuePair<string, SqlType>>();
            _keyColumns = new List<string>();
        }

        /// <summary>
        /// Creates a table builder using the specified table name.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public static TableBuilder WithName(string name)
        {            
            return new TableBuilder(name);
        }

        /// <summary>
        /// Adds a column to the table with the specified name and type.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="sqlType"></param>
        /// <returns></returns>
        public TableBuilder WithColumn(string name, SqlType sqlType)
        {
            _columns.Add(new KeyValuePair<string, SqlType>(name, sqlType));
            return this;
        }

        /// <summary>
        /// Adds columns to the table with the specified names and types.
        /// </summary>
        /// <param name="columns"></param>
        /// <returns></returns>
        public TableBuilder WithColumns(params KeyValuePair<string, SqlType>[] columns)
        {
            _columns.AddRange(columns);
            return this;
        }

        /// <summary>
        /// Adds columns to the table from the specified type properties.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public TableBuilder WithColumnsFromTypeProperties<T>()
        {
            return WithColumnsFromTypeProperties<T>(p => true);
        }

        /// <summary>
        /// Adds columns to the table from the specified type <see cref="DataMemberAttribute"/> decorated properties.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public TableBuilder WithColumnsFromTypeDataMemberProperties<T>()
        {
            return WithColumnsFromTypeProperties<T>(p => p.GetCustomAttribute<DataMemberAttribute>() != null);
        }

        /// <summary>
        /// Adds columns to the table from the specified type properties.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="filter"></param>
        /// <returns></returns>
        public TableBuilder WithColumnsFromTypeProperties<T>(Func<PropertyInfo, bool> filter)
        {
            _columns.AddRange(
                typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(filter).ToSqlColumns());
            return this;
        }

        /// <summary>
        /// Adds key columns to the table from the specified type properties.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public TableBuilder WithKeyColumnsFromTypeProperties<T>()
        {
            return WithKeyColumnsFromTypeProperties<T>(p => true);
        }

        /// <summary>
        /// Adds key columns to the table from the specified type <see cref="DataMemberAttribute"/> decorated properties.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public TableBuilder WithKeyColumnsFromTypeDataMemberProperties<T>()
        {
            return WithKeyColumnsFromTypeProperties<T>(p => p.GetCustomAttribute<DataMemberAttribute>() != null);
        }

        /// <summary>
        /// Adds key columns to the table from the specified type properties.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="filter"></param>
        /// <returns></returns>
        public TableBuilder WithKeyColumnsFromTypeProperties<T>(Func<PropertyInfo, bool> filter)
        {
            var columns = typeof (T).GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(filter).ToSqlColumns();
            _columns.AddRange(columns);
            _keyColumns.AddRange(columns.Keys);            
            return this;
        }

        /// <summary>
        /// Adds a key column to the table from the specified type.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="keyColumn"></param>
        /// <returns></returns>
        public TableBuilder WithKeyColumnFromType<T>(string keyColumn)
        {
            if (keyColumn == null) throw new ArgumentNullException(nameof(keyColumn));
            var column = new KeyValuePair<string, SqlType>(keyColumn, new SqlType(TypeMapper.GetDbType(typeof (T))));
            _columns.Add(column);
            _keyColumns.Add(keyColumn);
            return this;
        }

        /// <summary>
        /// Adds key columns names to the table. The specified columns must exists on the table.
        /// </summary>
        /// <param name="keyColumns"></param>
        /// <returns></returns>
        public TableBuilder WithKeyColumns(params string[] keyColumns)
        {
            _keyColumns.AddRange(keyColumns);
            return this;
        }

        /// <summary>
        /// Builds a table with the builder data.
        /// </summary>
        /// <returns></returns>
        public ITable Build()
        {
            return new Table(_name, _keyColumns.ToArray(), _columns.ToDictionary(c => c.Key, c => c.Value));            
        }
    }
}