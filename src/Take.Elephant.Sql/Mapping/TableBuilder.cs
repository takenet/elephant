using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;

namespace Take.Elephant.Sql.Mapping
{
    /// <summary>
    /// Utility class to allow build tables using fluent notation.
    /// </summary>
    public sealed class TableBuilder
    {
        /// <summary>
        /// Gets the table schema.
        /// </summary>
        /// <value>
        /// The schema.
        /// </value>
        public string Schema { get; }

        /// <summary>
        /// Gets the table name.
        /// </summary>
        /// <value>
        /// The name.
        /// </value>
        public string Name { get; }

        /// <summary>
        /// Gets the table columns.
        /// </summary>
        /// <value>
        /// The columns.
        /// </value>
        public IDictionary<string, SqlType> Columns { get; }

        /// <summary>
        /// Gets the table key columns names.
        /// </summary>
        /// <value>
        /// The key columns.
        /// </value>
        public HashSet<string> KeyColumns { get; }

        /// <summary>
        /// Indicates if the table schema is synchronized.
        /// </summary>
        public SchemaSynchronizationStrategy SynchronizationStrategy { get; private set; }

        private TableBuilder(string schema, string name)
        {
            Schema = schema;
            Name = name ?? throw new ArgumentNullException(nameof(name));
            Columns = new Dictionary<string, SqlType>();
            KeyColumns = new HashSet<string>();
            SynchronizationStrategy = SchemaSynchronizationStrategy.Ignore;
        }

        /// <summary>
        /// Creates a table builder using the specified table name.
        /// </summary>
        /// <param name="schema">The schema name to be used in the database. If null, the default drive schema will be used.</param>
        /// <param name="tableName">The table name.</param>
        /// <returns></returns>
        public static TableBuilder WithName(string tableName, string schema = null)
        {
            return new TableBuilder(schema, tableName);
        }

        /// <summary>
        /// Adds a column to the table with the specified name and type.
        /// </summary>
        /// <param name="columnName"></param>
        /// <param name="sqlType"></param>
        /// <returns></returns>
        public TableBuilder WithColumn(string columnName, SqlType sqlType)
        {
            Columns[columnName] = sqlType;
            return this;
        }

        /// <summary>
        /// Adds columns to the table with the specified names and types.
        /// </summary>
        /// <param name="columns"></param>
        /// <returns></returns>
        public TableBuilder WithColumns(params KeyValuePair<string, SqlType>[] columns)
        {
            foreach (var column in columns)
            {
                Columns[column.Key] = column.Value;
            }
            
            return this;
        }

        /// <summary>
        /// Adds a key column to the table from the specified type.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public TableBuilder WithColumnFromType<T>(string columnName)
        {
            if (columnName == null) throw new ArgumentNullException(nameof(columnName));
            Columns[columnName] = new SqlType(DbTypeMapper.GetDbType(typeof(T)));
            return this;
        }

        /// <summary>
        /// Adds a key column to the table from the specified type.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="columnName"></param>
        /// <param name="length"></param>
        /// <returns></returns>
        public TableBuilder WithColumnFromType<T>(string columnName, int length)
        {
            if (columnName == null) throw new ArgumentNullException(nameof(columnName));
            Columns[columnName] = new SqlType(DbTypeMapper.GetDbType(typeof(T)), length);
            return this;
        }

        /// <summary>
        /// Adds a key column to the table from the specified type.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="columnName"></param>
        /// <param name="precision"></param>
        /// <param name="scale"></param>
        /// <returns></returns>
        public TableBuilder WithColumnFromType<T>(string columnName, int precision, int scale)
        {
            if (columnName == null) throw new ArgumentNullException(nameof(columnName));
            Columns[columnName] = new SqlType(DbTypeMapper.GetDbType(typeof(T)), precision, scale);
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
            foreach (var column in typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(filter).ToSqlColumns())
            {
                Columns[column.Key] = column.Value;
            }

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
            foreach (var column in typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(filter).ToSqlColumns())
            {
                Columns[column.Key] = column.Value;
                KeyColumns.Add(column.Key);
            }
            return this;
        }

        /// <summary>
        /// Adds a key column to the table from the specified type.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="keyColumnName"></param>
        /// <param name="isIdentity"></param>
        /// <returns></returns>
        public TableBuilder WithKeyColumnFromType<T>(string keyColumnName, bool isIdentity = false)
        {
            if (keyColumnName == null) throw new ArgumentNullException(nameof(keyColumnName));
            Columns[keyColumnName] = new SqlType(DbTypeMapper.GetDbType(typeof(T)), isIdentity);
            KeyColumns.Add(keyColumnName);
            return this;
        }

        /// <summary>
        /// Adds key columns names to the table. The specified columns must exists on the table.
        /// </summary>
        /// <param name="keyColumnsNames"></param>
        /// <returns></returns>
        public TableBuilder WithKeyColumnsNames(params string[] keyColumnsNames)
        {
            foreach (var keyColumnName in keyColumnsNames)
            {
                KeyColumns.Add(keyColumnName);
            }
            
            return this;
        }

        /// <summary>
        /// The synchronization strategy to be used with the table.
        /// </summary>
        /// <param name="synchronizationStrategy"></param>
        /// <returns></returns>
        public TableBuilder WithSynchronizationStrategy(SchemaSynchronizationStrategy synchronizationStrategy)
        {
            SynchronizationStrategy = synchronizationStrategy;
            return this;
        }

        /// <summary>
        /// Indicates if the table schema is synchronized and should not be checked.
        /// </summary>
        /// <param name="schemaSynchronized"></param>
        /// <returns></returns>
        [Obsolete("Use WithSynchronizationStrategy")]
        public TableBuilder WithSchemaSynchronized(bool schemaSynchronized)
        {
            SynchronizationStrategy = schemaSynchronized
                ? SchemaSynchronizationStrategy.Ignore
                : SchemaSynchronizationStrategy.UntilSuccess;
            return this;
        }

        /// <summary>
        /// Builds a table with the builder data.
        /// </summary>
        /// <returns></returns>
        public ITable Build()
        {
            return new Table(Name, KeyColumns.ToArray(), Columns, Schema, SynchronizationStrategy);
        }
    }
}