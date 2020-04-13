using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Sql
{
    /// <summary>
    /// Implements the <see cref="IMap{TKey,TValue}"/> interface with a SQL database. 
    /// This class should be used only for local data, since the dictionary stores the values in the local process memory.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public class SqlMap<TKey, TValue> : MapStorageBase<TKey, TValue>, IKeysMap<TKey, TValue>, IPropertyMap<TKey, TValue>, IUpdatableMap<TKey, TValue>
    {
        public SqlMap(string connectionString, ITable table, IMapper<TKey> keyMapper, IMapper<TValue> valueMapper)
            : this(new SqlDatabaseDriver(), connectionString, table, keyMapper, valueMapper)
        {

        }

        public SqlMap(IDatabaseDriver databaseDriver, string connectionString, ITable table, IMapper<TKey> keyMapper, IMapper<TValue> valueMapper)
            : base(databaseDriver, connectionString, table, keyMapper, valueMapper)
        {

        }

        public virtual async Task<bool> TryAddAsync(TKey key,
            TValue value,
            bool overwrite = false,
            CancellationToken cancellationToken = default)
        {
            using (var cancellationTokenSource = CreateCancellationTokenSource())
            {
                using (var connection = await GetConnectionAsync(cancellationTokenSource.Token).ConfigureAwait(false))
                {
                    var columnValues = GetColumnValues(key, value, true);
                    var keyColumnValues = GetKeyColumnValues(columnValues);
                    using (var command = overwrite ? 
                        connection.CreateMergeCommand(DatabaseDriver, Table, keyColumnValues, columnValues) : 
                        connection.CreateInsertWhereNotExistsCommand(DatabaseDriver, Table, keyColumnValues, columnValues))
                    {
                        return await command.ExecuteNonQueryAsync(cancellationTokenSource.Token).ConfigureAwait(false) > 0;
                    }
                }
            }
        }

        public virtual async Task<TValue> GetValueOrDefaultAsync(TKey key,
            CancellationToken cancellationToken = default)
        {
            using (var cancellationTokenSource = CreateCancellationTokenSource())
            {
                using (var connection = await GetConnectionAsync(cancellationTokenSource.Token).ConfigureAwait(false))
                {
                    var keyColumnValues = KeyMapper.GetColumnValues(key);
                    var selectColumns = Table.Columns.Keys.ToArray();

                    return await new DbDataReaderAsyncEnumerable<TValue>(
                        // ReSharper disable once AccessToDisposedClosure
                        t => connection.AsCompletedTask(),
                        c => c.CreateSelectCommand(DatabaseDriver, Table, keyColumnValues, selectColumns),
                        Mapper,
                        selectColumns)
                        .FirstOrDefaultAsync(cancellationTokenSource.Token)
                        .ConfigureAwait(false);
                }
            }
        }

        public virtual async Task<bool> TryRemoveAsync(TKey key, CancellationToken cancellationToken = default)
        {
            using (var cancellationTokenSource = CreateCancellationTokenSource())
            {
                using (var connection = await GetConnectionAsync(cancellationTokenSource.Token).ConfigureAwait(false))
                {
                    return await TryRemoveAsync(key, connection, cancellationTokenSource.Token);
                }
            }
        }

        public virtual async Task<bool> ContainsKeyAsync(TKey key, CancellationToken cancellationToken = default)
        {
            using (var cancellationTokenSource = CreateCancellationTokenSource())
            {
                using (var connection = await GetConnectionAsync(cancellationTokenSource.Token).ConfigureAwait(false))
                {
                    return await ContainsKeyAsync(key, connection, cancellationTokenSource.Token);
                }
            }
        }

        public virtual Task<IAsyncEnumerable<TKey>> GetKeysAsync()
        {
            var selectColumns = Table.KeyColumnsNames;
            return Task.FromResult<IAsyncEnumerable<TKey>>(
                new DbDataReaderAsyncEnumerable<TKey>(GetConnectionAsync, c => c.CreateSelectCommand(DatabaseDriver, Table, null, selectColumns), KeyMapper, selectColumns));
        }

        public virtual async Task SetPropertyValueAsync<TProperty>(TKey key, string propertyName,
            TProperty propertyValue, CancellationToken cancellationToken = default)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (!Table.Columns.ContainsKey(propertyName)) throw new ArgumentException(@"Invalid property", nameof(propertyName));           
            if (Table.KeyColumnsNames.Contains(propertyName)) throw new ArgumentException(@"A key property cannot be changed", nameof(propertyName));

            using (var cancellationTokenSource = CreateCancellationTokenSource())
            {
                using (var connection = await GetConnectionAsync(cancellationTokenSource.Token).ConfigureAwait(false))
                {
                    var keyColumnValues = KeyMapper.GetColumnValues(key);
                    var columnValues = new Dictionary<string, object> { { propertyName, Mapper.DbTypeMapper.ToDbType(propertyValue, Table.Columns[propertyName].Type) } };

                    using (var command = connection.CreateMergeCommand(DatabaseDriver, Table, keyColumnValues, columnValues))
                    {
                        if (await command.ExecuteNonQueryAsync(cancellationTokenSource.Token).ConfigureAwait(false) == 0)
                        {
                            throw new Exception("The database operation failed");
                        }
                    }
                }
            }
        }

        public virtual async Task MergeAsync(TKey key, TValue value, CancellationToken cancellationToken = default)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (value == null) throw new ArgumentNullException(nameof(value));

            using (var cancellationTokenSource = CreateCancellationTokenSource())
            {
                using (var connection = await GetConnectionAsync(cancellationTokenSource.Token).ConfigureAwait(false))
                {
                    var keyColumnValues = KeyMapper.GetColumnValues(key);
                    var columnValues = GetColumnValues(value);

                    if (columnValues.Any())
                    {
                        using (var command = connection.CreateMergeCommand(DatabaseDriver, Table, keyColumnValues, columnValues))
                        {
                            if (await command.ExecuteNonQueryAsync(cancellationTokenSource.Token).ConfigureAwait(false) == 0)
                            {
                                throw new Exception("The database operation failed");
                            }
                        }
                    }
                }
            }
        }

        public virtual async Task<TProperty> GetPropertyValueOrDefaultAsync<TProperty>(TKey key, string propertyName,
            CancellationToken cancellationToken = default)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (!Table.Columns.ContainsKey(propertyName)) throw new ArgumentException(@"Invalid property", nameof(propertyName));

            using (var cancellationTokenSource = CreateCancellationTokenSource())
            {
                using (var connection = await GetConnectionAsync(cancellationTokenSource.Token).ConfigureAwait(false))
                {
                    var keyColumnValues = KeyMapper.GetColumnValues(key);

                    using (var command = connection.CreateSelectTop1Command(DatabaseDriver, Table, new[] { propertyName }, keyColumnValues))
                    {
                        var dbValue = await command.ExecuteScalarAsync(cancellationTokenSource.Token).ConfigureAwait(false);
                        if (dbValue != null && !(dbValue is DBNull))
                        {
                            return (TProperty)Mapper.DbTypeMapper.FromDbType(
                                dbValue,
                                Table.Columns[propertyName].Type,
                                typeof(TValue).GetTypeInfo().GetProperty(propertyName).PropertyType);
                        }
                    }
                }
            }

            return default(TProperty);
        }

        public virtual async Task<bool> TryUpdateAsync(TKey key, TValue newValue, TValue oldValue)
        {
            using (var cancellationTokenSource = CreateCancellationTokenSource())
            {
                using (var connection = await GetConnectionAsync(cancellationTokenSource.Token).ConfigureAwait(false))
                {
                    var oldColumnValues = GetColumnValues(key, oldValue);
                    var filterOldColumnValues = oldColumnValues
                        .Select(kv => new KeyValuePair<string, object>($"Old{kv.Key}", kv.Value))
                        .ToDictionary(t => t.Key, t => t.Value);

                    var newColumnValues = GetColumnValues(key, newValue, true);

                    using (var command = connection.CreateTextCommand(
                        DatabaseDriver.GetSqlStatementTemplate(SqlStatement.Update),                        
                        new
                        {
                            schemaName = DatabaseDriver.ParseIdentifier(Table.Schema ?? DatabaseDriver.DefaultSchema),
                            tableName = DatabaseDriver.ParseIdentifier(Table.Name),
                            columnValues = SqlHelper.GetCommaEqualsStatement(DatabaseDriver, newColumnValues.Keys.ToArray()),
                            filter = SqlHelper.GetAndEqualsStatement(DatabaseDriver, oldColumnValues.Keys.ToArray(), filterOldColumnValues.Keys.ToArray())
                        },
                        newColumnValues.Concat(filterOldColumnValues).Select(c => c.ToDbParameter(DatabaseDriver))))
                    {
                        return await command.ExecuteNonQueryAsync(cancellationTokenSource.Token).ConfigureAwait(false) == 1;
                    }
                }
            }
        }
    }
}
