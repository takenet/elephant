using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Takenet.Elephant.Sql.Mapping;

namespace Takenet.Elephant.Sql
{
    /// <summary>
    /// Implements the <see cref="IMap{TKey,TValue}"/> interface with a SQL database. 
    /// This class should be used only for local data, since the dictionary stores the values in the local process memory.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public class SqlMap<TKey, TValue> : MapStorageBase<TKey, TValue>, IKeysMap<TKey, TValue>, IPropertyMap<TKey, TValue>, IUpdatableMap<TKey, TValue>
    {
        #region Constructors

        public SqlMap(string connectionString, ITable table, IMapper<TKey> keyMapper, IMapper<TValue> valueMapper)
            : this(new SqlDatabaseDriver(), connectionString, table, keyMapper, valueMapper)
        {

        }

        public SqlMap(IDatabaseDriver databaseDriver, string connectionString, ITable table, IMapper<TKey> keyMapper, IMapper<TValue> valueMapper)
            : base(databaseDriver, connectionString, table, keyMapper, valueMapper)
        {

        }

        #endregion

        #region IMap<TKey,TValue> Members

        public async Task<bool> TryAddAsync(TKey key, TValue value, bool overwrite = false)
        {
            var cancellationToken = CreateCancellationToken();

            using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
            {
                var columnValues = GetColumnValues(key, value);
                var keyColumnValues = GetKeyColumnValues(columnValues);
                using (var command = connection.CreateInsertWhereNotExistsCommand(Table.Name, keyColumnValues, columnValues, overwrite))
                {
                    return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
                }
            }
        }

        public async Task<TValue> GetValueOrDefaultAsync(TKey key)
        {
            var cancellationToken = CreateCancellationToken();

            using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
            {
                var keyColumnValues = KeyMapper.GetColumnValues(key);
                var selectColumns = Table.Columns.Keys.ToArray();
                
                return await new DbDataReaderAsyncEnumerable<TValue>(
                    // ReSharper disable once AccessToDisposedClosure
                    t => connection.AsCompletedTask(), 
                    c => c.CreateSelectCommand(Table.Name, keyColumnValues, selectColumns), 
                    Mapper, 
                    selectColumns)
                    .FirstOrDefaultAsync(cancellationToken)
                    .ConfigureAwait(false);
            }
        }

        public async Task<bool> TryRemoveAsync(TKey key)
        {
            var cancellationToken = CreateCancellationToken();
            using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
            {
                return await TryRemoveAsync(key, connection, cancellationToken);
            }
        }

        public async Task<bool> ContainsKeyAsync(TKey key)
        {
            var cancellationToken = CreateCancellationToken();
            using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
            {
                return await ContainsKeyAsync(key, connection, cancellationToken);
            }
        }

        #endregion

        #region IKeysMap<TKey, TValue> Members

        public Task<IAsyncEnumerable<TKey>> GetKeysAsync()
        {
            var selectColumns = Table.KeyColumnsNames;
            return Task.FromResult<IAsyncEnumerable<TKey>>(
                new DbDataReaderAsyncEnumerable<TKey>(GetConnectionAsync, c => c.CreateSelectCommand(Table.Name, null, selectColumns), KeyMapper, selectColumns));
        }

        #endregion

        #region IPropertyMap<TKey,TValue> Members

        public async Task SetPropertyValueAsync<TProperty>(TKey key, string propertyName, TProperty propertyValue)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (!Table.Columns.ContainsKey(propertyName)) throw new ArgumentException(@"Invalid property", nameof(propertyName));           
            if (Table.KeyColumnsNames.Contains(propertyName)) throw new ArgumentException(@"A key property cannot be changed", nameof(propertyName));

            var cancellationToken = CreateCancellationToken();

            using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
            {                                
                var keyColumnValues = KeyMapper.GetColumnValues(key);
                var columnValues = new Dictionary<string, object> {{propertyName, TypeMapper.ToDbType(propertyValue, Table.Columns[propertyName].Type) } };

                using (var command = connection.CreateMergeCommand(Table.Name, keyColumnValues, columnValues))
                {
                    if (await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 0)
                    {
                        throw new Exception("The database operation failed");
                    }
                }
            }
        }

        public async Task MergeAsync(TKey key, TValue value)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (value == null) throw new ArgumentNullException(nameof(value));

            var cancellationToken = CreateCancellationToken();

            using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
            {
                var keyColumnValues = KeyMapper.GetColumnValues(key);
                var columnValues = GetColumnValues(value);

                if (columnValues.Any())
                {
                    using (var command = connection.CreateMergeCommand(Table.Name, keyColumnValues, columnValues))
                    {
                        if (await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 0)
                        {
                            throw new Exception("The database operation failed");
                        }
                    }
                }
            }
        }

        public async Task<TProperty> GetPropertyValueOrDefaultAsync<TProperty>(TKey key, string propertyName)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (!Table.Columns.ContainsKey(propertyName)) throw new ArgumentException(@"Invalid property", nameof(propertyName));

            var cancellationToken = CreateCancellationToken();
            
            using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
            {
                var keyColumnValues = KeyMapper.GetColumnValues(key);

                using (var command = connection.CreateSelectTop1Command(Table.Name, new[] { propertyName }, keyColumnValues))
                {
                    var dbValue = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                    if (dbValue != null && !(dbValue is DBNull))
                    {
                        return (TProperty)TypeMapper.FromDbType(
                            dbValue,
                            Table.Columns[propertyName].Type,
                            typeof(TValue).GetProperty(propertyName).PropertyType);
                    }
                }
            }

            return default(TProperty);
        }

        #endregion

        #region IUpdatableMap<TKey, TValue> Members

        public async Task<bool> TryUpdateAsync(TKey key, TValue newValue, TValue oldValue)
        {
            var cancellationToken = CreateCancellationToken();

            using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
            {               
                var oldColumnValues = GetColumnValues(key, oldValue);
                var filterOldColumnValues = oldColumnValues
                    .Select(kv => new KeyValuePair<string, object>($"Old{kv.Key}", kv.Value))
                    .ToDictionary(t => t.Key, t => t.Value);

                var newColumnValues = GetColumnValues(key, newValue);

                using (var command = connection.CreateTextCommand(
                    SqlTemplates.Update,
                    new
                    {
                        tableName = Table.Name.AsSqlIdentifier(),
                        columnValues = SqlHelper.GetCommaEqualsStatement(newColumnValues.Keys.ToArray()),
                        filter = SqlHelper.GetAndEqualsStatement(oldColumnValues.Keys.ToArray(), filterOldColumnValues.Keys.ToArray())
                    },
                    newColumnValues.Concat(filterOldColumnValues).Select(c => c.ToSqlParameter())))
                {
                    return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 1;
                }
            }
        }

        #endregion
    }
}
