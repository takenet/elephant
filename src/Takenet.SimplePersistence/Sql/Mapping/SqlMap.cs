using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence.Sql.Mapping
{
    /// <summary>
    /// Implements the <see cref="IMap{TKey,TValue}"/> interface with a SQL database. 
    /// This class should be used only for local data, since the dictionary stores the values in the local process memory.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public class SqlMap<TKey, TValue> : MapBase<TKey, TValue>, IMap<TKey, TValue>, IPropertyMap<TKey, TValue>, IKeysMap<TKey, TValue>, IUpdatableMap<TKey, TValue>
    {
        public SqlMap(IExtendedTableMapper<TKey, TValue> tableMapper, string connectionString)
            : base(tableMapper, connectionString)
        {

        }  

        #region IMap<TKey,TValue> Members

        public async Task<bool> TryAddAsync(TKey key, TValue value, bool overwrite = false)
        {
            var cancellationToken = CreateCancellationToken();

            using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
            {
                return await TryAddAsync(key, value, overwrite, connection, cancellationToken).ConfigureAwait(false);
            }
        }

        public async Task<TValue> GetValueOrDefaultAsync(TKey key)
        {
            var cancellationToken = CreateCancellationToken();

            using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
            {
                var values = await GetValuesByExtensionKeyAsync(key, connection, cancellationToken).ConfigureAwait(false);
                return values.FirstOrDefault();
            }
        }

        public async Task<bool> TryRemoveAsync(TKey key)
        {
            var cancellationToken = CreateCancellationToken();

            using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
            {
                return await TryRemoveAsync(key, connection, cancellationToken).ConfigureAwait(false);
            }
        }

        public async Task<bool> ContainsKeyAsync(TKey key)
        {
            var cancellationToken = CreateCancellationToken();

            using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
            {
                return await ContainsKeyAsync(key, connection, cancellationToken).ConfigureAwait(false);
            }
        }

        #endregion

        #region IPropertyMap<TKey,TValue> Members

        public async Task SetPropertyValueAsync<TProperty>(TKey key, string propertyName, TProperty propertyValue)
        {
            var cancellationToken = CreateCancellationToken();

            if (_extendedTableMapper.KeyColumns.Contains(propertyName))
            {
                throw new ArgumentException("A key property cannot be changed");
            }

            if (!_extendedTableMapper.Columns.ContainsKey(propertyName))
            {
                throw new ArgumentException("Invalid property");
            }

            var dbPropertyValue = TypeMapper.ToDbType(propertyValue, _extendedTableMapper.Columns[propertyName].Type);

            using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
            {
                SqlCommand command;

                if (await ContainsKeyAsync(key).ConfigureAwait(false))
                {
                    command = connection.CreateTextCommand(
                        SqlTemplates.Update,
                        new
                        {
                            tableName = _extendedTableMapper.TableName,
                            columnValues = GetEqualsStatement(propertyName)
                        },
                        new[]
                        {
                            new SqlParameter(
                                propertyName.AsSqlParameterName(),
                                dbPropertyValue)
                        });
                }
                else
                {
                    var keyValues = _extendedTableMapper.GetExtensionColumnValues(key);
                    keyValues.Add(propertyName, dbPropertyValue);

                    command = connection.CreateTextCommand(
                        SqlTemplates.Insert,
                        new
                        {
                            tableName = _extendedTableMapper.TableName.AsSqlIdentifier(),
                            columns = keyValues.Keys.Select(c => c.AsSqlIdentifier()).ToCommaSepparate(),
                            values = keyValues.Keys.Select(v => v.AsSqlParameterName()).ToCommaSepparate(),
                            filter = GetAndEqualsStatement(keyValues.Keys.ToArray())
                        },
                        keyValues.Select(c => c.ToSqlParameter()));
                }

                using (command)
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
            var cancellationToken = CreateCancellationToken();

            if (await ContainsKeyAsync(key).ConfigureAwait(false))
            {
                var columnValues = _extendedTableMapper.GetColumnValues(value);

                if (columnValues.Any())
                {
                    using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
                    {
                        var keyValues = _extendedTableMapper.GetExtensionColumnValues(key);

                        using (var command = connection.CreateTextCommand(
                            SqlTemplates.Update,
                            new
                            {
                                tableName = _extendedTableMapper.TableName,
                                columnValues = columnValues.Keys.Select(c =>
                                    SqlTemplates.QueryEquals.Format(
                                    new
                                    {
                                        column = c.AsSqlIdentifier(),
                                        value = c.AsSqlParameterName()
                                    })).ToCommaSepparate(),
                                filter = GetAndEqualsStatement(keyValues.Keys.ToArray())
                            },
                            columnValues.Union(keyValues).Select(c => c.ToSqlParameter())))
                        {

                            if (await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 0)
                            {
                                throw new Exception("The database operation failed");
                            }
                        }
                    }
                }
            }
            else if (!await TryAddAsync(key, value, true).ConfigureAwait(false))
            {
                throw new Exception("The database operation failed");
            }
        }

        public async Task<TProperty> GetPropertyValueOrDefaultAsync<TProperty>(TKey key, string propertyName)
        {
            var cancellationToken = CreateCancellationToken();

            if (!_extendedTableMapper.Columns.ContainsKey(propertyName))
            {
                throw new ArgumentException("Invalid property");
            }

            using (var connection = await GetConnectionAsync(cancellationToken))
            {
                var keyValues = _extendedTableMapper.GetExtensionColumnValues(key);

                using (var command = connection.CreateTextCommand(
                    SqlTemplates.SelectTop1,
                    new
                    {
                        tableName = _extendedTableMapper.TableName.AsSqlIdentifier(),
                        columns = propertyName.AsSqlIdentifier(),
                        filter = GetAndEqualsStatement(keyValues.Keys.ToArray())
                    },
                    keyValues.Select(k => k.ToSqlParameter())))
                {

                    var dbValue = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);

                    if (dbValue != null &&
                        !(dbValue is DBNull))
                    {
                        return (TProperty)TypeMapper.FromDbType(
                            dbValue,
                            _extendedTableMapper.Columns[propertyName].Type,
                            typeof(TValue).GetProperty(propertyName).PropertyType);
                    }
                }
            }

            return default(TProperty);
        }

        #endregion

        #region IKeysMap<TKey,TValue> Members

        public async Task<IEnumerable<TKey>> GetKeysAsync()
        {
            var cancellationToken = CreateCancellationToken();

            using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
            {
                return await base.GetKeysAsync(connection, cancellationToken).ConfigureAwait(false);
            }
        }

        #endregion

        public async Task<bool> TryUpdateAsync(TKey key, TValue newValue, TValue oldValue)
        {
            var cancellationToken = CreateCancellationToken();

            using (var connection = await GetConnectionAsync(cancellationToken))
            {
                // Key value update is not supported
                var keyValues = _extendedTableMapper.GetKeyColumnValues(key, oldValue);

                var newColumnValues = _extendedTableMapper.GetColumnValues(key, newValue);
                var oldColumnValues = _extendedTableMapper.GetColumnValues(key, oldValue);

                var filterColumnNames = keyValues
                    .Keys
                    .Concat(oldColumnValues.Keys)
                    .ToArray();

                var filterColumnValues = keyValues
                    .Concat(
                        oldColumnValues
                            .Select(kv => new KeyValuePair<string, object>("Old" + kv.Key, kv.Value)))
                    .ToDictionary(t => t.Key, t => t.Value);

                var sqlTemplate = SqlTemplates.Update;

                using (var command = connection.CreateTextCommand(
                    sqlTemplate,
                    new
                    {
                        tableName = _extendedTableMapper.TableName.AsSqlIdentifier(),
                        columnValues = GetCommaEqualsStatement(newColumnValues.Keys.ToArray()),
                        filter = GetAndEqualsStatement(filterColumnNames, filterColumnValues.Keys.ToArray())
                    },
                    newColumnValues.Concat(filterColumnValues).Select(c => c.ToSqlParameter())))
                {
                    return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
                }
            }
        }
    }
}
