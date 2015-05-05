using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Takenet.SimplePersistence.Sql.Mapping;
using static Takenet.SimplePersistence.Sql.SqlHelper;

namespace Takenet.SimplePersistence.Sql
{
    /// <summary>
    /// Implements the <see cref="IMap{TKey,TValue}"/> interface with a SQL database. 
    /// This class should be used only for local data, since the dictionary stores the values in the local process memory.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public class SqlMap<TKey, TValue> : MapStorageBase<TKey, TValue>, IMap<TKey, TValue>, IKeysMap<TKey, TValue>, IKeyQueryableMap<TKey, TValue>
    {
        public SqlMap(IDatabaseDriver databaseDriver, string connectionString, ITable table, IMapper<TKey> keyMapper, IMapper<TValue> valueMapper) 
            : base(databaseDriver, connectionString, table, keyMapper, valueMapper)
        {

        }


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
                var command = connection.CreateSelectCommand(Table.Name, keyColumnValues, selectColumns);
                using (var values = new DbDataReaderAsyncEnumerable<TValue>(command, Mapper, selectColumns))
                {
                    return await values.FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
                }
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

        public async Task<IAsyncEnumerable<TKey>> GetKeysAsync()
        {
            var cancellationToken = CreateCancellationToken();
            var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
            return await GetKeysAsync(connection, cancellationToken).ConfigureAwait(false);
        }

        #endregion

        #region IPropertyMap<TKey,TValue> Members

        //public async Task SetPropertyValueAsync<TProperty>(TKey key, string propertyName, TProperty propertyValue)
        //{
        //    var cancellationToken = CreateCancellationToken();

        //    if (_extendedTableMapper.KeyColumns.Contains(propertyName))
        //    {
        //        throw new ArgumentException("A key property cannot be changed");
        //    }

        //    if (!_extendedTableMapper.Columns.ContainsKey(propertyName))
        //    {
        //        throw new ArgumentException("Invalid property");
        //    }

        //    var dbPropertyValue = TypeMapper.ToDbType(propertyValue, _extendedTableMapper.Columns[propertyName].Type);

        //    using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
        //    {
        //        SqlCommand command;

        //        if (await ContainsKeyAsync(key).ConfigureAwait(false))
        //        {
        //            command = connection.CreateTextCommand(
        //                SqlTemplates.Update,
        //                new
        //                {
        //                    tableName = _extendedTableMapper.TableName,
        //                    columnValues = GetEqualsStatement(propertyName)
        //                },
        //                new[]
        //                {
        //                    new SqlParameter(
        //                        propertyName.AsSqlParameterName(),
        //                        dbPropertyValue)
        //                });
        //        }
        //        else
        //        {
        //            var keyValues = _extendedTableMapper.GetExtensionColumnValues(key);
        //            keyValues.Add(propertyName, dbPropertyValue);

        //            command = connection.CreateTextCommand(
        //                SqlTemplates.Insert,
        //                new
        //                {
        //                    tableName = _extendedTableMapper.TableName.AsSqlIdentifier(),
        //                    columns = keyValues.Keys.Select(c => c.AsSqlIdentifier()).ToCommaSepparate(),
        //                    values = keyValues.Keys.Select(v => v.AsSqlParameterName()).ToCommaSepparate(),
        //                    filter = GetAndEqualsStatement(keyValues.Keys.ToArray())
        //                },
        //                keyValues.Select(c => c.ToSqlParameter()));
        //        }

        //        using (command)
        //        {
        //            if (await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 0)
        //            {
        //                throw new Exception("The database operation failed");
        //            }
        //        }
        //    }
        //}

        //public async Task MergeAsync(TKey key, TValue value)
        //{
        //    var cancellationToken = CreateCancellationToken();

        //    if (await ContainsKeyAsync(key).ConfigureAwait(false))
        //    {
        //        var columnValues = _extendedTableMapper.GetColumnValues(value);

        //        if (columnValues.Any())
        //        {
        //            using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
        //            {
        //                var keyValues = _extendedTableMapper.GetExtensionColumnValues(key);

        //                using (var command = connection.CreateTextCommand(
        //                    SqlTemplates.Update,
        //                    new
        //                    {
        //                        tableName = _extendedTableMapper.TableName,
        //                        columnValues = columnValues.Keys.Select(c =>
        //                            SqlTemplates.QueryEquals.Format(
        //                            new
        //                            {
        //                                column = c.AsSqlIdentifier(),
        //                                value = c.AsSqlParameterName()
        //                            })).ToCommaSepparate(),
        //                        filter = GetAndEqualsStatement(keyValues.Keys.ToArray())
        //                    },
        //                    columnValues.Union(keyValues).Select(c => c.ToSqlParameter())))
        //                {

        //                    if (await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 0)
        //                    {
        //                        throw new Exception("The database operation failed");
        //                    }
        //                }
        //            }
        //        }
        //    }
        //    else if (!await TryAddAsync(key, value, true).ConfigureAwait(false))
        //    {
        //        throw new Exception("The database operation failed");
        //    }
        //}

        //public async Task<TProperty> GetPropertyValueOrDefaultAsync<TProperty>(TKey key, string propertyName)
        //{
        //    var cancellationToken = CreateCancellationToken();

        //    if (!_extendedTableMapper.Columns.ContainsKey(propertyName))
        //    {
        //        throw new ArgumentException("Invalid property");
        //    }

        //    using (var connection = await GetConnectionAsync(cancellationToken))
        //    {
        //        var keyValues = _extendedTableMapper.GetExtensionColumnValues(key);

        //        using (var command = connection.CreateTextCommand(
        //            SqlTemplates.SelectTop1,
        //            new
        //            {
        //                tableName = _extendedTableMapper.TableName.AsSqlIdentifier(),
        //                columns = propertyName.AsSqlIdentifier(),
        //                filter = GetAndEqualsStatement(keyValues.Keys.ToArray())
        //            },
        //            keyValues.Select(k => k.ToSqlParameter())))
        //        {

        //            var dbValue = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);

        //            if (dbValue != null &&
        //                !(dbValue is DBNull))
        //            {
        //                return (TProperty)TypeMapper.FromDbType(
        //                    dbValue,
        //                    _extendedTableMapper.Columns[propertyName].Type,
        //                    typeof(TValue).GetProperty(propertyName).PropertyType);
        //            }
        //        }
        //    }

        //    return default(TProperty);
        //}

        #endregion

        #region IKeyQueryableMap<TKey, TValue> Members

        public async Task<QueryResult<TKey>> QueryForKeysAsync<TResult>(Expression<Func<TValue, bool>> @where, Expression<Func<TKey, TResult>> @select, int skip, int take,
            CancellationToken cancellationToken)
        {
            using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
            {
                return await QueryForKeysAsync(connection, where, @select, skip, take, cancellationToken);
            }
        }

        #endregion
    }
}
