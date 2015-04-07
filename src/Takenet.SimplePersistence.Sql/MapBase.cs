using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Takenet.SimplePersistence.Sql.Mapping;

namespace Takenet.SimplePersistence.Sql
{
    public abstract class MapBase<TKey, TValue> : StorageBase<TValue> //, IQueryableStorage<TKey, TValue>
    {
        protected abstract IMapper<TKey> KeyMapper { get; }

        protected MapBase(ITable table, string connectionString)
            : base(table, connectionString)
        {
            
        }

        #region Protected Members

        protected IDictionary<string, object> GetColumnValues(TKey key, TValue value)
        {
            return KeyMapper
                .GetColumnValues(key)
                .Concat(Mapper.GetColumnValues(value))
                .ToDictionary(t => t.Key, t => t.Value);
        }

        protected async Task<bool> TryAddAsync(TKey key, TValue value, bool overwrite, SqlConnection connection, CancellationToken cancellationToken)
        {
            var columnValues = GetColumnValues(key, value);
            var keyColumnValues = GetKeyColumnValues(columnValues);

            string sqlTemplate;

            if (overwrite)
            {
                sqlTemplate = string.Format("{0}; {1};", SqlTemplates.Delete, SqlTemplates.InsertWhereNotExists);
            }
            else
            {
                sqlTemplate = SqlTemplates.InsertWhereNotExists;
            }

            using (var command = connection.CreateTextCommand(
                sqlTemplate,
                new
                {
                    tableName = Table.Name.AsSqlIdentifier(),
                    columns = columnValues.Keys.Select(c => c.AsSqlIdentifier()).ToCommaSepparate(),
                    values = columnValues.Keys.Select(v => v.AsSqlParameterName()).ToCommaSepparate(),
                    filter = GetAndEqualsStatement(keyColumnValues.Keys.ToArray())
                },
                columnValues.Select(c => c.ToSqlParameter())))
            {
                return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
            }
        }

        protected IAsyncEnumerable<TValue> GetValuesByKey(TKey key, SqlConnection connection, CancellationToken cancellationToken)
        {
            var keyValues = KeyMapper.GetColumnValues(key);
            var selectColumns = Table.Columns.Keys.ToArray();

            var command = connection.CreateTextCommand(
                SqlTemplates.Select,
                new
                {
                    columns = selectColumns.Select(c => c.AsSqlIdentifier()).ToCommaSepparate(),
                    tableName = Table.Name.AsSqlIdentifier(),
                    filter = GetAndEqualsStatement(keyValues.Keys.ToArray())
                },
                keyValues.Select(k => k.ToSqlParameter()));

            return new SqlDataReaderAsyncEnumerable<TValue>(command, Mapper, selectColumns);
        }

        protected Task<bool> TryRemoveAsync(TKey key, SqlConnection connection, CancellationToken cancellationToken)
        {
            var keyValues = KeyMapper.GetColumnValues(key);
            return TryRemoveAsync(keyValues, connection, cancellationToken);
        }

        protected async Task<bool> ContainsKeyAsync(TKey key, SqlConnection connection, CancellationToken cancellationToken)
        {
            var keyValues = KeyMapper.GetColumnValues(key);

            return await ContainsKeyAsync(keyValues, connection, cancellationToken);
        }



        protected async Task<IEnumerable<TKey>> GetKeysAsync(SqlConnection connection, CancellationToken cancellationToken)
        {
            var selectColumns = Table.KeyColumns.ToArray();
            var result = new List<TKey>();

            using (var command = connection.CreateTextCommand(
                SqlTemplates.Select,
                new
                {
                    columns = selectColumns.ToCommaSepparate(),
                    tableName = Table.Name.AsSqlIdentifier(),
                    filter = "1 = 1"
                }))
            {

                using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
                {
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        result.Add(KeyMapper.Create(reader, selectColumns));
                    }
                }
            }

            return result;
        }


        #endregion

        //#region IQueryableStorage<TKey, TValue>

        //public async Task<QueryResult<TKey>> QueryForKeysAsync<TResult>(Expression<Func<TValue, bool>> where,
        //    Expression<Func<TKey, TResult>> select,
        //    int skip,
        //    int take,
        //    CancellationToken cancellationToken)
        //{
        //    if (select != null &&
        //        select.ReturnType != typeof(TKey))
        //    {
        //        throw new NotImplementedException("The select parameter is not supported yet");
        //    }

        //    var result = new List<TKey>();

        //    var selectColumns = _extendedTableMapper.ExtensionColumns.ToArray();
        //    if (selectColumns.Length == 0)
        //    {
        //        // The keys is not part of the extension
        //        selectColumns = _extendedTableMapper.KeyColumns.ToArray();
        //    }

        //    var selectColumnsCommaSepareted = selectColumns.Select(c => c.AsSqlIdentifier()).ToCommaSepparate();
        //    var tableName = _extendedTableMapper.TableName.AsSqlIdentifier();
        //    var filters = GetFilters(where);

        //    using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
        //    {
        //        var totalCount = -1;
        //        using (var countCommand = connection.CreateTextCommand(
        //            SqlTemplates.SelectCount,
        //            new
        //            {
        //                tableName = tableName,
        //                filter = filters
        //            }))
        //        {
        //            totalCount = Convert.ToInt32(await countCommand.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false));
        //        }

        //        using (var command = connection.CreateTextCommand(
        //            SqlTemplates.SelectSkipTake,
        //            new
        //            {
        //                columns = selectColumnsCommaSepareted,
        //                tableName = tableName,
        //                filter = filters,
        //                skip = skip,
        //                take = take,
        //                keys = selectColumnsCommaSepareted
        //            }))
        //        {
        //            using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        //            {
        //                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        //                {
        //                    // TODO Apply select parameter
        //                    result.Add(_extendedTableMapper.CreateExtension(reader, selectColumns));
        //                }
        //            }
        //        }
        //        return new QueryResult<TKey>(result, totalCount);
        //    }
        //}

        //#endregion
    }
}
