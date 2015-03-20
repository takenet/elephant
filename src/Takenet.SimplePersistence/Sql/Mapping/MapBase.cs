using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence.Sql.Mapping
{
    public abstract class MapBase<TKey, TValue> : StorageBase<TValue>, IQueryableStorage<TKey, TValue>
    {
        #region Private Fields

        protected readonly IExtendedTableMapper<TKey, TValue> _extendedTableMapper;

        #endregion

        #region Constructor

        public MapBase(IExtendedTableMapper<TKey, TValue> keyTableMapper, string connectionString)
            : base(keyTableMapper, connectionString)
        {
            _extendedTableMapper = keyTableMapper;
        }

        #endregion

        #region Protected Members

        protected async Task<bool> TryAddAsync(TKey key, TValue value, bool overwrite, SqlConnection connection, CancellationToken cancellationToken)
        {
            var keyValues = _extendedTableMapper.GetKeyColumnValues(key, value);
            var columnValues = _extendedTableMapper.GetColumnValues(key, value);

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
                    tableName = _extendedTableMapper.TableName.AsSqlIdentifier(),
                    columns = columnValues.Keys.Select(c => c.AsSqlIdentifier()).ToCommaSepparate(),
                    values = columnValues.Keys.Select(v => v.AsSqlParameterName()).ToCommaSepparate(),
                    filter = GetAndEqualsStatement(keyValues.Keys.ToArray())
                },
                columnValues.Select(c => c.ToSqlParameter())))
            {
                return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
            }
        }

        protected async Task<IEnumerable<TValue>> GetValuesByExtensionKeyAsync(TKey key, SqlConnection connection, CancellationToken cancellationToken)
        {
            var result = new List<TValue>();
            var extensionKeyValues = _extendedTableMapper.GetExtensionKeyColumnValues(key);
            var selectColumns = _extendedTableMapper.Columns.Keys.ToArray();

            using (var command = connection.CreateTextCommand(
                SqlTemplates.Select,
                new
                {
                    columns = selectColumns.Select(c => c.AsSqlIdentifier()).ToCommaSepparate(),
                    tableName = _extendedTableMapper.TableName.AsSqlIdentifier(),
                    filter = GetAndEqualsStatement(extensionKeyValues.Keys.ToArray())
                },
                extensionKeyValues.Select(k => k.ToSqlParameter())))
            {
                using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
                {
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        result.Add(_extendedTableMapper.Create(reader, selectColumns));
                    }
                }
            }
            return result;
        }

        protected async Task<bool> TryRemoveAsync(TKey key, SqlConnection connection, CancellationToken cancellationToken)
        {
            var extensionKeyValues = _extendedTableMapper.GetExtensionKeyColumnValues(key);

            using (var command = connection.CreateTextCommand(
                SqlTemplates.Delete,
                new
                {
                    tableName = _extendedTableMapper.TableName.AsSqlIdentifier(),
                    filter = GetAndEqualsStatement(extensionKeyValues.Keys.ToArray())
                },
                extensionKeyValues.Select(k => k.ToSqlParameter())))
            {
                return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
            }
        }

        protected async Task<bool> ContainsKeyAsync(TKey key, SqlConnection connection, CancellationToken cancellationToken)
        {
            var extensionKeyValues = _extendedTableMapper.GetExtensionKeyColumnValues(key);

            using (var command = connection.CreateTextCommand(
                SqlTemplates.Exists,
                new
                {
                    tableName = _extendedTableMapper.TableName.AsSqlIdentifier(),
                    filter = GetAndEqualsStatement(extensionKeyValues.Keys.ToArray())
                },
                extensionKeyValues.Select(k => k.ToSqlParameter())))
            {
                return (bool)await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        protected async Task<IEnumerable<TKey>> GetKeysAsync(SqlConnection connection, CancellationToken cancellationToken)
        {
            var selectColumns = _extendedTableMapper.KeyColumns.ToArray();
            var result = new List<TKey>();

            using (var command = connection.CreateTextCommand(
                SqlTemplates.Select,
                new
                {
                    columns = selectColumns.ToCommaSepparate(),
                    tableName = _extendedTableMapper.TableName.AsSqlIdentifier(),
                    filter = "1 = 1"
                }))
            {

                using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
                {
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        result.Add(_extendedTableMapper.CreateExtension(reader, selectColumns));
                    }
                }
            }

            return result;
        }


        #endregion

        #region IQueryableStorage<TKey, TValue>

        public async Task<QueryResult<TKey>> QueryForKeysAsync<TResult>(Expression<Func<TValue, bool>> where,
            Expression<Func<TKey, TResult>> select,
            int skip,
            int take,
            CancellationToken cancellationToken)
        {
            if (select != null &&
                select.ReturnType != typeof(TKey))
            {
                throw new NotImplementedException("The select parameter is not supported yet");
            }

            var result = new List<TKey>();

            var selectColumns = _extendedTableMapper.ExtensionColumns.ToArray();
            if (selectColumns.Length == 0)
            {
                // The keys is not part of the extension
                selectColumns = _extendedTableMapper.KeyColumns.ToArray();
            }

            var selectColumnsCommaSepareted = selectColumns.Select(c => c.AsSqlIdentifier()).ToCommaSepparate();
            var tableName = _extendedTableMapper.TableName.AsSqlIdentifier();
            var filters = GetFilters(where);

            using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
            {
                var totalCount = -1;
                using (var countCommand = connection.CreateTextCommand(
                    SqlTemplates.SelectCount,
                    new
                    {
                        tableName = tableName,
                        filter = filters
                    }))
                {
                    totalCount = Convert.ToInt32(await countCommand.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false));
                }

                using (var command = connection.CreateTextCommand(
                    SqlTemplates.SelectSkipTake,
                    new
                    {
                        columns = selectColumnsCommaSepareted,
                        tableName = tableName,
                        filter = filters,
                        skip = skip,
                        take = take,
                        keys = selectColumnsCommaSepareted
                    }))
                {
                    using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
                    {
                        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                        {
                            // TODO Apply select parameter
                            result.Add(_extendedTableMapper.CreateExtension(reader, selectColumns));
                        }
                    }
                }
                return new QueryResult<TKey>(result, totalCount);
            }
        }

        #endregion
    }
}
