using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Takenet.Elephant.Sql.Mapping;

namespace Takenet.Elephant.Sql
{
    public abstract class MapStorageBase<TKey, TValue> : StorageBase<TValue>,
        IQueryableStorage<KeyValuePair<TKey, TValue>>, IKeyQueryableMap<TKey, TValue>
    {
        protected MapStorageBase(IDatabaseDriver databaseDriver, string connectionString, ITable table,
            IMapper<TKey> keyMapper, IMapper<TValue> valueMapper)
            : base(databaseDriver, connectionString, table, valueMapper)
        {
            KeyMapper = keyMapper;
        }

        #region IQueryableStorage<KeyValuePair<TKey, TValue>> Members

        public async Task<QueryResult<KeyValuePair<TKey, TValue>>> QueryAsync<TResult>(
            Expression<Func<KeyValuePair<TKey, TValue>, bool>> @where,
            Expression<Func<KeyValuePair<TKey, TValue>, TResult>> @select, int skip, int take,
            CancellationToken cancellationToken)
        {
            if (select != null &&
                select.ReturnType != typeof(KeyValuePair<TKey, TValue>))
            {
                throw new NotImplementedException("The 'select' parameter is not supported yet");
            }

            var selectColumns = Table.Columns.Keys.ToArray();
            var orderByColumns = Table.KeyColumnsNames;

            var expressionParameterReplacementDictionary = new Dictionary<string, string>();

            if (KeyMapper is ValueMapper<TKey>)
            {
                expressionParameterReplacementDictionary.Add("Key", ((ValueMapper<TKey>) KeyMapper).ColumnName);
            }

            if (Mapper is ValueMapper<TValue>)
            {
                expressionParameterReplacementDictionary.Add("Value", ((ValueMapper<TValue>) Mapper).ColumnName);
            }

            var filter = SqlHelper.TranslateToSqlWhereClause(where, expressionParameterReplacementDictionary);
            var connection = await GetConnectionAsync(cancellationToken);
            int totalCount;
            using (var countCommand = connection.CreateSelectCountCommand(Table.Name, filter))
            {
                totalCount = (int) await countCommand.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            }

            var command = connection.CreateSelectSkipTakeCommand(
                Table.Name, selectColumns, filter, skip, take, orderByColumns);

            return new QueryResult<KeyValuePair<TKey, TValue>>(
                new DbDataReaderAsyncEnumerable<KeyValuePair<TKey, TValue>>(command,
                    new KeyValuePairMapper<TKey, TValue>(KeyMapper, Mapper), selectColumns),
                totalCount);
        }

        #endregion

        #region IKeyQueryableMap<TKey, TValue> Members

        public async Task<QueryResult<TKey>> QueryForKeysAsync<TResult>(Expression<Func<TValue, bool>> @where,
            Expression<Func<TKey, TResult>> @select, int skip, int take,
            CancellationToken cancellationToken)
        {
            var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);            
            return await QueryForKeysAsync(connection, where, @select, skip, take, cancellationToken);
            
        }

        #endregion

        protected IMapper<TKey> KeyMapper { get; }

        protected virtual IDictionary<string, object> GetColumnValues(TKey key, TValue value)
        {
            return KeyMapper
                .GetColumnValues(key)
                .Union(GetColumnValues(value))
                .ToDictionary(t => t.Key, t => t.Value);
        }

        protected virtual Task<bool> TryRemoveAsync(TKey key, DbConnection connection,
            CancellationToken cancellationToken, SqlTransaction sqlTransaction = null)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            var keyColumnValues = KeyMapper.GetColumnValues(key);
            return TryRemoveAsync(keyColumnValues, connection, cancellationToken, sqlTransaction);

        }

        protected virtual Task<bool> ContainsKeyAsync(TKey key, DbConnection connection,
            CancellationToken cancellationToken)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            var keyColumnValues = KeyMapper.GetColumnValues(key);
            return ContainsAsync(keyColumnValues, connection, cancellationToken);
        }

        protected virtual Task<IAsyncEnumerable<TKey>> GetKeysAsync(DbConnection connection,
            CancellationToken cancellationToken)
        {
            var selectColumns = Table.KeyColumnsNames;
            var command = connection.CreateSelectCommand(Table.Name, null, selectColumns);
            return Task.FromResult<IAsyncEnumerable<TKey>>(
                new DbDataReaderAsyncEnumerable<TKey>(command, KeyMapper, selectColumns));
        }

        protected virtual async Task<QueryResult<TKey>> QueryForKeysAsync<TResult>(
            DbConnection connection,
            Expression<Func<TValue, bool>> where,
            Expression<Func<TKey, TResult>> select,
            int skip,
            int take,
            CancellationToken cancellationToken)
        {
            if (select != null &&
                select.ReturnType != typeof (TKey))
            {
                throw new NotImplementedException("The 'select' parameter is not supported yet");
            }

            var selectColumns = Table.KeyColumnsNames;
            var filter = SqlHelper.TranslateToSqlWhereClause(where);
            int totalCount;
            using (var countCommand = connection.CreateSelectCountCommand(Table.Name, filter))
            {
                totalCount = (int) await countCommand.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            }

            var command = connection.CreateSelectSkipTakeCommand(
                Table.Name, selectColumns, filter, skip, take, selectColumns);

            return new QueryResult<TKey>(
                new DbDataReaderAsyncEnumerable<TKey>(command, KeyMapper, selectColumns),
                totalCount);
        }
    }
}