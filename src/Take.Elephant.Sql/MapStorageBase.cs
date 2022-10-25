using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Sql
{
    public abstract class MapStorageBase<TKey, TValue> : StorageBase<TValue>,
        IQueryableStorage<KeyValuePair<TKey, TValue>>, IKeyQueryableMap<TKey, TValue>
    {
        protected MapStorageBase(IDatabaseDriver databaseDriver, string connectionString, ITable table, IMapper<TKey> keyMapper, IMapper<TValue> valueMapper)
            : base(databaseDriver, connectionString, table, valueMapper)
        {
            KeyMapper = keyMapper;
        }

        #region IQueryableStorage<KeyValuePair<TKey, TValue>> Members

        public virtual async Task<QueryResult<KeyValuePair<TKey, TValue>>> QueryAsync<TResult>(
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

            var filter = SqlHelper.TranslateToSqlWhereClause(DatabaseDriver, where, Mapper.DbTypeMapper, expressionParameterReplacementDictionary);
            using (var connection = await GetConnectionAsync(cancellationToken))
            {
                int totalCount = 0;
                if (FetchQueryResultTotal)
                {
                    using (var countCommand = connection.CreateSelectCountCommand(DatabaseDriver, Table, filter.Where, filter.FilterValues))
                    {
                        totalCount = Convert.ToInt32(
                            await countCommand.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false));
                    }
                }

                return new QueryResult<KeyValuePair<TKey, TValue>>(
                    new DbDataReaderAsyncEnumerable<KeyValuePair<TKey, TValue>>(
                        GetConnectionAsync,
                        c =>
                            c.CreateSelectSkipTakeCommand(DatabaseDriver, Table, selectColumns, filter.Where, skip, take, orderByColumns, filterValues: filter.FilterValues),
                        new KeyValuePairMapper<TKey, TValue>(KeyMapper, Mapper),
                        selectColumns,
                        UseFullyAsyncEnumerator),
                    totalCount);
            }
        }

        #endregion

        #region IKeyQueryableMap<TKey, TValue> Members

        public virtual async Task<QueryResult<TKey>> QueryForKeysAsync<TResult>(Expression<Func<TValue, bool>> @where,
            Expression<Func<TKey, TResult>> @select, int skip, int take,
            CancellationToken cancellationToken)
        {
            using (var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false))
            {
                if (select != null &&
                    select.ReturnType != typeof (TKey))
                {
                    throw new NotImplementedException("The 'select' parameter is not supported yet");
                }

                var selectColumns = Table.KeyColumnsNames;
                var filter = SqlHelper.TranslateToSqlWhereClause(DatabaseDriver, where, Mapper.DbTypeMapper);
                int totalCount = 0;
                if (FetchQueryResultTotal)
                {
                    using (var countCommand = connection.CreateSelectCountCommand(DatabaseDriver, Table, filter.Where, filter.FilterValues))
                    {
                        totalCount = Convert.ToInt32(
                            await countCommand.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false));
                    }
                }

                return new QueryResult<TKey>(
                    new DbDataReaderAsyncEnumerable<TKey>(
                        GetConnectionAsync, 
                        c => c.CreateSelectSkipTakeCommand(
                            DatabaseDriver,
                            Table,
                            selectColumns,
                            filter.Where,
                            skip,
                            take,
                            selectColumns,
                            filterValues: filter.FilterValues),
                        KeyMapper, 
                        selectColumns,
                        UseFullyAsyncEnumerator),
                    totalCount);
            }
        }

        #endregion

        protected IMapper<TKey> KeyMapper { get; }

        protected virtual IDictionary<string, object> GetColumnValues(TKey key, TValue value, bool valueEmitDefaultValues = false)
        {
            return KeyMapper
                .GetColumnValues(key)
                .Union(GetColumnValues(value, valueEmitDefaultValues))
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
    }
}