using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Takenet.Elephant.Sql.Mapping;

namespace Takenet.Elephant.Sql
{
    public abstract class StorageBase<TEntity> : IQueryableStorage<TEntity>, IOrderedQueryableStorage<TEntity>, IDistinctQueryableStorage<TEntity>
    {
        protected StorageBase(IDatabaseDriver databaseDriver, string connectionString, ITable table, IMapper<TEntity> mapper)
        {
            ConnectionString = connectionString;
            Table = table;            
            Mapper = mapper;
            DatabaseDriver = databaseDriver;
        }

        protected IDatabaseDriver DatabaseDriver { get; }

        protected string ConnectionString { get; }

        protected ITable Table { get; }

        protected IMapper<TEntity> Mapper { get; }

        protected async Task<bool> TryRemoveAsync(IDictionary<string, object> filterValues, DbConnection connection, CancellationToken cancellationToken, DbTransaction sqlTransaction = null)
        {
            using (var command = connection.CreateDeleteCommand(DatabaseDriver, Table.Schema, Table.Name, filterValues))
            {
                if (sqlTransaction != null) command.Transaction = sqlTransaction;                
                return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
            }
        }

        protected async Task<bool> ContainsAsync(IDictionary<string, object> filterValues, DbConnection connection, CancellationToken cancellationToken)
        {
            using (var command = connection.CreateContainsCommand(DatabaseDriver, Table.Schema, Table.Name, filterValues))
            {
                return (bool)await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        public virtual Task<QueryResult<TEntity>> QueryAsync<TResult>(Expression<Func<TEntity, bool>> where, Expression<Func<TEntity, TResult>> select, int skip, int take, CancellationToken cancellationToken) 
            => QueryAsync<TResult>(@where, @select, false, skip, take, cancellationToken);

        public virtual async Task<QueryResult<TEntity>> QueryAsync<TResult>(Expression<Func<TEntity, bool>> @where, Expression<Func<TEntity, TResult>> @select, bool distinct, int skip, int take,
            CancellationToken cancellationToken)
        {
            if (select != null &&
                select.ReturnType != typeof(TEntity))
            {
                throw new NotImplementedException("The select parameter is not supported yet");
            }

            var selectColumns = Table.Columns.Keys.ToArray();
            var filter = SqlHelper.TranslateToSqlWhereClause(DatabaseDriver, where);
            var orderByColumns = Table.KeyColumnsNames;

            return await QueryAsync<TResult>(filter, selectColumns, skip, take, cancellationToken, orderByColumns, distinct: distinct);
        }

        public virtual async Task<QueryResult<TEntity>> QueryAsync<TResult, TOrderBy>(Expression<Func<TEntity, bool>> @where, Expression<Func<TEntity, TResult>> @select, Expression<Func<TEntity, TOrderBy>> orderBy, bool orderByAscending, int skip, int take, CancellationToken cancellationToken)
        {
            if (select != null &&
                select.ReturnType != typeof(TEntity))
            {
                throw new NotImplementedException("The select parameter is not supported yet");
            }

            var selectColumns = Table.Columns.Keys.ToArray();
            var filter = SqlHelper.TranslateToSqlWhereClause(DatabaseDriver, where);
            var orderByColumns = Table.KeyColumnsNames;
            if (orderBy != null)
            {
                var memberExpression = orderBy.Body as MemberExpression;
                if (memberExpression == null)
                {
                    throw new ArgumentException("Only ordering by a single member is supported by now");
                }

                orderByColumns = new[] {memberExpression.Member.Name};
            }

            return await QueryAsync<TResult>(filter, selectColumns, skip, take, cancellationToken, orderByColumns, orderByAscending);
        }

        protected virtual async Task<QueryResult<TEntity>> QueryAsync<TResult>(string filter, string[] selectColumns, int skip, int take,
            CancellationToken cancellationToken, string[] orderByColumns, bool orderByAscending = true, IDictionary<string, object> filterValues = null, bool distinct = false)
        {
            using (var connection = await GetConnectionAsync(cancellationToken))
            {
                if (filterValues != null)
                {
                    var filterValuesSql = SqlHelper.GetAndEqualsStatement(DatabaseDriver, filterValues);
                    filter = $"{filter} {DatabaseDriver.GetSqlStatementTemplate(SqlStatement.And)} ({filterValuesSql})";
                }

                int totalCount;
                using (var countCommand = connection.CreateSelectCountCommand(DatabaseDriver, Table.Schema, Table.Name, filter, filterValues))
                {
                    totalCount = Convert.ToInt32(
                        await countCommand.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false));
                }

                return new QueryResult<TEntity>(
                    new DbDataReaderAsyncEnumerable<TEntity>(
                        GetConnectionAsync,
                        c =>
                            c.CreateSelectSkipTakeCommand(DatabaseDriver, Table.Schema, Table.Name, selectColumns, filter, skip, take,
                                orderByColumns, orderByAscending, filterValues, distinct),
                        Mapper,
                        selectColumns),
                    totalCount);
            }
        }

        protected async Task<DbConnection> GetConnectionAsync(CancellationToken cancellationToken)
        {
            await Table.SynchronizeSchemaAsync(ConnectionString, DatabaseDriver, cancellationToken).ConfigureAwait(false);
            var connection = DatabaseDriver.CreateConnection(ConnectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            return connection;
        }

        protected CancellationTokenSource CreateCancellationTokenSource()
            => new CancellationTokenSource(DatabaseDriver.Timeout);

        protected virtual IDictionary<string, object> GetColumnValues(TEntity entity, bool emitNullValues = false, bool includeIdentityTypes = false)
            => Mapper.GetColumnValues(entity, emitNullValues: emitNullValues, includeIdentityTypes: includeIdentityTypes);

        protected virtual IDictionary<string, object> GetKeyColumnValues(IDictionary<string, object> columnValues, bool includeIdentityTypes = false)
            => Table
                .KeyColumnsNames
                .Where(s => columnValues.ContainsKey(s) && (includeIdentityTypes || !Table.Columns[s].IsIdentity))
                .Select(c => new { Key = c, Value = columnValues[c] })
                .ToDictionary(t => t.Key, t => t.Value);

        protected IDictionary<string, object> GetKeyColumnValues(TEntity entity, bool includeIdentityTypes = false) 
            => GetKeyColumnValues(GetColumnValues(entity, includeIdentityTypes: includeIdentityTypes), includeIdentityTypes);

        protected virtual IDictionary<string, object> GetIdentityColumnValues(IDictionary<string, object> columnValues, bool emitNullValues = false)
            => columnValues
                .Where(c => Table.Columns[c.Key].IsIdentity)
                .ToDictionary(t => t.Key, t => t.Value);

        protected virtual IDictionary<string, object> GetIdentityColumnValues(TEntity entity, bool emitNullValues = false)
            => GetIdentityColumnValues(GetColumnValues(entity, includeIdentityTypes: true), emitNullValues);

        protected IDictionary<string, object> GetIdentityKeyColumnValues(TEntity entity, bool emitNullValues = false)
            => GetKeyColumnValues(GetIdentityColumnValues(entity, emitNullValues), true);


    }
}
