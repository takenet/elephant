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
    public abstract class StorageBase<TEntity> : IQueryableStorage<TEntity>, IOrderedQueryableStorage<TEntity>
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
            using (var command = connection.CreateDeleteCommand(DatabaseDriver, Table.Name, filterValues))
            {
                if (sqlTransaction != null) command.Transaction = sqlTransaction;                
                return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
            }
        }

        protected async Task<bool> ContainsAsync(IDictionary<string, object> filterValues, DbConnection connection, CancellationToken cancellationToken)
        {
            using (var command = connection.CreateContainsCommand(DatabaseDriver, Table.Name, filterValues))
            {
                return (bool)await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        public async Task<QueryResult<TEntity>> QueryAsync<TResult>(Expression<Func<TEntity, bool>> where, Expression<Func<TEntity, TResult>> select, int skip, int take, CancellationToken cancellationToken)
        {
            if (select != null && 
                select.ReturnType != typeof(TEntity))
            {
                throw new NotImplementedException("The select parameter is not supported yet");
            }

            var selectColumns = Table.Columns.Keys.ToArray();
            var filter = SqlHelper.TranslateToSqlWhereClause(DatabaseDriver, where);
            var orderByColumns = Table.KeyColumnsNames;

            return await QueryAsync<TResult>(filter, selectColumns, skip, take, cancellationToken, orderByColumns);
        }

        public async Task<QueryResult<TEntity>> QueryAsync<TResult, TOrderBy>(Expression<Func<TEntity, bool>> @where, Expression<Func<TEntity, TResult>> @select, Expression<Func<TEntity, TOrderBy>> orderBy, bool orderByAscending, int skip, int take, CancellationToken cancellationToken)
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
            CancellationToken cancellationToken, string[] orderByColumns, bool orderByAscending = true, IDictionary<string, object> filterValues = null)
        {
            using (var connection = await GetConnectionAsync(cancellationToken))
            {
                if (filterValues != null)
                {
                    var filterValuesSql = SqlHelper.GetAndEqualsStatement(DatabaseDriver, filterValues);
                    filter = $"{filter} {DatabaseDriver.GetSqlStatementTemplate(SqlStatement.And)} ({filterValuesSql})";
                }

                int totalCount;
                using (var countCommand = connection.CreateSelectCountCommand(DatabaseDriver, Table.Name, filter, filterValues))
                {
                    totalCount = Convert.ToInt32(
                        await countCommand.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false));
                }

                return new QueryResult<TEntity>(
                    new DbDataReaderAsyncEnumerable<TEntity>(
                        GetConnectionAsync,
                        c =>
                            c.CreateSelectSkipTakeCommand(DatabaseDriver, Table.Name, selectColumns, filter, skip, take,
                                orderByColumns, orderByAscending, filterValues),
                        Mapper,
                        selectColumns),
                    totalCount);
            }
        }




        protected async Task<DbConnection> GetConnectionAsync(CancellationToken cancellationToken)
        {
            var connection = DatabaseDriver.CreateConnection(ConnectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await CheckTableSchemaAsync(connection, cancellationToken).ConfigureAwait(false);
            return connection;
        }

        protected CancellationTokenSource CreateCancellationTokenSource()
        {
            return new CancellationTokenSource(DatabaseDriver.Timeout);
        }

        protected virtual IDictionary<string, object> GetColumnValues(TEntity entity, bool emitDefaultValues = false, bool includeIdentityTypes = false)
        {
            return Mapper.GetColumnValues(entity, emitDefaultValues: emitDefaultValues, includeIdentityTypes: includeIdentityTypes);
        }

        protected IDictionary<string, object> GetKeyColumnValues(TEntity entity, bool includeIdentityTypes = false)
        {
            return GetKeyColumnValues(GetColumnValues(entity, includeIdentityTypes: includeIdentityTypes));
        }

        protected virtual IDictionary<string, object> GetKeyColumnValues(IDictionary<string, object> columnValues)
        {
            return Table
                .KeyColumnsNames
                .Where(columnValues.ContainsKey)
                .Select(c => new { Key = c, Value = columnValues[c] })
                .ToDictionary(t => t.Key, t => t.Value);
        }     

        protected bool SchemaChecked;
        private readonly SemaphoreSlim _schemaValidationSemaphore = new SemaphoreSlim(1);

        private async Task CheckTableSchemaAsync(DbConnection connection, CancellationToken cancellationToken)
        {
            if (!SchemaChecked)
            {
                await _schemaValidationSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

                try
                {
                    if (!SchemaChecked)
                    {
                        // Check if the table exists
                        var tableExists = await connection.ExecuteScalarAsync<bool>(
                            DatabaseDriver.GetSqlStatementTemplate(SqlStatement.TableExists).Format(
                            new
                            {
                                tableName = Table.Name
                            }),
                            cancellationToken).ConfigureAwait(false);

                        if (!tableExists)
                        {
                            await DatabaseSchema.CreateTableAsync(DatabaseDriver, connection, Table, cancellationToken).ConfigureAwait(false);
                        }

                        await DatabaseSchema.UpdateTableSchemaAsync(DatabaseDriver, connection, Table, cancellationToken).ConfigureAwait(false);
                        SchemaChecked = true;
                    }
                }
                finally
                {
                    _schemaValidationSemaphore.Release();
                }
            }
        }


    }
}
