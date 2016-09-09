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
    public abstract class StorageBase<TEntity> : IQueryableStorage<TEntity>
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

            return await QueryAsync<TResult>(filter, selectColumns, skip, take, cancellationToken, QueryOrderByColumns, QueryOrderByAscending);
        }

        protected virtual string[] QueryOrderByColumns => Table.KeyColumnsNames;

        protected virtual bool QueryOrderByAscending => true;

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

        protected virtual IDictionary<string, object> GetColumnValues(TEntity entity, bool emitDefaultValues = false)
        {
            return Mapper.GetColumnValues(entity, emitDefaultValues: emitDefaultValues);
        }

        protected IDictionary<string, object> GetKeyColumnValues(TEntity entity)
        {
            return GetKeyColumnValues(GetColumnValues(entity));
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
