using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Takenet.SimplePersistence.Sql.Mapping;

namespace Takenet.SimplePersistence.Sql
{
    public abstract class StorageBase<TEntity> : IQueryableStorage<TEntity>
    {
        #region Fields

        protected abstract IMapper<TEntity> Mapper { get; }
        protected readonly ITable Table;
        private readonly string _connectionString;

        #endregion

        #region Constructor

        protected StorageBase(ITable table,  string connectionString)
        {
            Table = table;
            _connectionString = connectionString;
        }

        #endregion

        protected async Task<bool> TryRemoveAsync(IDictionary<string, object> keyValues, SqlConnection connection, CancellationToken cancellationToken)
        {
            using (var command = connection.CreateTextCommand(
                SqlTemplates.Delete,
                new
                {
                    tableName = Table.TableName.AsSqlIdentifier(),
                    filter = GetAndEqualsStatement(keyValues.Keys.ToArray())
                },
                keyValues.Select(k => k.ToSqlParameter())))
            {
                return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
            }
        }

        protected async Task<bool> ContainsKeyAsync(IDictionary<string, object> keyValues, SqlConnection connection, CancellationToken cancellationToken)
        {
            using (var command = connection.CreateTextCommand(
                SqlTemplates.Exists,
                new
                {
                    tableName = Table.TableName.AsSqlIdentifier(),
                    filter = GetAndEqualsStatement(keyValues.Keys.ToArray())
                },
                keyValues.Select(k => k.ToSqlParameter())))
            {
                return (bool)await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        #region IQueryableStorage<TEntity>

        public async Task<QueryResult<TEntity>> QueryAsync<TResult>(Expression<Func<TEntity, bool>> where, Expression<Func<TEntity, TResult>> select, int skip, int take, CancellationToken cancellationToken)
        {
            if (select != null &&
                select.ReturnType != typeof(TEntity))
            {
                throw new NotImplementedException("The select parameter is not supported yet");
            }

            var result = new List<TEntity>();
            var selectColumns = Table.Columns.Keys;
            var selectColumnsCommaSepparate = selectColumns.Select(c => c.AsSqlIdentifier()).ToCommaSepparate();
            var keysColumnsCommaSepparate = Table.KeyColumns.Select(c => c.AsSqlIdentifier()).ToCommaSepparate();
            var tableName = Table.TableName.AsSqlIdentifier();
            var filters = GetFilters(where);        
            var connection = await GetConnectionAsync(cancellationToken);            
            int totalCount;
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

            var command = connection.CreateTextCommand(
                SqlTemplates.SelectSkipTake,
                new
                {
                    columns = selectColumnsCommaSepparate,
                    tableName = tableName,
                    filter = filters,
                    skip = skip,
                    take = take,
                    keys = keysColumnsCommaSepparate
                });
                                            
            return new QueryResult<TEntity>(new SqlDataReaderAsyncEnumerable<TEntity>(command, Mapper, selectColumns.ToArray()), totalCount);            
        }

        #endregion

        #region Protected Members

        protected CancellationToken CreateCancellationToken()
        {
            return CancellationToken.None;
        }

        protected string GetAndEqualsStatement(string[] columns)
        {
            return GetSeparateEqualsStatement(SqlTemplates.And, columns);
        }

        protected string GetAndEqualsStatement(string[] columns, string[] parameters)
        {
            return GetSeparateEqualsStatement(SqlTemplates.And, columns, parameters);
        }

        protected string GetCommaEqualsStatement(string[] columns)
        {
            return GetSeparateEqualsStatement(",", columns);
        }

        protected string GetCommaEqualsStatement(string[] columns, string[] parameters)
        {
            return GetSeparateEqualsStatement(",", columns, parameters);
        }

        protected string GetSeparateEqualsStatement(string separator, string[] columns)
        {
            return GetSeparateEqualsStatement(separator, columns, columns);
        }

        protected string GetSeparateEqualsStatement(string separator, string[] columns, string[] parameters)
        {
            if (columns.Length == 0)
            {
                throw new ArgumentException("The columns are empty");
            }

            if (parameters.Length == 0)
            {
                throw new ArgumentException("The parameters are empty");
            }

            var filter = new StringBuilder();

            for (int i = 0; i < columns.Length; i++)
            {
                var column = columns[i];
                var parameter = parameters[i];

                filter.Append(
                    GetEqualsStatement(column, parameter));

                if (i + 1 < columns.Length)
                {
                    filter.AppendFormat(" {0} ", separator);
                }
            }

            return filter.ToString();
        }

        protected string GetEqualsStatement(string column)
        {
            return GetEqualsStatement(column, column);
        }

        protected string GetEqualsStatement(string column, string parameter)
        {
            return SqlTemplates.QueryEquals.Format(
                new
                {
                    column = column.AsSqlIdentifier(),
                    value = parameter.AsSqlParameterName()
                });
        }

        protected async Task<SqlConnection> GetConnectionAsync(CancellationToken cancellationToken)
        {
            var connection = GetConnection();
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await CheckTableSchemaAsync(connection, cancellationToken).ConfigureAwait(false);
            return connection;
        }

        protected string GetFilters(Expression<Func<TEntity, bool>> where)
        {
            var translator = new ExpressionTranslator();
            return translator.GetStatement(where);
        }

        protected IDictionary<string, object> GetKeyColumnValues(TEntity entity)
        {
            return Mapper.GetColumnValues(entity, Table.KeyColumns);
        }

        #endregion

        #region Private Methods

        private SqlConnection GetConnection()
        {
            return new SqlConnection(_connectionString);
        }

        private bool _schemaChecked;
        private readonly SemaphoreSlim _schemaValidationSemaphore = new SemaphoreSlim(1);

        private async Task CheckTableSchemaAsync(SqlConnection connection, CancellationToken cancellationToken)
        {
            if (!_schemaChecked)
            {
                await _schemaValidationSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

                try
                {
                    if (!_schemaChecked)
                    {
                        // Check if the table exists
                        var tableExists = await connection.ExecuteScalarAsync<bool>(
                            SqlTemplates.TableExists.Format(
                            new
                            {
                                tableName = Table.TableName
                            }),
                            cancellationToken).ConfigureAwait(false);

                        if (!tableExists)
                        {
                            await CreateTableAsync(connection, cancellationToken).ConfigureAwait(false);
                        }

                        await ValidateTableSchema(connection, cancellationToken).ConfigureAwait(false);
                        _schemaChecked = true;
                    }
                }
                finally
                {
                    _schemaValidationSemaphore.Release();
                }
            }
        }

        private async Task ValidateTableSchema(SqlConnection connection, CancellationToken cancellationToken)
        {
            var tableColumnsDictionary = new Dictionary<string, string>();

            using (var command = connection.CreateCommand())
            {
                command.CommandText = SqlTemplates.GetTableColumns.Format(
                    new
                    {
                        tableName = Table.TableName
                    });

                using (var reader = await command.ExecuteReaderAsync(cancellationToken))
                {
                    while (await reader.ReadAsync(cancellationToken))
                    {
                        tableColumnsDictionary.Add((string)reader[0], (string)reader[1]);
                    }
                }
            }

            var columnsToBeCreated = new List<KeyValuePair<string, SqlType>>();

            foreach (var column in Table.Columns)
            {
                // Check if the column exists in the database
                if (!tableColumnsDictionary.ContainsKey(column.Key))
                {
                    columnsToBeCreated.Add(column);
                }
                // Checks if the existing column type matches with the definition
                // The comparion is with startsWith for the NVARCHAR values
                else if (!GetSqlTypeSql(column.Value).StartsWith(
                         tableColumnsDictionary[column.Key], StringComparison.OrdinalIgnoreCase))
                {
                    throw new InvalidOperationException("The existing column '{columnName}' type '{columnType}' is not compatible with the definition type '{dbType}'".Format(new { columnName = column.Key, columnType = tableColumnsDictionary[column.Key], dbType = column.Value }));
                }
            }

            if (columnsToBeCreated.Any())
            {
                await CreateColumnsAsync(connection, columnsToBeCreated, cancellationToken);
            }

        }

        private async Task CreateColumnsAsync(SqlConnection connection, IEnumerable<KeyValuePair<string, SqlType>> columns, CancellationToken cancellationToken)
        {
            var command = connection.CreateCommand();
            command.CommandText = SqlTemplates.AlterTableAddColumn.Format(
                new
                {
                    tableName = Table.TableName,
                    columnDefinition = GetColumnsDefinitionSql(columns).TrimEnd(',')
                });

            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        private async Task CreateTableAsync(SqlConnection connection, CancellationToken cancellationToken)
        {
            if (Table.Columns.Count == 0)
            {
                throw new InvalidOperationException("The table mapper has no defined columns");
            }

            if (!Table.KeyColumns.Any())
            {
                throw new InvalidOperationException("The table mapper has no defined key columns");
            }

            // Table columns
            var createTableSqlBuilder = new StringBuilder();
            createTableSqlBuilder.AppendLine(GetColumnsDefinitionSql(Table.Columns));

            // Constraints
            createTableSqlBuilder.AppendLine(
                SqlTemplates.PrimaryKeyConstraintDefinition.Format(
                    new
                    {
                        tableName = Table.TableName,
                        columns = Table.KeyColumns.Select(c => c.AsSqlIdentifier()).ToCommaSepparate()
                    })
            );

            // Create table 
            var createTableSql = SqlTemplates.CreateTable.Format(
                new
                {
                    tableName = Table.TableName.AsSqlIdentifier(),
                    tableDefinition = createTableSqlBuilder.ToString()
                });

            await connection.ExecuteNonQueryAsync(
                createTableSql,
                cancellationToken).ConfigureAwait(false);

        }

        private string GetColumnsDefinitionSql(IEnumerable<KeyValuePair<string, SqlType>> columns)
        {
            var columnSqlBuilder = new StringBuilder();

            foreach (var column in columns)
            {
                // All columns, except the key are nullable
                if (Table.KeyColumns.Contains(column.Key))
                {
                    if (column.Value.IsIdentity)
                    {
                        columnSqlBuilder.AppendLine(
                            SqlTemplates.IdentityColumnDefinition.Format(
                                new
                                {
                                    columnName = column.Key.AsSqlIdentifier(),
                                    sqlType = GetSqlTypeSql(column.Value)
                                })
                            );
                    }
                    else
                    {
                        columnSqlBuilder.AppendLine(
                            SqlTemplates.ColumnDefinition.Format(
                                new
                                {
                                    columnName = column.Key.AsSqlIdentifier(),
                                    sqlType = GetSqlTypeSql(column.Value)
                                })
                            );
                    }
                }
                else
                {
                    columnSqlBuilder.AppendLine(
                        SqlTemplates.NullableColumnDefinition.Format(
                            new
                            {
                                columnName = column.Key.AsSqlIdentifier(),
                                sqlType = GetSqlTypeSql(column.Value)
                            })
                        );
                }

                columnSqlBuilder.Append(",");
            }
            return columnSqlBuilder.ToString();
        }

        private static string GetSqlTypeSql(SqlType sqlType)
        {
            var typeSql = SqlTemplates.ResourceManager.GetString(
                string.Format("DbType{0}", sqlType.Type));

            if (sqlType.Length.HasValue)
            {
                string lengthValue = sqlType.Length == int.MaxValue ? SqlType.MAX_LENGTH : sqlType.Length.ToString();
                typeSql = typeSql.Format(new
                {
                    length = lengthValue
                });
            }

            if (sqlType.Precision.HasValue)
            {
                typeSql = typeSql.Format(new
                {
                    precision = sqlType.Precision
                });
            }

            if (sqlType.Scale.HasValue)
            {
                typeSql = typeSql.Format(new
                {
                    scale = sqlType.Scale
                });
            }

            return typeSql;
        }

        #endregion
    }
}
