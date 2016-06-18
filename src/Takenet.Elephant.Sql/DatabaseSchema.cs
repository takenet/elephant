using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Takenet.Elephant.Sql.Mapping;

namespace Takenet.Elephant.Sql
{
    internal static class DatabaseSchema
    {
        internal static async Task CreateTableAsync(IDatabaseDriver databaseDriver, DbConnection connection, ITable table, CancellationToken cancellationToken)
        {
            if (table.Columns.Count == 0)
            {
                throw new InvalidOperationException("The table mapper has no defined columns");
            }

            if (!table.KeyColumnsNames.Any())
            {
                throw new InvalidOperationException("The table mapper has no defined key columns");
            }

            // Table columns
            var createTableSqlBuilder = new StringBuilder();
            createTableSqlBuilder.AppendLine(GetColumnsDefinitionSql(databaseDriver, table, table.Columns));

            // Constraints
            createTableSqlBuilder.AppendLine(
                databaseDriver.GetSqlStatementTemplate(SqlStatement.PrimaryKeyConstraintDefinition).Format(
                    new
                    {
                        tableName = table.Name,
                        columns = table.KeyColumnsNames.Select(databaseDriver.ParseIdentifier).ToCommaSeparate()
                    })
            );

            // Create table 
            var createTableSql = databaseDriver.GetSqlStatementTemplate(SqlStatement.CreateTable).Format(
                new
                {
                    tableName = databaseDriver.ParseIdentifier(table.Name),
                    tableDefinition = createTableSqlBuilder.ToString()
                });

            using (var command = connection.CreateCommand())
            {
                command.CommandText = createTableSql;
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        internal static async Task UpdateTableSchemaAsync(IDatabaseDriver databaseDriver, DbConnection connection, ITable table, CancellationToken cancellationToken)
        {
            var tableColumnsDictionary = new Dictionary<string, string>();

            using (var command = connection.CreateCommand())
            {
                command.CommandText = databaseDriver.GetSqlStatementTemplate(SqlStatement.GetTableColumns).Format(
                    new
                    {
                        tableName = table.Name
                    });

                using (var reader = await command.ExecuteReaderAsync(cancellationToken))
                {
                    while (await reader.ReadAsync(cancellationToken))
                    {
                        tableColumnsDictionary.Add(
                            reader[0].ToString().ToLowerInvariant(), 
                            (string)reader[1]);
                    }
                }
            }

            var columnsToBeCreated = new HashSet<KeyValuePair<string, SqlType>>();

            foreach (var column in table.Columns)
            {
                // Check if the column exists in the database
                var columnKey = column.Key.ToLowerInvariant();
                if (!tableColumnsDictionary.ContainsKey(columnKey))
                {
                    columnsToBeCreated.Add(column);
                }
                // Checks if the existing column type matches with the definition
                // The comparion is with startsWith for the NVARCHAR values
                else if (!GetSqlTypeSql(databaseDriver, column.Value).StartsWith(
                         tableColumnsDictionary[columnKey], StringComparison.OrdinalIgnoreCase))
                {
                    throw new InvalidOperationException($"The existing column '{column.Key}' type '{tableColumnsDictionary[columnKey]}' is not compatible with the definition type '{column.Value}'");
                }
            }

            if (columnsToBeCreated.Any())
            {
                await CreateColumnsAsync(databaseDriver, connection, table, columnsToBeCreated, cancellationToken);
            }
        }

        private static async Task CreateColumnsAsync(IDatabaseDriver databaseDriver, DbConnection connection, ITable table, IEnumerable<KeyValuePair<string, SqlType>> columns, CancellationToken cancellationToken)
        {
            // Create one column each time to improve SQL query compatibility
            foreach (var column in columns)
            {
                var command = connection.CreateCommand();
                command.CommandText = databaseDriver.GetSqlStatementTemplate(
                    SqlStatement.AlterTableAddColumn).Format(
                        new
                        {
                            tableName = databaseDriver.ParseIdentifier(table.Name),
                            columnDefinition = GetColumnsDefinitionSql(databaseDriver, table, new[] { column }).TrimEnd(',')
                        });

                await command.ExecuteNonQueryAsync(cancellationToken);
            }
        }

        private static string GetColumnsDefinitionSql(IDatabaseDriver databaseDriver, ITable table, IEnumerable<KeyValuePair<string, SqlType>> columns)
        {
            var columnSqlBuilder = new StringBuilder();

            foreach (var column in columns)
            {
                var format = new
                {
                    columnName = databaseDriver.ParseIdentifier(column.Key),
                    sqlType = GetSqlTypeSql(databaseDriver, column.Value)
                };

                SqlStatement statement;

                // All columns, except the key are nullable
                if (table.KeyColumnsNames.Contains(column.Key))
                {
                    if (column.Value.IsIdentity)
                    {                        
                        switch (column.Value.Type)
                        {
                            case DbType.Int16:
                                statement = SqlStatement.Int16IdentityColumnDefinition;                                
                                break;
                            case DbType.Int32:
                                statement = SqlStatement.Int32IdentityColumnDefinition;
                                break;
                            case DbType.Int64:
                                statement = SqlStatement.Int64IdentityColumnDefinition;
                                break;
                            default:
                                statement = SqlStatement.IdentityColumnDefinition;
                                break;
                        }
                    }
                    else
                    {
                        statement = SqlStatement.ColumnDefinition;                        
                    }
                }
                else
                {
                    statement = SqlStatement.NullableColumnDefinition;
                }

                columnSqlBuilder.AppendLine(
                    databaseDriver.GetSqlStatementTemplate(statement).Format(
                        format));

                columnSqlBuilder.Append(",");
            }
            return columnSqlBuilder.ToString();
        }

        private static string GetSqlTypeSql(IDatabaseDriver databaseDriver, SqlType sqlType)
        {
            var typeSql = databaseDriver.GetSqlTypeName(sqlType.Type);

            if (sqlType.Length.HasValue)
            {
                var lengthValue = sqlType.Length == int.MaxValue ? 
                    databaseDriver.GetSqlStatementTemplate(SqlStatement.MaxLength) : 
                    sqlType.Length.ToString();
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
    }
}
