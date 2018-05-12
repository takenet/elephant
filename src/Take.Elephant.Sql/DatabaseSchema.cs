using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Sql
{
    internal static class DatabaseSchema
    {
        internal static async Task CreateTableAsync(IDatabaseDriver databaseDriver, DbConnection connection, ITable table, CancellationToken cancellationToken)
        {
            if (table.Columns.Count == 0)
            {
                throw new InvalidOperationException("The table mapper has no defined columns");
            }

            // Schema
            var schemaName = table.Schema ?? databaseDriver.DefaultSchema;
            using (var command = connection.CreateCommand())
            {
                command.CommandText = databaseDriver.GetSqlStatementTemplate(SqlStatement.CreateSchemaIfNotExists).Format(
                    new
                    {
                        schemaName = schemaName
                    });
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            // Table columns
            var createTableSqlBuilder = new StringBuilder();
            createTableSqlBuilder.AppendLine(GetColumnsDefinitionSql(databaseDriver, table, table.Columns));

            // Constraints
            if (table.KeyColumnsNames.Length > 0)
            {
                createTableSqlBuilder.AppendLine(
                    databaseDriver
                        .GetSqlStatementTemplate(SqlStatement.PrimaryKeyConstraintDefinition)
                        .Format(
                            new
                            {
                                schemaName = schemaName,
                                tableName = table.Name,
                                columns = table.KeyColumnsNames.Select(databaseDriver.ParseIdentifier).ToCommaSeparate()
                            })
                );
            }

            // Create table 
            var createTableSql = databaseDriver.GetSqlStatementTemplate(SqlStatement.CreateTable).Format(
                new
                {
                    schemaName = databaseDriver.ParseIdentifier(schemaName),
                    tableName = databaseDriver.ParseIdentifier(table.Name),
                    tableDefinition = createTableSqlBuilder.ToString().TrimEnd(',', '\n', '\r')
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
                        // Do not parse the identifiers here.
                        schemaName = table.Schema ?? databaseDriver.DefaultSchema,
                        tableName = table.Name
                    });

                using (var reader = await command.ExecuteReaderAsync(cancellationToken))
                {
                    while (await reader.ReadAsync(cancellationToken))
                    {
                        var columnType = (string)reader[1];

                        if (reader.FieldCount == 3 && 
                            reader[2] != DBNull.Value)
                        {
                            var columnLength = (int)reader[2];
                            if (columnLength == -1)
                            {
                                columnType = $"{columnType}({databaseDriver.GetSqlStatementTemplate(SqlStatement.MaxLength)})";
                            }
                            else
                            {
                                columnType = $"{columnType}({columnLength})";
                            }
                        }

                        tableColumnsDictionary.Add(
                            reader[0].ToString().ToLowerInvariant(),
                            columnType);
                    }
                }
            }

            var columnsToBeCreated = new HashSet<KeyValuePair<string, SqlType>>();
            var columnsToBeAltered = new HashSet<KeyValuePair<string, SqlType>>();

            foreach (var column in table.Columns)
            {
                // Check if the column exists in the database
                var columnKey = column.Key.ToLowerInvariant();
                if (!tableColumnsDictionary.ContainsKey(columnKey))
                {
                    columnsToBeCreated.Add(column);
                }
                // Checks if the existing column type matches with the definition
                else if (!GetSqlTypeSql(databaseDriver, column.Value).Equals(
                         tableColumnsDictionary[columnKey], StringComparison.OrdinalIgnoreCase))
                {
                    columnsToBeAltered.Add(column);
                }
            }

            if (columnsToBeCreated.Any())
            {
                await AddColumnsAsync(databaseDriver, connection, table, columnsToBeCreated, cancellationToken);
            }

            if (columnsToBeAltered.Any())
            {
                await AlterColumnsAsync(databaseDriver, connection, table, columnsToBeAltered, cancellationToken);
            }
        }

        private static Task AddColumnsAsync(IDatabaseDriver databaseDriver, DbConnection connection, ITable table, 
            IEnumerable<KeyValuePair<string, SqlType>> columns, CancellationToken cancellationToken) =>
                AlterTableColumnsAsync(databaseDriver, connection, table, columns, SqlStatement.AlterTableAddColumn,
                    cancellationToken);

        private static Task AlterColumnsAsync(IDatabaseDriver databaseDriver, DbConnection connection, ITable table,
            IEnumerable<KeyValuePair<string, SqlType>> columns, CancellationToken cancellationToken) =>
                AlterTableColumnsAsync(databaseDriver, connection, table, columns, SqlStatement.AlterTableAlterColumn,
                    cancellationToken);

        private static async Task AlterTableColumnsAsync(IDatabaseDriver databaseDriver, DbConnection connection, ITable table, IEnumerable<KeyValuePair<string, SqlType>> columns, SqlStatement sqlStatement, CancellationToken cancellationToken)
        {
            foreach (var column in columns)
            {
                var command = connection.CreateCommand();
                command.CommandText = databaseDriver.GetSqlStatementTemplate(
                    sqlStatement).Format(
                        new
                        {
                            schemaName = databaseDriver.ParseIdentifier(table.Schema ?? databaseDriver.DefaultSchema),
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
