using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Sql
{
    public class DbConnectionExtensions : IDbConnectionExtensions
    {

        public async Task<TResult> ExecuteScalarAsync<TResult>(
            DbConnection connection,
            string commandText,
            CancellationToken cancellationToken,
            SqlParameter[] sqlParameters = null)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = commandText;
                command.CommandType = System.Data.CommandType.Text;

                if (sqlParameters != null &&
                    sqlParameters.Length > 0)
                {
                    command.Parameters.AddRange(sqlParameters);
                }

                return (TResult)(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false));
            }
        }

        public DbCommand CreateTextCommand(
            DbConnection connection,
            string commandTemplate,
            object format,
            IEnumerable<DbParameter> sqlParameters = null)
        {
            var command = connection.CreateCommand();
            command.CommandText = commandTemplate.Format(format);
            command.CommandType = System.Data.CommandType.Text;

            if (sqlParameters != null)
            {
                foreach (var sqlParameter in sqlParameters)
                {
                    command.Parameters.Add(sqlParameter);
                }
            }

            return command;
        }

        public DbCommand CreateDeleteCommand(
            DbConnection connection,
            IDatabaseDriver databaseDriver,
            ITable table,
            IDictionary<string, object> filterValues)
        {
            if (filterValues == null) throw new ArgumentNullException(nameof(filterValues));
            return CreateTextCommand(connection,
                databaseDriver.GetSqlStatementTemplate(SqlStatement.Delete),
                new
                {
                    schemaName = databaseDriver.ParseIdentifier(table.Schema ?? databaseDriver.DefaultSchema),
                    tableName = databaseDriver.ParseIdentifier(table.Name),
                    filter = SqlHelper.GetAndEqualsStatement(databaseDriver, filterValues)
                },
                filterValues.ToDbParameters(databaseDriver, table));
        }

        public DbCommand CreateUpdateCommand(
            DbConnection connection,
            IDatabaseDriver databaseDriver,
            ITable table,
            IDictionary<string, object> filterValues,
            IDictionary<string, object> columnValues)
        {
            if (filterValues == null) throw new ArgumentNullException(nameof(filterValues));
            if (columnValues == null) throw new ArgumentNullException(nameof(columnValues));
            return CreateTextCommand(connection,
                databaseDriver.GetSqlStatementTemplate(SqlStatement.Update),
                new
                {
                    schemaName = databaseDriver.ParseIdentifier(table.Schema ?? databaseDriver.DefaultSchema),
                    tableName = databaseDriver.ParseIdentifier(table.Name),
                    columnValues = SqlHelper.GetCommaEqualsStatement(databaseDriver, columnValues.Keys.ToArray()),
                    filter = SqlHelper.GetAndEqualsStatement(databaseDriver, filterValues.Keys.ToArray())
                },
                filterValues.ToDbParameters(databaseDriver, table));
        }

        public DbCommand CreateContainsCommand(
            DbConnection connection,
            IDatabaseDriver databaseDriver,
            ITable table,
            IDictionary<string, object> filterValues)
        {
            return CreateTextCommand(connection,
                databaseDriver.GetSqlStatementTemplate(SqlStatement.Exists),
                new
                {
                    schemaName = databaseDriver.ParseIdentifier(table.Schema ?? databaseDriver.DefaultSchema),
                    tableName = databaseDriver.ParseIdentifier(table.Name),
                    filter = SqlHelper.GetAndEqualsStatement(databaseDriver, filterValues)
                },
                filterValues?.ToDbParameters(databaseDriver, table));
        }

        public DbCommand CreateInsertCommand(
            DbConnection connection,
            IDatabaseDriver databaseDriver,
            ITable table,
            IDictionary<string, object> columnValues)
        {
            return CreateTextCommand(connection,
                databaseDriver.GetSqlStatementTemplate(SqlStatement.Insert),
                new
                {
                    schemaName = databaseDriver.ParseIdentifier(table.Schema ?? databaseDriver.DefaultSchema),
                    tableName = databaseDriver.ParseIdentifier(table.Name),
                    columns = columnValues.Keys.Select(databaseDriver.ParseIdentifier).ToCommaSeparate(),
                    values = columnValues.Keys.Select(databaseDriver.ParseParameterName).ToCommaSeparate()
                },
                columnValues.ToDbParameters(databaseDriver, table));
        }

        public DbCommand CreateInsertOutputCommand(
            DbConnection connection,
            IDatabaseDriver databaseDriver,
            ITable table,
            IDictionary<string, object> columnValues,
            string[] outputColumnNames)
        {
            return CreateTextCommand(connection,
                databaseDriver.GetSqlStatementTemplate(SqlStatement.InsertOutput),
                new
                {
                    schemaName = databaseDriver.ParseIdentifier(table.Schema ?? databaseDriver.DefaultSchema),
                    tableName = databaseDriver.ParseIdentifier(table.Name),
                    columns = columnValues.Keys.Select(databaseDriver.ParseIdentifier).ToCommaSeparate(),
                    values = columnValues.Keys.Select(databaseDriver.ParseParameterName).ToCommaSeparate(),
                    outputColumns = outputColumnNames.Select(databaseDriver.ParseIdentifier).ToCommaSeparate()
                },
                columnValues.ToDbParameters(databaseDriver, table));
        }

        public DbCommand CreateInsertWhereNotExistsCommand(
            DbConnection connection,
            IDatabaseDriver databaseDriver,
            ITable table,
            IDictionary<string, object> filterValues,
            IDictionary<string, object> columnValues)
        {
            var sqlTemplate = databaseDriver.GetSqlStatementTemplate(SqlStatement.InsertWhereNotExists);

            return CreateTextCommand(connection,
                sqlTemplate,
                new
                {
                    schemaName = databaseDriver.ParseIdentifier(table.Schema ?? databaseDriver.DefaultSchema),
                    tableName = databaseDriver.ParseIdentifier(table.Name),
                    columns = columnValues.Keys.Select(databaseDriver.ParseIdentifier).ToCommaSeparate(),
                    values = columnValues.Keys.Select(databaseDriver.ParseParameterName).ToCommaSeparate(),
                    filter = filterValues == null || filterValues.Count == 0
                        ? databaseDriver.GetSqlStatementTemplate(SqlStatement.OneEqualsZero)
                        : SqlHelper.GetAndEqualsStatement(databaseDriver, filterValues.Keys.ToArray())
                },
                columnValues.ToDbParameters(databaseDriver, table));
        }

        public DbCommand CreateSelectCommand(
            DbConnection connection,
            IDatabaseDriver databaseDriver,
            ITable table,
            IDictionary<string, object> filterValues,
            string[] selectColumns,
            bool distinct = false)
        {
            if (selectColumns == null) throw new ArgumentNullException(nameof(selectColumns));
            var command = CreateTextCommand(connection,
                databaseDriver.GetSqlStatementTemplate(distinct ? SqlStatement.SelectDistinct : SqlStatement.Select),
                new
                {
                    schemaName = databaseDriver.ParseIdentifier(table.Schema ?? databaseDriver.DefaultSchema),
                    columns = selectColumns.Select(databaseDriver.ParseIdentifier).ToCommaSeparate(),
                    tableName = databaseDriver.ParseIdentifier(table.Name),
                    filter = SqlHelper.GetAndEqualsStatement(databaseDriver, filterValues)
                },
                filterValues?.ToDbParameters(databaseDriver, table));

            AddParameter(command);

            return command;
        }

        public DbCommand CreateSelectCountCommand(
            DbConnection connection,
            IDatabaseDriver databaseDriver,
            ITable table,
            string filter = null,
            IDictionary<string, object> filterValues = null,
            bool distinct = false)
        {
            if (filter == null) filter = databaseDriver.GetSqlStatementTemplate(SqlStatement.OneEqualsOne);
            return CreateTextCommand(connection,
                databaseDriver.GetSqlStatementTemplate(distinct ? SqlStatement.SelectCountDistinct : SqlStatement.SelectCount),
                new
                {
                    schemaName = databaseDriver.ParseIdentifier(table.Schema ?? databaseDriver.DefaultSchema),
                    tableName = databaseDriver.ParseIdentifier(table.Name),
                    filter = filter
                },
                filterValues?.ToDbParameters(databaseDriver, table));
        }

        public DbCommand CreateSelectCountCommand(
            DbConnection connection,
            IDatabaseDriver databaseDriver,
            ITable table,
            IDictionary<string, object> filterValues)
        {
            return CreateTextCommand(connection,
                databaseDriver.GetSqlStatementTemplate(SqlStatement.SelectCount),
                new
                {
                    schemaName = databaseDriver.ParseIdentifier(table.Schema ?? databaseDriver.DefaultSchema),
                    tableName = databaseDriver.ParseIdentifier(table.Name),
                    filter = SqlHelper.GetAndEqualsStatement(databaseDriver, filterValues)
                },
                filterValues?.ToDbParameters(databaseDriver, table));
        }

        public DbCommand CreateSelectSkipTakeCommand(
            DbConnection connection,
            IDatabaseDriver databaseDriver,
            ITable table,
            string[] selectColumns,
            string filter,
            int skip,
            int take,
            string[] orderByColumns,
            bool orderByAscending = true,
            IDictionary<string, object> filterValues = null,
            bool distinct = false)
        {
            var orderBy = orderByColumns.Length > 0
                ? orderByColumns.Select(databaseDriver.ParseIdentifier).ToCommaSeparate()
                : "1";

            if (!orderByAscending)
            {
                orderBy = $"{orderBy} {databaseDriver.GetSqlStatementTemplate(SqlStatement.Desc)}";
            }

            return CreateTextCommand(connection,
                databaseDriver.GetSqlStatementTemplate(distinct
                    ? SqlStatement.SelectDistinctSkipTake
                    : SqlStatement.SelectSkipTake),
                new
                {
                    schemaName = databaseDriver.ParseIdentifier(table.Schema ?? databaseDriver.DefaultSchema),
                    tableName = databaseDriver.ParseIdentifier(table.Name),
                    columns = selectColumns.Select(databaseDriver.ParseIdentifier).ToCommaSeparate(),
                    filter = filter,
                    skip = skip,
                    take = take,
                    orderBy = orderBy
                },
                filterValues?.ToDbParameters(databaseDriver, table));
        }

        public DbCommand CreateSelectTop1Command(
            DbConnection connection,
            IDatabaseDriver databaseDriver,
            ITable table,
            string[] selectColumns,
            IDictionary<string, object> filterValues)
        {
            var command = CreateTextCommand(connection,
                databaseDriver.GetSqlStatementTemplate(SqlStatement.SelectTop1),
                new
                {
                    schemaName = databaseDriver.ParseIdentifier(table.Schema ?? databaseDriver.DefaultSchema),
                    tableName = databaseDriver.ParseIdentifier(table.Name),
                    columns = selectColumns.Select(databaseDriver.ParseIdentifier).ToCommaSeparate(),
                    filter = SqlHelper.GetAndEqualsStatement(databaseDriver, filterValues)
                },
                filterValues?.ToDbParameters(databaseDriver, table));

            AddParameter(command);

            return command;
        }

        private DbCommand AddParameter(DbCommand command)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = "@ExpirableKeySqlMap_ExpirationDate";
            parameter.Value = DateTimeOffset.UtcNow;
            command.Parameters.Add(parameter);

            return command;
        }

        public DbCommand CreateMergeCommand(
            DbConnection connection,
            IDatabaseDriver databaseDriver,
            ITable table,
            IDictionary<string, object> keyValues,
            IDictionary<string, object> columnValues,
            IDictionary<string, object> identityKeyValues = null)
        {
            var keyAndColumnValues = keyValues
                .Union(columnValues)
                .ToDictionary(c => c.Key, c => c.Value);

            IEnumerable<DbParameter> parameters;
            string columnNamesAndValues;

            var columns = keyAndColumnValues.Keys.Select(databaseDriver.ParseIdentifier).ToCommaSeparate();
            string allColumns; // Including identity columns
            var values = keyAndColumnValues.Keys.Select(databaseDriver.ParseParameterName).ToCommaSeparate();
            string allValues; // Including identity columns values

            // If there's identity key values, should be used only for filtering, since it is not possible to insert.
            if (identityKeyValues != null && identityKeyValues.Count > 0)
            {
                keyValues = keyValues.Union(identityKeyValues).ToDictionary(c => c.Key, c => c.Value);
                var allKeyColumnValues = keyAndColumnValues.Union(identityKeyValues).ToDictionary(c => c.Key, c => c.Value);
                parameters = allKeyColumnValues.ToDbParameters(databaseDriver, table);
                columnNamesAndValues =
                    SqlHelper.GetCommaValueAsColumnStatement(databaseDriver, allKeyColumnValues.Keys.ToArray());
                allColumns = allKeyColumnValues.Keys.Select(databaseDriver.ParseIdentifier).ToCommaSeparate();
                allValues = allKeyColumnValues.Keys.Select(databaseDriver.ParseParameterName).ToCommaSeparate();
            }
            else
            {
                parameters = keyAndColumnValues.ToDbParameters(databaseDriver, table);
                columnNamesAndValues =
                    SqlHelper.GetCommaValueAsColumnStatement(databaseDriver, keyAndColumnValues.Keys.ToArray());
                allColumns = columns;
                allValues = values;
            }

            return CreateTextCommand(connection,
                databaseDriver.GetSqlStatementTemplate(SqlStatement.Merge),
                new
                {
                    schemaName = databaseDriver.ParseIdentifier(table.Schema ?? databaseDriver.DefaultSchema),
                    tableName = databaseDriver.ParseIdentifier(table.Name),
                    columnNamesAndValues = columnNamesAndValues,
                    on = SqlHelper.GetLiteralJoinConditionStatement(databaseDriver, keyValues.Keys.ToArray(), "source", "target"),
                    columnValues = columnValues.Any() ? SqlHelper.GetCommaEqualsStatement(databaseDriver, columnValues.Keys.ToArray()) : databaseDriver.GetSqlStatementTemplate(SqlStatement.DummyEqualsZero),
                    columns = columns,
                    allColumns = allColumns,
                    values = values,
                    allValues = allValues,
                    keyColumns = keyValues.Keys.Select(databaseDriver.ParseIdentifier).ToCommaSeparate()
                },
                parameters);
        }

        public DbCommand CreateMergeIncrementCommand(
            DbConnection connection,
            IDatabaseDriver databaseDriver,
            ITable table,
            string incrementColumnName,
            IDictionary<string, object> keyValues,
            IDictionary<string, object> columnValues)
        {
            var keyAndColumnValues = keyValues
                .Union(columnValues)
                .ToDictionary(c => c.Key, c => c.Value);

            return CreateTextCommand(connection,
                databaseDriver.GetSqlStatementTemplate(SqlStatement.MergeIncrement),
                new
                {
                    schemaName = databaseDriver.ParseIdentifier(table.Schema ?? databaseDriver.DefaultSchema),
                    tableName = databaseDriver.ParseIdentifier(table.Name),
                    columnNamesAndValues =
                        SqlHelper.GetCommaValueAsColumnStatement(databaseDriver, keyAndColumnValues.Keys.ToArray()),
                    on = SqlHelper.GetLiteralJoinConditionStatement(databaseDriver, keyValues.Keys.ToArray(), "source",
                        "target"),
                    incrementColumnName = databaseDriver.ParseIdentifier(incrementColumnName),
                    increment = databaseDriver.ParseParameterName(incrementColumnName),
                    columns = keyAndColumnValues.Keys.Select(databaseDriver.ParseIdentifier).ToCommaSeparate(),
                    values = keyAndColumnValues.Keys.Select(databaseDriver.ParseParameterName).ToCommaSeparate(),
                    keyColumns = keyValues.Keys.Select(databaseDriver.ParseIdentifier).ToCommaSeparate()
                },
                keyAndColumnValues.ToDbParameters(databaseDriver, table));
        }
    }
}
