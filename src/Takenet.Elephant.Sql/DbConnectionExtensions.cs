using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Takenet.Elephant.Sql
{
    public static class DbConnectionExtensions
    {
        public static Task<int> ExecuteNonQueryAsync(this DbConnection connection, string commandText, CancellationToken cancellationToken, SqlParameter[] sqlParameters = null)
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

                return command.ExecuteNonQueryAsync(cancellationToken);
            }
        }

        public static async Task<TResult> ExecuteScalarAsync<TResult>(this DbConnection connection, string commandText, CancellationToken cancellationToken, SqlParameter[] sqlParameters = null)
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

        public static DbCommand CreateTextCommand(this DbConnection connection, string commandTemplate, object format, IEnumerable<DbParameter> sqlParameters = null)
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

        public static DbCommand CreateDeleteCommand(this DbConnection connection, IDatabaseDriver databaseDriver, string tableName, IDictionary<string, object> filterValues)
        {
            if (filterValues == null) throw new ArgumentNullException(nameof(filterValues));
            return connection.CreateTextCommand(
                databaseDriver.GetSqlStatementTemplate(SqlStatement.Delete),
                new
                {
                    tableName = databaseDriver.ParseIdentifier(tableName),
                    filter = SqlHelper.GetAndEqualsStatement(databaseDriver, filterValues)
                },
                filterValues.Select(k => k.ToDbParameter(databaseDriver)));
        }

        public static DbCommand CreateUpdateCommand(this DbConnection connection, IDatabaseDriver databaseDriver, string tableName, IDictionary<string, object> filterValues, IDictionary<string, object> columnValues)
        {
            if (filterValues == null) throw new ArgumentNullException(nameof(filterValues));
            if (columnValues == null) throw new ArgumentNullException(nameof(columnValues));
            return connection.CreateTextCommand(
                databaseDriver.GetSqlStatementTemplate(SqlStatement.Update),                
                new
                {
                    tableName = databaseDriver.ParseIdentifier(tableName),
                    columnValues = SqlHelper.GetCommaEqualsStatement(databaseDriver, columnValues.Keys.ToArray()),
                    filter = SqlHelper.GetAndEqualsStatement(databaseDriver, filterValues.Keys.ToArray())
                },
                filterValues.Union(columnValues).Select(c => c.ToDbParameter(databaseDriver)));
        }

        public static DbCommand CreateContainsCommand(this DbConnection connection, IDatabaseDriver databaseDriver, string tableName, IDictionary<string, object> filterValues)
        {
            return connection.CreateTextCommand(
                databaseDriver.GetSqlStatementTemplate(SqlStatement.Exists),
                new
                {
                    tableName = databaseDriver.ParseIdentifier(tableName),
                    filter = SqlHelper.GetAndEqualsStatement(databaseDriver, filterValues)
                },
                filterValues?.Select(k => k.ToDbParameter(databaseDriver)));
        }        

        public static DbCommand CreateInsertCommand(this DbConnection connection, IDatabaseDriver databaseDriver, string tableName, IDictionary<string, object> columnValues)
        {
            return connection.CreateTextCommand(
                databaseDriver.GetSqlStatementTemplate(SqlStatement.Insert),
                new
                {
                    tableName = databaseDriver.ParseIdentifier(tableName),
                    columns = columnValues.Keys.Select(databaseDriver.ParseIdentifier).ToCommaSeparate(),
                    values = columnValues.Keys.Select(databaseDriver.ParseParameterName).ToCommaSeparate()
                },
                columnValues.Select(c => c.ToDbParameter(databaseDriver)));
        }

        public static DbCommand CreateInsertWhereNotExistsCommand(this DbConnection connection, IDatabaseDriver databaseDriver, string tableName, IDictionary<string, object> filterValues, IDictionary<string, object> columnValues)
        {
            var sqlTemplate = databaseDriver.GetSqlStatementTemplate(SqlStatement.InsertWhereNotExists);

            return connection.CreateTextCommand(
                sqlTemplate,
                new
                {
                    tableName = databaseDriver.ParseIdentifier(tableName),
                    columns = columnValues.Keys.Select(databaseDriver.ParseIdentifier).ToCommaSeparate(),
                    values = columnValues.Keys.Select(databaseDriver.ParseParameterName).ToCommaSeparate(),
                    filter = filterValues == null || !filterValues.Any() ? databaseDriver.GetSqlStatementTemplate(SqlStatement.OneEqualsZero) : SqlHelper.GetAndEqualsStatement(databaseDriver, filterValues.Keys.ToArray())
                },
                columnValues.Select(c => c.ToDbParameter(databaseDriver)));
        }

        public static DbCommand CreateSelectCommand(this DbConnection connection, IDatabaseDriver databaseDriver, string tableName, IDictionary<string, object> filterValues, string[] selectColumns)
        {
            if (selectColumns == null) throw new ArgumentNullException(nameof(selectColumns));
            return connection.CreateTextCommand(
                databaseDriver.GetSqlStatementTemplate(SqlStatement.Select),
                new
                {
                    columns = selectColumns.Select(databaseDriver.ParseIdentifier).ToCommaSeparate(),
                    tableName = databaseDriver.ParseIdentifier(tableName),
                    filter = SqlHelper.GetAndEqualsStatement(databaseDriver, filterValues)
                },
                filterValues?.Select(k => k.ToDbParameter(databaseDriver)));
        }

        public static DbCommand CreateSelectCountCommand(this DbConnection connection, IDatabaseDriver databaseDriver, string tableName, string filter = null, IDictionary<string, object> filterValues = null)
        {
            if (filter == null) filter = databaseDriver.GetSqlStatementTemplate(SqlStatement.OneEqualsOne);
            return connection.CreateTextCommand(
                databaseDriver.GetSqlStatementTemplate(SqlStatement.SelectCount),
                new
                {
                    tableName = databaseDriver.ParseIdentifier(tableName),
                    filter = filter
                },
                filterValues?.Select(k => k.ToDbParameter(databaseDriver)));
        }

        public static DbCommand CreateSelectCountCommand(this DbConnection connection, IDatabaseDriver databaseDriver, string tableName, IDictionary<string, object> filterValues)
        {
            return connection.CreateTextCommand(
                databaseDriver.GetSqlStatementTemplate(SqlStatement.SelectCount),
                new
                {
                    tableName = databaseDriver.ParseIdentifier(tableName),
                    filter = SqlHelper.GetAndEqualsStatement(databaseDriver, filterValues)
                },
                filterValues?.Select(k => k.ToDbParameter(databaseDriver)));
        }

        public static DbCommand CreateSelectSkipTakeCommand(this DbConnection connection, IDatabaseDriver databaseDriver, string tableName, string[] selectColumns, string filter, int skip, int take, string[] orderByColumns, bool orderByAscending = true, IDictionary<string, object> filterValues = null)
        {
            var orderBy = orderByColumns.Select(databaseDriver.ParseIdentifier).ToCommaSeparate();
            if (!orderByAscending)
            {
                orderBy = $"{orderBy} {databaseDriver.GetSqlStatementTemplate(SqlStatement.Desc)}";
            }

            return connection.CreateTextCommand(
                databaseDriver.GetSqlStatementTemplate(SqlStatement.SelectSkipTake),
                new
                {
                    columns = selectColumns.Select(databaseDriver.ParseIdentifier).ToCommaSeparate(),
                    tableName = databaseDriver.ParseIdentifier(tableName),
                    filter = filter,
                    skip = skip,
                    take = take,
                    orderBy = orderBy
                },
                filterValues?.Select(k => k.ToDbParameter(databaseDriver)));
        }

        public static DbCommand CreateSelectTop1Command(this DbConnection connection, IDatabaseDriver databaseDriver, string tableName, string[] selectColumns, IDictionary<string, object> filterValues)
        {
            return connection.CreateTextCommand(
                databaseDriver.GetSqlStatementTemplate(SqlStatement.SelectTop1),
                new
                {
                    tableName = databaseDriver.ParseIdentifier(tableName),
                    columns = selectColumns.Select(databaseDriver.ParseIdentifier).ToCommaSeparate(),
                    filter = SqlHelper.GetAndEqualsStatement(databaseDriver, filterValues)
                },
                filterValues?.Select(k => k.ToDbParameter(databaseDriver)));
        }

        public static DbCommand CreateMergeCommand(
            this DbConnection connection,
            IDatabaseDriver databaseDriver,
            string tableName,
            IDictionary<string, object> keyValues,
            IDictionary<string, object> columnValues)
        {
            var keyAndColumnValues = keyValues
                .Union(columnValues)
                .ToDictionary(c => c.Key, c => c.Value);

            return connection.CreateTextCommand(
                databaseDriver.GetSqlStatementTemplate(SqlStatement.Merge),
                new
                {
                    tableName = databaseDriver.ParseIdentifier(tableName),
                    columnNamesAndValues = SqlHelper.GetCommaValueAsColumnStatement(databaseDriver, keyAndColumnValues.Keys.ToArray()),
                    on = SqlHelper.GetLiteralJoinConditionStatement(databaseDriver, keyValues.Keys.ToArray(), "source", "target"),
                    columnValues = columnValues.Any() ? SqlHelper.GetCommaEqualsStatement(databaseDriver, columnValues.Keys.ToArray()) : databaseDriver.GetSqlStatementTemplate(SqlStatement.DummyEqualsZero),
                    columns = keyAndColumnValues.Keys.Select(databaseDriver.ParseIdentifier).ToCommaSeparate(),
                    values = keyAndColumnValues.Keys.Select(databaseDriver.ParseParameterName).ToCommaSeparate(),
                    keyColumns = keyValues.Keys.Select(databaseDriver.ParseIdentifier).ToCommaSeparate()
                },
                keyAndColumnValues.Select(k => k.ToDbParameter(databaseDriver)));
        }

        public static DbCommand CreateMergeIncrementCommand(
            this DbConnection connection,
            IDatabaseDriver databaseDriver,
            string tableName,
            string incrementColumnName,
            IDictionary<string, object> keyValues,
            IDictionary<string, object> columnValues)
        {
            var keyAndColumnValues = keyValues
                .Union(columnValues)
                .ToDictionary(c => c.Key, c => c.Value);

            return connection.CreateTextCommand(
                databaseDriver.GetSqlStatementTemplate(SqlStatement.MergeIncrement),
                new
                {
                    tableName = databaseDriver.ParseIdentifier(tableName),
                    columnNamesAndValues = SqlHelper.GetCommaValueAsColumnStatement(databaseDriver, keyAndColumnValues.Keys.ToArray()),
                    on = SqlHelper.GetLiteralJoinConditionStatement(databaseDriver, keyValues.Keys.ToArray(), "source", "target"),
                    incrementColumnName = databaseDriver.ParseIdentifier(incrementColumnName),
                    increment = databaseDriver.ParseParameterName(incrementColumnName),
                    columns = keyAndColumnValues.Keys.Select(databaseDriver.ParseIdentifier).ToCommaSeparate(),
                    values = keyAndColumnValues.Keys.Select(databaseDriver.ParseParameterName).ToCommaSeparate(),
                    keyColumns = keyValues.Keys.Select(databaseDriver.ParseIdentifier).ToCommaSeparate()
                },
                keyAndColumnValues.Select(k => k.ToDbParameter(databaseDriver)));
        }
    }
}
