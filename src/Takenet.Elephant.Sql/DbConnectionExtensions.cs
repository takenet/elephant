using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Takenet.Elephant.Sql
{
    internal static class DbConnectionExtensions
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

        public static DbCommand CreateTextCommand(this DbConnection connection, string commandTemplate, object format, IEnumerable<SqlParameter> sqlParameters = null)
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

        public static DbCommand CreateDeleteCommand(this DbConnection connection, string tableName, IDictionary<string, object> filterValues)
        {
            if (filterValues == null) throw new ArgumentNullException(nameof(filterValues));
            return connection.CreateTextCommand(
                SqlTemplates.Delete,
                new
                {
                    tableName = tableName.AsSqlIdentifier(),
                    filter = SqlHelper.GetAndEqualsStatement(filterValues)
                },
                filterValues.Select(k => k.ToSqlParameter()));
        }

        public static DbCommand CreateUpdateCommand(this DbConnection connection, string tableName, IDictionary<string, object> filterValues, IDictionary<string, object> columnValues)
        {
            if (filterValues == null) throw new ArgumentNullException(nameof(filterValues));
            if (columnValues == null) throw new ArgumentNullException(nameof(columnValues));
            return connection.CreateTextCommand(
                SqlTemplates.Update,
                new
                {
                    tableName = tableName.AsSqlIdentifier(),
                    columnValues = SqlHelper.GetCommaEqualsStatement(columnValues.Keys.ToArray()),
                    filter = SqlHelper.GetAndEqualsStatement(filterValues.Keys.ToArray())
                },
                filterValues.Union(columnValues).Select(c => c.ToSqlParameter()));
        }

        public static DbCommand CreateContainsCommand(this DbConnection connection, string tableName, IDictionary<string, object> filterValues)
        {
            return connection.CreateTextCommand(
                SqlTemplates.Exists,
                new
                {
                    tableName = tableName.AsSqlIdentifier(),
                    filter = SqlHelper.GetAndEqualsStatement(filterValues)
                },
                filterValues?.Select(k => k.ToSqlParameter()));
        }        

        public static DbCommand CreateInsertCommand(this DbConnection connection, string tableName, IDictionary<string, object> columnValues)
        {
            return connection.CreateTextCommand(
                SqlTemplates.Insert,
                new
                {
                    tableName = tableName.AsSqlIdentifier(),
                    columns = columnValues.Keys.Select(c => c.AsSqlIdentifier()).ToCommaSeparate(),
                    values = columnValues.Keys.Select(v => v.AsSqlParameterName()).ToCommaSeparate()
                },
                columnValues.Select(c => c.ToSqlParameter()));
        }

        public static DbCommand CreateInsertWhereNotExistsCommand(this DbConnection connection, string tableName,
            IDictionary<string, object> filterValues, IDictionary<string, object> columnValues, bool deleteBeforeInsert = false)
        {
            var sqlTemplate = deleteBeforeInsert ?
                SqlTemplates.DeleteAndInsertWhereNotExists :
                SqlTemplates.InsertWhereNotExists;

            return connection.CreateTextCommand(
                sqlTemplate,
                new
                {
                    tableName = tableName.AsSqlIdentifier(),
                    columns = columnValues.Keys.Select(c => c.AsSqlIdentifier()).ToCommaSeparate(),
                    values = columnValues.Keys.Select(v => v.AsSqlParameterName()).ToCommaSeparate(),
                    filter = filterValues == null || !filterValues.Any() ? SqlTemplates.OneEqualsZero : SqlHelper.GetAndEqualsStatement(filterValues.Keys.ToArray())
                },
                columnValues.Select(c => c.ToSqlParameter()));
        }

        public static DbCommand CreateSelectCommand(this DbConnection connection, string tableName, IDictionary<string, object> filterValues, string[] selectColumns)
        {
            if (selectColumns == null) throw new ArgumentNullException(nameof(selectColumns));
            return connection.CreateTextCommand(
                SqlTemplates.Select,
                new
                {
                    columns = selectColumns.Select(c => c.AsSqlIdentifier()).ToCommaSeparate(),
                    tableName = tableName.AsSqlIdentifier(),
                    filter = SqlHelper.GetAndEqualsStatement(filterValues)
                },
                filterValues?.Select(k => k.ToSqlParameter()));
        }

        public static DbCommand CreateSelectCountCommand(this DbConnection connection, string tableName, string filter)
        {
            return connection.CreateTextCommand(
                SqlTemplates.SelectCount,
                new
                {
                    tableName = tableName.AsSqlIdentifier(),
                    filter = filter
                });
        }

        public static DbCommand CreateSelectSkipTakeCommand(this DbConnection connection, string tableName, string[] selectColumns,
            string filter, int skip, int take, string[] orderByColumns)
        {
            return connection.CreateTextCommand(
                SqlTemplates.SelectSkipTake,
                new
                {
                    columns = selectColumns.Select(c => c.AsSqlIdentifier()).ToCommaSeparate(),
                    tableName = tableName.AsSqlIdentifier(),
                    filter = filter,
                    skip = skip,
                    take = take,
                    orderBy = orderByColumns.Select(c => c.AsSqlIdentifier()).ToCommaSeparate()
                });
        }

        public static DbCommand CreateSelectTop1Command(this DbConnection connection, string tableName, string[] selectColumns, IDictionary<string, object> filterValues)
        {
            return connection.CreateTextCommand(
                SqlTemplates.SelectTop1,
                new
                {
                    tableName = tableName.AsSqlIdentifier(),
                    columns = selectColumns.Select(c => c.AsSqlIdentifier()).ToCommaSeparate(),
                    filter = SqlHelper.GetAndEqualsStatement(filterValues)
                },
                filterValues?.Select(k => k.ToSqlParameter()));
        }

        public static DbCommand CreateMergeCommand(this DbConnection connection, string tableName, IDictionary<string, object> keyValues, IDictionary<string, object> columnValues)
        {
            var keyAndColumnValues = keyValues
                .Union(columnValues)
                .ToDictionary(c => c.Key, c => c.Value);

            return connection.CreateTextCommand(
                SqlTemplates.Merge,
                new
                {
                    tableName = tableName.AsSqlIdentifier(),
                    columnNamesAndValues = SqlHelper.GetCommaValueAsColumnStatement(keyAndColumnValues.Keys.ToArray()),
                    on = SqlHelper.GetLiteralJoinConditionStatement(keyValues.Keys.ToArray(), "source", "target"),
                    columnValues = columnValues.Any() ? SqlHelper.GetCommaEqualsStatement(columnValues.Keys.ToArray()) : SqlTemplates.DummyEqualsZero,
                    columns = keyAndColumnValues.Keys.Select(c => c.AsSqlIdentifier()).ToCommaSeparate(),
                    values = keyAndColumnValues.Keys.Select(v => v.AsSqlParameterName()).ToCommaSeparate()
                },
                keyAndColumnValues.Select(k => k.ToSqlParameter()));
        }
    }
}
