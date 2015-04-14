using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Takenet.SimplePersistence.Sql.Mapping;
using static Takenet.SimplePersistence.Sql.SqlHelper;

namespace Takenet.SimplePersistence.Sql
{
    public static class SqlConnectionExtensions
    {
        public static Task<int> ExecuteNonQueryAsync(this SqlConnection connection, string commandText, CancellationToken cancellationToken, SqlParameter[] sqlParameters = null)
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

        public static async Task<TResult> ExecuteScalarAsync<TResult>(this SqlConnection connection, string commandText, CancellationToken cancellationToken, SqlParameter[] sqlParameters = null)
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

        public static SqlCommand CreateTextCommand(this SqlConnection connection, string commandTemplate, object format, IEnumerable<SqlParameter> sqlParameters = null)
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

        public static SqlCommand CreateDeleteCommand(this SqlConnection connection, string tableName, IDictionary<string, object> filterValues)
        {
            return connection.CreateTextCommand(
                SqlTemplates.Delete,
                new
                {
                    tableName = tableName.AsSqlIdentifier(),
                    filter = GetAndEqualsStatement(filterValues.Keys.ToArray())
                },
                filterValues.Select(k => k.ToSqlParameter()));
        }

        public static SqlCommand CreateContainsCommand(this SqlConnection connection, string tableName, IDictionary<string, object> filterValues)
        {
            return connection.CreateTextCommand(
                SqlTemplates.Exists,
                new
                {
                    tableName = tableName.AsSqlIdentifier(),
                    filter = GetAndEqualsStatement(filterValues.Keys.ToArray())
                },
                filterValues.Select(k => k.ToSqlParameter()));
        }

        public static SqlCommand CreateSelectCommand(this SqlConnection connection, string tableName, IDictionary<string, object> filterValues,
            string[] selectColumns)
        {
            return connection.CreateTextCommand(
                SqlTemplates.Select,
                new
                {
                    columns = selectColumns.Select(c => c.AsSqlIdentifier()).ToCommaSepparate(),
                    tableName = tableName.AsSqlIdentifier(),
                    filter = filterValues != null ? GetAndEqualsStatement(filterValues.Keys.ToArray()) : "1 = 1"
                },
                filterValues?.Select(k => k.ToSqlParameter()));
        }

        public static SqlCommand CreateInsertWhereNotExistsCommand(this SqlConnection connection, string tableName,
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
                    columns = columnValues.Keys.Select(c => c.AsSqlIdentifier()).ToCommaSepparate(),
                    values = columnValues.Keys.Select(v => v.AsSqlParameterName()).ToCommaSepparate(),
                    filter = GetAndEqualsStatement(filterValues.Keys.ToArray())
                },
                columnValues.Select(c => c.ToSqlParameter()));
        }
    }
}
