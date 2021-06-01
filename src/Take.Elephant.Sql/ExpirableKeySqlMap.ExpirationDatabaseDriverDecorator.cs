using System;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Take.Elephant.Sql.Mapping;

[assembly: InternalsVisibleTo("Take.Elephant.Tests")]
namespace Take.Elephant.Sql
{
    public partial class ExpirableKeySqlMap<TKey, TValue>
    {
        /// <summary>
        /// Injects a SQL statement to avoid retrieving expired items.
        /// </summary>
        /// <seealso cref="Take.Elephant.Sql.IDatabaseDriver" />
        internal class ExpirationDatabaseDriverDecorator : IDatabaseDriver
        {
            private readonly IDatabaseDriver _underlyingDatabaseDriver;
            private readonly string _expirationColumnName;
            public const string EXPIRATION_DATE_PARAMETER_NAME = "@ExpirableKeySqlMap_ExpirationDate";

            public ExpirationDatabaseDriverDecorator(IDatabaseDriver underlyingDatabaseDriver, string expirationColumnName)
            {
                _underlyingDatabaseDriver = underlyingDatabaseDriver ?? throw new ArgumentNullException(nameof(underlyingDatabaseDriver));
                _expirationColumnName = underlyingDatabaseDriver.ParseIdentifier(expirationColumnName ?? throw new ArgumentNullException(nameof(expirationColumnName)));
            }

            public DbConnection CreateConnection(string connectionString)
                => new ExpirationDbConnectionDecorator(_underlyingDatabaseDriver.CreateConnection(connectionString));

            public string GetSqlStatementTemplate(SqlStatement sqlStatement)
            {
                var sql = _underlyingDatabaseDriver.GetSqlStatementTemplate(sqlStatement);
                switch (sqlStatement)
                {
                    case SqlStatement.Select:
                    case SqlStatement.SelectCount:
                    case SqlStatement.SelectTop1:
                    case SqlStatement.SelectSkipTake:
                    case SqlStatement.SelectDistinct:
                    case SqlStatement.SelectCountDistinct:
                    case SqlStatement.SelectDistinctSkipTake:
                    case SqlStatement.Exists:
                        var expirationFilter = $"AND ({_expirationColumnName} IS NULL OR {_expirationColumnName} > {EXPIRATION_DATE_PARAMETER_NAME})";
                        sql = InjectSqlFilter(sql, expirationFilter);
                        break;
                }
                return sql;
            }

            public string GetSqlTypeName(DbType dbType) => _underlyingDatabaseDriver.GetSqlTypeName(dbType);

            public DbParameter CreateParameter(string parameterName, object value) => _underlyingDatabaseDriver.CreateParameter(parameterName, value);

            public DbParameter CreateParameter(string parameterName, object value, SqlType sqlType) => _underlyingDatabaseDriver.CreateParameter(parameterName, value, sqlType);

            public string ParseParameterName(string parameterName) => _underlyingDatabaseDriver.ParseParameterName(parameterName);

            public string ParseIdentifier(string identifier) => _underlyingDatabaseDriver.ParseIdentifier(identifier);

            public TimeSpan Timeout => _underlyingDatabaseDriver.Timeout;

            public string DefaultSchema => _underlyingDatabaseDriver.DefaultSchema;

            private static string InjectSqlFilter(string sql, string filter)
            {
                if (sql.Contains("ORDER BY"))
                    return sql.Replace("ORDER BY", $"{filter} ORDER BY");
                if (sql.StartsWith("SELECT CASE WHEN EXISTS", StringComparison.InvariantCultureIgnoreCase))
                {
                    var regex = new Regex(@"(SELECT CASE WHEN EXISTS \(\()(.*?)(\))(.*?)(END)");
                    var matches = regex.Matches(sql);
                    if (matches.Any())
                    {
                        var captureGroups = matches.First().Groups;
                        // first group is always the entire string, so we can skip that
                        return $"{captureGroups[1]}{captureGroups[2]} {filter}{captureGroups[3]}{captureGroups[4]}{captureGroups[5]}";
                    }
                }
                return $"{sql} {filter}";
            }
        }
    }
}
