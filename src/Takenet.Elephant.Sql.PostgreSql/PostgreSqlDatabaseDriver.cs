using System;
using System.Data;
using System.Data.Common;
using Npgsql;
using Takenet.Elephant.Sql.Mapping;

namespace Takenet.Elephant.Sql.PostgreSql
{
    /// <summary>
    /// Implementation of the PostgreSQL database driver.
    /// <see cref="http://www.sqlines.com/sql-server-to-postgresql"/>
    /// <see cref="http://www.postgresql.org/docs/9.4/static/index.html"/>
    /// </summary>
    public class PostgreSqlDatabaseDriver : IDatabaseDriver
    {
        public TimeSpan Timeout => TimeSpan.FromSeconds(180);

        public DbConnection CreateConnection(string connectionString)
        {
            return new NpgsqlConnection(connectionString);
        }

        public string GetSqlStatementTemplate(SqlStatement sqlStatement)
        {
            return PostgreSqlTemplates.ResourceManager.GetString(sqlStatement.ToString());
        }

        public string GetSqlTypeName(DbType dbType)
        {
            return PostgreSqlTemplates.ResourceManager.GetString($"DbType{dbType}");
        }

        public DbParameter CreateParameter(string parameterName, object value)
        {
            return new NpgsqlParameter()
            {
                ParameterName = parameterName,
                Value = value
            };
        }
        
    }
}
