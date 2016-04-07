using System;
using System.Data;
using System.Data.Common;
using Npgsql;

namespace Takenet.Elephant.Sql.PostgreSql
{
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
    }
}
