using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;

namespace Takenet.Elephant.Sql
{
    /// <summary>
    /// SQL Server database driver implementation.
    /// </summary>
    public sealed class SqlDatabaseDriver : IDatabaseDriver
    {
        public TimeSpan Timeout => TimeSpan.FromSeconds(180);

        public DbConnection CreateConnection(string connectionString)
        {
            return new SqlConnection(connectionString);
        }

        public string GetSqlStatementTemplate(SqlStatement sqlStatement)
        {
            return SqlTemplates.ResourceManager.GetString(sqlStatement.ToString());
        }

        public string GetSqlTypeName(DbType dbType)
        {
            return SqlTemplates.ResourceManager.GetString($"DbType{dbType}");
        }
    }
}