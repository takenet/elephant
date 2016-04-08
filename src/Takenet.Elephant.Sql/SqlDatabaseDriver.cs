using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using Takenet.Elephant.Sql.Mapping;

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

        public DbParameter CreateParameter(string parameterName, object value) => new SqlParameter(parameterName, value);
        
    }
}