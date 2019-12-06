using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Sql
{
    /// <summary>
    /// SQL Server database driver implementation.
    /// </summary>
    public sealed class SqlDatabaseDriver : IDatabaseDriver
    {
        public TimeSpan Timeout => TimeSpan.FromSeconds(180);

        public string DefaultSchema => "dbo";

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

        public DbParameter CreateParameter(string parameterName, object value, SqlType sqlType)
        {
            if (!TryGetSqlDbType(sqlType.Type, out var sqlDbType) || sqlType.Length == null)
            {
                return CreateParameter(parameterName, value);
            }
            
            return new SqlParameter(parameterName, sqlDbType.Value, sqlType.Length.Value)
            {
                Value = value
            };
        }

        public string ParseParameterName(string parameterName) => $"@{parameterName}";

        public string ParseIdentifier(string identifier) => $"[{identifier}]";

        private static bool TryGetSqlDbType(DbType dbType, out SqlDbType? type)
        {
            // Use SqlParameter class to convert a DbType to SqlDbType
            SqlParameter sqlParameter = new SqlParameter();
            try
            {
                sqlParameter.DbType = dbType;
            }
            catch (Exception)
            {
                type = null;
                return false;
            }

            type = sqlParameter.SqlDbType;
            return true;
        }
    }
}