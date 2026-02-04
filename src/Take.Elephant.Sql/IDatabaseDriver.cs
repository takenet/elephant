using System;
using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Sql
{
    public interface IDatabaseDriver
    {
        string DefaultSchema { get; }

        TimeSpan Timeout { get; }

        DbConnection CreateConnection(string connectionString, SqlRetryLogicOption retryOptions = null);

        string GetSqlStatementTemplate(SqlStatement sqlStatement);

        string GetSqlTypeName(DbType dbType);

        DbParameter CreateParameter(string parameterName, object value);

        DbParameter CreateParameter(string parameterName, object value, SqlType sqlType);

        string ParseParameterName(string parameterName);

        string ParseIdentifier(string identifier);
    }
}
