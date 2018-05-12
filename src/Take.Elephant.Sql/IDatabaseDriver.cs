using System;
using System.Data;
using System.Data.Common;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Sql
{
    public interface IDatabaseDriver
    {
        string DefaultSchema { get; }

        TimeSpan Timeout { get; }

        DbConnection CreateConnection(string connectionString);

        string GetSqlStatementTemplate(SqlStatement sqlStatement);

        string GetSqlTypeName(DbType dbType);

        DbParameter CreateParameter(string parameterName, object value);

        string ParseParameterName(string parameterName);

        string ParseIdentifier(string identifier);
    }
}
