using System;
using System.Data;
using System.Data.Common;
using Takenet.Elephant.Sql.Mapping;

namespace Takenet.Elephant.Sql
{
    public interface IDatabaseDriver
    {
        TimeSpan Timeout { get; }

        DbConnection CreateConnection(string connectionString);

        string GetSqlStatementTemplate(SqlStatement sqlStatement);

        string GetSqlTypeName(DbType dbType);

        DbParameter CreateParameter(string parameterName, object value);

        string ParseParameterName(string parameterName);

        string ParseIdentifier(string identifier);
    }
}
