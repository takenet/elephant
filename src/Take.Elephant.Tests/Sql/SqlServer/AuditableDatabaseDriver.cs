using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Take.Elephant.Sql;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Tests.Sql.SqlServer
{
    /// <summary>
    /// Database driver that allow auditing received operations.
    /// </summary>
    public class AuditableDatabaseDriver : IDatabaseDriver
    {
        private readonly IDatabaseDriver _databaseDriver;

        public AuditableDatabaseDriver(IDatabaseDriver databaseDriver)
        {
            _databaseDriver = databaseDriver ?? throw new ArgumentNullException(nameof(databaseDriver));
            ReceivedConnectionStrings = new List<string>();
        }

        public List<string> ReceivedConnectionStrings { get; }
        
        public string DefaultSchema => _databaseDriver.DefaultSchema;

        public TimeSpan Timeout => _databaseDriver.Timeout;
        
        public DbConnection CreateConnection(string connectionString)
        {
            ReceivedConnectionStrings.Add(connectionString);
            return _databaseDriver.CreateConnection(connectionString);
        }

        public string GetSqlStatementTemplate(SqlStatement sqlStatement) => _databaseDriver.GetSqlStatementTemplate(sqlStatement);

        public string GetSqlTypeName(DbType dbType) => _databaseDriver.GetSqlTypeName(dbType);

        public DbParameter CreateParameter(string parameterName, object value) => _databaseDriver.CreateParameter(parameterName, value);

        public DbParameter CreateParameter(string parameterName, object value, SqlType sqlType) => _databaseDriver.CreateParameter(parameterName, value, sqlType);

        public string ParseParameterName(string parameterName) => _databaseDriver.ParseParameterName(parameterName);

        public string ParseIdentifier(string identifier) => _databaseDriver.ParseIdentifier(identifier);
    }
}