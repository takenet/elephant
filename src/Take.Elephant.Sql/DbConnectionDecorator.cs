using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;

namespace Take.Elephant.Sql
{
    public class DbConnectionDecorator : DbConnection
    {
        private readonly DbConnection _dbConnection;
        public DbConnectionDecorator(DbConnection dbConnection)
        {
            _dbConnection = dbConnection;
        }

        public override string ConnectionString { get => _dbConnection.ConnectionString; set => _dbConnection.ConnectionString = value; }

        public override string Database => _dbConnection.Database;

        public override string DataSource => _dbConnection.DataSource;

        public override string ServerVersion => _dbConnection.ServerVersion;

        public override ConnectionState State => _dbConnection.State;

        public override void ChangeDatabase(string databaseName)
        {
            _dbConnection.ChangeDatabase(databaseName);
        }

        public override void Close()
        {
            _dbConnection.Close();
        }

        public override void Open()
        {
            _dbConnection.Open();
        }

        protected override DbCommand CreateDbCommand()
        {
            var command = _dbConnection.CreateCommand();
            var parameter = command.CreateParameter();
            parameter.ParameterName = "@ExpirableKeySqlMap_ExpirationDate";
            parameter.Value = DateTimeOffset.UtcNow;

            command.Parameters.Add(parameter);
            return command;
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            throw new NotImplementedException();
        }
    }
}
