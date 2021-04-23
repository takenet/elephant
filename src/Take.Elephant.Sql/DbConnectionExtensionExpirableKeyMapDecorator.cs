using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace Take.Elephant.Sql
{
    public class DbConnectionExtensionExpirableKeyMapDecorator : IDbConnection
    {
        private readonly IDbConnection _dbConnection;

        public DbConnectionExtensionExpirableKeyMapDecorator(IDbConnection dbConnection)
        {
            _dbConnection = dbConnection;
        }

        public string ConnectionString { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public int ConnectionTimeout => throw new NotImplementedException();

        public string Database => throw new NotImplementedException();

        public ConnectionState State => throw new NotImplementedException();

        public IDbTransaction BeginTransaction()
        {
            throw new NotImplementedException();
        }

        public IDbTransaction BeginTransaction(IsolationLevel il)
        {
            throw new NotImplementedException();
        }

        public void ChangeDatabase(string databaseName)
        {
            throw new NotImplementedException();
        }

        public void Close()
        {
            throw new NotImplementedException();
        }

        public IDbCommand CreateCommand()
        {
            var command = _dbConnection.CreateCommand();
            var parameter = command.CreateParameter();
            parameter.ParameterName = "@ExpirableKeySqlMap_ExpirationDate";
            parameter.Value = DateTimeOffset.UtcNow;

            command.Parameters.Add(parameter);
            return command;
        }

        public override void Dispose(bool disposing)
        {
            base.dis
        }

        public void Open()
        {
            throw new NotImplementedException();
        }
    }
}
