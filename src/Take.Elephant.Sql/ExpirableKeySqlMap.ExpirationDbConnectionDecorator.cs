using System;
using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

[assembly: InternalsVisibleTo("Take.Elephant.Tests")]
namespace Take.Elephant.Sql
{
    public partial class ExpirableKeySqlMap<TKey, TValue>
    {
        /// <summary>
        /// Implements a new behavior for DbConnection with a custom parameter.
        /// </summary>
        internal class ExpirationDbConnectionDecorator : DbConnection
        {
            private readonly DbConnection _dbConnection;
            public ExpirationDbConnectionDecorator(DbConnection dbConnection)
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

            public override async ValueTask DisposeAsync()
            {
                await base.DisposeAsync();
                await _dbConnection.DisposeAsync();
            }

            protected override DbCommand CreateDbCommand()
            {
                var command = _dbConnection.CreateCommand();
                var parameter = command.CreateParameter();
                parameter.ParameterName = ExpirationDatabaseDriverDecorator.EXPIRATION_DATE_PARAMETER_NAME;
                parameter.Value = DateTimeOffset.UtcNow;

                command.Parameters.Add(parameter);
                return command;
            }

            protected override void Dispose(bool disposing)
            {
                base.Dispose(disposing);
                _dbConnection.Dispose();
            }

            protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
            {
                return _dbConnection.BeginTransaction(isolationLevel);
            }
        }
    }
}
