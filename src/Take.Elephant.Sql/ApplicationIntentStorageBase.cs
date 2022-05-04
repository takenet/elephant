using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Sql
{
    public class ApplicationIntentStorageBase
    {
        public ApplicationIntentStorageBase(IDatabaseDriver databaseDriver, string connectionString, ITable table)
        {
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new ArgumentException("The connection string cannot be null or empty", nameof(connectionString));
            }

            if (connectionString.Contains("ApplicationIntent", StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException("'ApplicationIntent' should not be provided in the connection string",
                    nameof(connectionString));
            }

            DatabaseDriver = databaseDriver ?? throw new ArgumentNullException(nameof(databaseDriver));
            ConnectionString = connectionString;
            Table = table ?? throw new ArgumentNullException(nameof(table));

            // Ignore schema synchronization in the readonly connection. It should only occurs in the writable one.
            ReadOnlyTable = new Table(table.Name, table.KeyColumnsNames, table.Columns, table.Schema,
                SchemaSynchronizationStrategy.Ignore);
            ReadOnlyConnectionString = $"{connectionString.TrimEnd(';')};ApplicationIntent=ReadOnly";
        }

        public ITable Table { get; }

        public string ConnectionString { get; }

        public ITable ReadOnlyTable { get; }

        public string ReadOnlyConnectionString { get; }

        public IDatabaseDriver DatabaseDriver { get; }

        protected async Task<DbConnection> GetConnectionAsync(CancellationToken cancellationToken) =>
            await GetConnectionAsync(ConnectionString, cancellationToken);

        protected async Task<DbConnection> GetReadOnlyConnectionAsync(CancellationToken cancellationToken) =>
            await GetConnectionAsync(ReadOnlyConnectionString, cancellationToken);

        private async Task<DbConnection> GetConnectionAsync(string connectionString,
            CancellationToken cancellationToken)
        {
            // Schema synchronization must always occur in the "writable" connection
            await SynchronizeSchemaAsync(cancellationToken);
            var connection = DatabaseDriver.CreateConnection(connectionString);
            await connection.OpenAsync(cancellationToken);
            return connection;
        }

        protected Task SynchronizeSchemaAsync(CancellationToken cancellationToken) =>
            Table.SynchronizeSchemaAsync(ConnectionString, DatabaseDriver, cancellationToken);
    }
}