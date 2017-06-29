using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Takenet.Elephant.Sql.Mapping
{
    public class Table : ITable
    {
        private readonly SemaphoreSlim _schemaSynchronizedSemaphore;


        public Table(string name, string[] keyColumnsNames, IDictionary<string, SqlType> columns, string schema = null)
        {
            if (keyColumnsNames == null) throw new ArgumentNullException(nameof(keyColumnsNames));
            var repeatedKeyColumn = keyColumnsNames.GroupBy(k => k).FirstOrDefault(c => c.Count() > 1);
            if (repeatedKeyColumn != null) throw new ArgumentException($"The key column named '{repeatedKeyColumn.Key}' appears more than once", nameof(columns));
            if (columns == null) throw new ArgumentNullException(nameof(columns));
            if (columns.Count == 0) throw new ArgumentException(@"The table must define at least one column", nameof(columns));
            var repeatedColumn = columns.GroupBy(c => c.Key).FirstOrDefault(c => c.Count() > 1);
            if (repeatedColumn != null) throw new ArgumentException($"The column named '{repeatedColumn.Key}' appears more than once", nameof(columns));
            Name = name ?? throw new ArgumentNullException(nameof(name));
            KeyColumnsNames = keyColumnsNames;
            Columns = columns;
            Schema = schema;
            _schemaSynchronizedSemaphore = new SemaphoreSlim(1);
        }

        public string Schema { get; }

        public string Name { get; }

        public string[] KeyColumnsNames { get; }

        public IDictionary<string, SqlType> Columns { get; }

        public bool SchemaSynchronized { get; private set; }

        public async Task SynchronizeSchemaAsync(string connectionString, IDatabaseDriver databaseDriver, CancellationToken cancellationToken)
        {
            if (!SchemaSynchronized)
            {
                await _schemaSynchronizedSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

                try
                {
                    if (!SchemaSynchronized)
                    {
                        if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));
                        if (databaseDriver == null) throw new ArgumentNullException(nameof(databaseDriver));

                        using (var connection = databaseDriver.CreateConnection(connectionString))
                        {
                            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

                            var tableExistsSql = databaseDriver.GetSqlStatementTemplate(SqlStatement.TableExists).Format(
                                new
                                {
                                    // Do not parse the identifiers here.
                                    schemaName = Schema ?? databaseDriver.DefaultSchema,
                                    tableName = Name
                                });

                            // Check if the table exists
                            var tableExists = await connection.ExecuteScalarAsync<bool>(
                                tableExistsSql,
                                cancellationToken).ConfigureAwait(false);

                            if (!tableExists)
                            {
                                await DatabaseSchema.CreateTableAsync(databaseDriver, connection, this, cancellationToken).ConfigureAwait(false);
                            }

                            await DatabaseSchema.UpdateTableSchemaAsync(databaseDriver, connection, this, cancellationToken).ConfigureAwait(false);
                            SchemaSynchronized = true;
                        }
                    }
                }
                finally
                {
                    _schemaSynchronizedSemaphore.Release();
                }
            }
        }
    }
}