using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Takenet.Elephant.Sql.Mapping
{
    /// <inheritdoc />        
    public class Table : ITable
    {
        private readonly SemaphoreSlim _schemaSynchronizedSemaphore;

        /// <summary>
        /// Creates a new <see cref="Table"/> instance.
        /// </summary>
        /// <param name="name">The table anem</param>
        /// <param name="keyColumnsNames"></param>
        /// <param name="columns"></param>
        /// <param name="schema"></param>
        /// <param name="schemaSynchronized">Indicates if the table schema is synchronized.</param>
        public Table(string name, string[] keyColumnsNames, IDictionary<string, SqlType> columns, string schema = null, bool schemaSynchronized = false)
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
            SchemaSynchronized = schemaSynchronized;
            _schemaSynchronizedSemaphore = new SemaphoreSlim(1);
        }

        /// <inheritdoc />        
        public string Schema { get; }

        /// <inheritdoc />        
        public string Name { get; }

        /// <inheritdoc />        
        public string[] KeyColumnsNames { get; }

        /// <inheritdoc />        
        public IDictionary<string, SqlType> Columns { get; }

        /// <inheritdoc />        
        public bool SchemaSynchronized { get; private set; }

        /// <inheritdoc />        
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