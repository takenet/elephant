using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Sql.Mapping
{
    /// <inheritdoc />        
    public class Table : ITable
    {
        private readonly SchemaSynchronizationStrategy _synchronizationStrategy;
        private readonly SemaphoreSlim _schemaSynchronizedSemaphore;
        private int _synchronizationsTries;
        
        public Table(
            string name,
            string[] keyColumnsNames,
            IDictionary<string, SqlType> columns,
            string schema = null,
            SchemaSynchronizationStrategy synchronizationStrategy = SchemaSynchronizationStrategy.UntilSuccess)
        {
            if (keyColumnsNames == null) throw new ArgumentNullException(nameof(keyColumnsNames));
            _synchronizationStrategy = synchronizationStrategy;
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

        public event EventHandler<DbCommandEventArgs> SchemaChanged;

        internal void RaiseSchemaChanged(DbCommand dbCommand)
        {
            SchemaChanged?.Invoke(this, new DbCommandEventArgs(dbCommand));
        }

        /// <inheritdoc />        
        public virtual async Task SynchronizeSchemaAsync(string connectionString, IDatabaseDriver databaseDriver, CancellationToken cancellationToken)
        {
            if (_synchronizationStrategy == SchemaSynchronizationStrategy.Ignore) return;
            
            if (ShouldSynchronizeSchema)
            {
                await _schemaSynchronizedSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

                try
                {
                    if (ShouldSynchronizeSchema)
                    {
                        if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));
                        if (databaseDriver == null) throw new ArgumentNullException(nameof(databaseDriver));

                        _synchronizationsTries++;
                        
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

        private bool ShouldSynchronizeSchema =>
            !SchemaSynchronized &&
            _synchronizationStrategy != SchemaSynchronizationStrategy.Ignore &&
            (_synchronizationStrategy == SchemaSynchronizationStrategy.UntilSuccess ||
             (_synchronizationStrategy == SchemaSynchronizationStrategy.TryOnce && _synchronizationsTries == 0));
    }

    public enum SchemaSynchronizationStrategy
    {
        /// <summary>
        /// Try to execute the schema synchronization until it succeeds.
        /// </summary>
        UntilSuccess,
        
        /// <summary>
        /// Try to execute the schema synchronization only one time. 
        /// </summary>
        TryOnce,
        
        /// <summary>
        /// Do not try to synchronize the table schema.
        /// </summary>
        Ignore
    }
}