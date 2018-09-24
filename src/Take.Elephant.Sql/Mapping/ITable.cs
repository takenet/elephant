using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Sql.Mapping
{
    /// <summary>
    /// Provides scheme information for a SQL table.
    /// </summary>
    public interface ITable
    {
        /// <summary>
        /// Gets the table schema.
        /// </summary>
        string Schema { get; }

        /// <summary>
        /// Gets the table name.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Gets the names of the table key columns.
        /// </summary>
        string[] KeyColumnsNames { get; }

        /// <summary>
        /// Gets the columns of the table with its types.
        /// The key columns must be included.
        /// </summary>
        IDictionary<string, SqlType> Columns { get; }

        /// <summary>
        /// Defines if the table schema has been synchronized with the database.
        /// </summary>
        bool SchemaSynchronized { get;  }

        /// <summary>
        /// Synchronize the table schema in the database using the specified database.
        /// The synchronization can occurs only once per table instance.
        /// </summary>
        /// <param name="connectionString">The connection string for the execution of the synchronization SQL DDL commands.</param>
        /// <param name="databaseDriver">The database driver.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns></returns>
        Task SynchronizeSchemaAsync(string connectionString, IDatabaseDriver databaseDriver,  CancellationToken cancellationToken);

        /// <summary>
        /// Occurs when the table schema is changed in the database.
        /// </summary>
        event EventHandler<DbCommandEventArgs> SchemaChanged;
    }
}
