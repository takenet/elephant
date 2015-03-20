using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence.Sql.Mapping
{
    /// <summary>
    /// Defines the mapping of an entity to a table.
    /// </summary>
    /// <typeparam name="TEntity"></typeparam>
    public interface ITableMapper<TEntity>
    {
        /// <summary>
        /// Gets the table name.
        /// </summary>
        string TableName { get; }

        /// <summary>
        /// Gets the names of
        /// the table key columns.
        /// </summary>
        IEnumerable<string> KeyColumns { get; }

        /// <summary>
        /// Gets the columns of the table with its types.
        /// The key columns must be included.
        /// </summary>
        IDictionary<string, SqlType> Columns { get; }

        /// <summary>
        /// Gets the column values for an entity.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <param name="columns">The columns to be returned.</param>
        /// <param name="returnNullValues">indicates if null column values should be returned.</param>
        /// <returns></returns>
        IDictionary<string, object> GetColumnValues(TEntity value, string[] columns = null, bool returnNullValues = false);

        /// <summary>
        /// Creates an entity for the specified data record.
        /// </summary>
        /// <param name="record"></param>
        /// <param name="columns"></param>
        /// <returns></returns>
        TEntity Create(IDataRecord record, string[] columns);
    }
}
