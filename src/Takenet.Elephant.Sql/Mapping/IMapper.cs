using System.Collections.Generic;
using System.Data;

namespace Takenet.Elephant.Sql.Mapping
{
    public interface IMapper<TEntity>
    {
        /// <summary>
        /// Gets the database type mapper.
        /// </summary>
        /// <value>
        /// The database type mapper.
        /// </value>
        IDbTypeMapper DbTypeMapper { get; }

        /// <summary>
        /// Gets the column values for an entity.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <param name="columns">The columns to be returned.</param>
        /// <param name="emitDefaultValues">Indicates if default column values should be returned.</param>
        /// <returns></returns>
        IDictionary<string, object> GetColumnValues(TEntity value, string[] columns = null, bool emitDefaultValues = false);

        /// <summary>
        /// Creates an entity for the specified data record.
        /// </summary>
        /// <param name="record"></param>
        /// <param name="columns"></param>
        /// <returns></returns>
        TEntity Create(IDataRecord record, string[] columns);
    }
}