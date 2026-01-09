using System.Collections.Generic;
using System.Data;

namespace Take.Elephant.Sql.Mapping
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
        /// <param name="emitNullValues">Indicates if null column values should be returned.</param>
        /// <param name="includeIdentityTypes"></param>
        /// <returns></returns>
        IDictionary<string, object> GetColumnValues(TEntity value, string[] columns = null, bool emitNullValues = false, bool includeIdentityTypes = false);

        /// <summary>
        /// Creates an entity for the specified data record.
        /// </summary>
        /// <param name="record"></param>
        /// <param name="columns"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        TEntity Create(IDataRecord record, string[] columns, TEntity value = default(TEntity));
    }
}