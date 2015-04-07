using System.Collections.Generic;
using System.Data;

namespace Takenet.SimplePersistence.Sql.Mapping
{
    public interface IMapper<TEntity>
    {
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