using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence.Sql.Mapping
{
    /// <summary>
    /// Allows to extend a <see cref="ITableMapper{TEntity}"/> with additional columns.
    /// </summary>
    /// <typeparam name="TExtension"></typeparam>
    /// <typeparam name="TEntity"></typeparam>

    public interface IExtendedTableMapper<TExtension, TEntity> : ITableMapper<TEntity>
    {
        /// <summary>
        /// Gets the names of
        /// the extension columns.
        /// </summary>
        IEnumerable<string> ExtensionColumns { get; }

        /// <summary>
        /// Gets the extension column values.
        /// </summary>
        /// <param name="extension"></param>
        /// <param name="columns"></param>
        /// <param name="returnNullValues"></param>
        /// <returns></returns>
        IDictionary<string, object> GetExtensionColumnValues(TExtension extension, string[] columns = null, bool returnNullValues = false);

        /// <summary>
        /// Creates an extension for the specified data record.
        /// </summary>
        /// <param name="record"></param>
        /// <param name="columns"></param>
        /// <returns></returns>
        TExtension CreateExtension(IDataRecord record, string[] columns);
    }
}
