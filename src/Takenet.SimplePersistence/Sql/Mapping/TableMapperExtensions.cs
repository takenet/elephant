using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence.Sql.Mapping
{
    public static class TableMapperExtensions
    {
        //public static IDictionary<string, object> GetKeyColumnValues<TEntity>(this ITable<TEntity> table, TEntity entity)
        //{
        //    return table.GetColumnValues(entity, table.KeyColumns.ToArray());
        //}

        //public static IDictionary<string, object> GetColumnValues<TExtension, TEntity>(this IExtendedTableMapper<TExtension, TEntity> extendedTableMapper, TExtension extension, TEntity entity)
        //{
        //    var columnValues = extendedTableMapper.GetColumnValues(entity);
        //    var extensionColumnValues = extendedTableMapper.GetExtensionColumnValues(extension);

        //    return columnValues
        //        .Concat(extensionColumnValues)
        //        .ToDictionary(k => k.Key, v => v.Value);
        //}

        //public static IDictionary<string, object> GetKeyColumnValues<TExtension, TEntity>(this IExtendedTableMapper<TExtension, TEntity> extendedTableMapper, TExtension extension, TEntity entity)
        //{
        //    var keyColumns = extendedTableMapper.KeyColumns.ToArray();

        //    var keyColumnValues = extendedTableMapper.GetColumnValues(entity, columns: keyColumns);
        //    var extensionKeyColumnValues = extendedTableMapper.GetExtensionColumnValues(extension, columns: keyColumns);

        //    var concatKeyColumnValues = keyColumnValues
        //        .Concat(extensionKeyColumnValues)
        //        .ToDictionary(k => k.Key, v => v.Value);

        //    return keyColumns
        //        .ToDictionary(c => c, c => concatKeyColumnValues[c]);
        //}

        //public static IDictionary<string, object> GetExtensionKeyColumnValues<TExtension, TEntity>(this IExtendedTableMapper<TExtension, TEntity> extendedTableMapper, TExtension extension)
        //{
        //    var keyColumns = extendedTableMapper
        //        .KeyColumns
        //        .Where(c => extendedTableMapper.ExtensionColumns.Contains(c))
        //        .ToArray();

        //    var extensionKeyColumnValues = extendedTableMapper
        //        .GetExtensionColumnValues(extension, columns: keyColumns);

        //    return keyColumns
        //        .ToDictionary(c => c, c => extensionKeyColumnValues[c]);
        //}
    }
}
