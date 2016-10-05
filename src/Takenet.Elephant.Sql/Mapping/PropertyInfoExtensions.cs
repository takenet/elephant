using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Takenet.Elephant.Sql.Mapping
{
    public static class PropertyInfoExtensions
    {
        public static IDictionary<string, SqlType> ToSqlColumns(this IEnumerable<PropertyInfo> properties)
        {
            return properties.ToDictionary(p => p.Name, p => new SqlType(DbTypeMapper.GetDbType(p.PropertyType)));
        }
    }
}