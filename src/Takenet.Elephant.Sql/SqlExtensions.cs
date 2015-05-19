using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace Takenet.Elephant.Sql
{
    public static class SqlExtensions
    {
        public static string AsSqlParameterName(this string columnName)
        {
            return $"@{columnName}";
        }

        public static string AsSqlIdentifier(this string identifier)
        {
            return $"[{identifier}]";
        }

        /// <summary>
        /// Transform to a flat string with comma separate values.
        /// </summary>
        /// <param name="values"></param>
        /// <returns></returns>
        public static string ToCommaSeparate(this IEnumerable<string> values)
        {
            return values.Aggregate((a, b) => $"{a},{b}").TrimEnd(',');
        }

        public static SqlParameter ToSqlParameter(this KeyValuePair<string, object> keyValuePair)
        {
            return new SqlParameter(keyValuePair.Key.AsSqlParameterName(), keyValuePair.Value);
        }
    }
}
