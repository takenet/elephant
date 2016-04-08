using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using Takenet.Elephant.Sql.Mapping;

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
            return $"\"{identifier}\"";
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

        public static DbParameter ToDbParameter(this KeyValuePair<string, object> keyValuePair, IDatabaseDriver databaseDriver)
        {
            return databaseDriver.CreateParameter(keyValuePair.Key.AsSqlParameterName(), keyValuePair.Value);
        }
    }
}
