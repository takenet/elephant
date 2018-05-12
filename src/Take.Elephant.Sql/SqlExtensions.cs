using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Sql
{
    public static class SqlExtensions
    {
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
            return databaseDriver.CreateParameter(databaseDriver.ParseParameterName(keyValuePair.Key), keyValuePair.Value);
        }
    }
}
