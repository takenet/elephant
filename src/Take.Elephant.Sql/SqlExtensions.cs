using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text.RegularExpressions;
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

        public static DbParameter ToDbParameter(this KeyValuePair<string, object> keyValuePair, IDatabaseDriver databaseDriver, SqlType sqlType)
        {
            return databaseDriver.CreateParameter(databaseDriver.ParseParameterName(keyValuePair.Key), keyValuePair.Value, sqlType);
        }

        public static DbParameter ToDbParameter(
            this KeyValuePair<string, object> keyValuePair,
            IDatabaseDriver databaseDriver,
            IDictionary<string, SqlType> columnTypes)
        {
            if (columnTypes.TryGetValue(keyValuePair.Key, out var sqlType))
            {
                return keyValuePair.ToDbParameter(databaseDriver, sqlType);
            }
            else
            {
                // Queries with multiples parameters for same column add a number to the end of parameter key.
                // Try to get the sqlType for parameter removing the digits at the end of parameter key
                var key = Regex.Replace(keyValuePair.Key, @"\d*$", string.Empty);

                return columnTypes.TryGetValue(key, out var sqlType2)
                    ? keyValuePair.ToDbParameter(databaseDriver, sqlType2)
                    : keyValuePair.ToDbParameter(databaseDriver);
            }
        }

        public static IEnumerable<DbParameter> ToDbParameters(
            this IDictionary<string, object> parameters,
            IDatabaseDriver databaseDriver,
            ITable table)
        {
            return parameters.Select(p => p.ToDbParameter(databaseDriver, table.Columns));
        }
    }
}