using System.Collections.Generic;
using System.Data;
using System.Data.Common;
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

        public static DbParameter ToDbParameter(this KeyValuePair<string, object> keyValuePair, IDatabaseDriver databaseDriver, SqlType sqlType)
        {
            return databaseDriver.CreateParameter(databaseDriver.ParseParameterName(keyValuePair.Key), keyValuePair.Value, sqlType);
        }

        public static DbParameter ToDbParameter(
            this KeyValuePair<string, object> keyValuePair,
            IDatabaseDriver databaseDriver,
            IDictionary<string, SqlType> columnTypes)
        {
            SqlType sqlType;
            if (columnTypes.TryGetValue(keyValuePair.Key, out sqlType))
            {
                return keyValuePair.ToDbParameter(databaseDriver, sqlType);
            }

            // Queries with multiples parameters for same column add separator between the parameter name and the parameter number.
            // Try to get the sqlType for parameter spliting the key on the parameter separator
            var key = keyValuePair.Key.Split(SqlExpressionTranslator.PARAMETER_COUNT_SEPARATOR)[0];

            return columnTypes.TryGetValue(key, out sqlType)
                ? keyValuePair.ToDbParameter(databaseDriver, sqlType)
                : keyValuePair.ToDbParameter(databaseDriver);
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