using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence.Sql
{
    public static class SqlExtensions
    {
        public static string AsSqlParameterName(this string columnName)
        {
            return string.Format("@{0}", columnName);
        }

        public static string AsSqlIdentifier(this string identifier)
        {
            return string.Format("[{0}]", identifier);
        }

        /// <summary>
        /// Transform to a flat string
        /// with comma sepparate values.
        /// </summary>
        /// <param name="values"></param>
        /// <returns></returns>
        public static string ToCommaSepparate(this IEnumerable<string> values)
        {
            return values.Aggregate((a, b) => string.Format("{0},{1}", a, b)).TrimEnd(',');
        }

        public static SqlParameter ToSqlParameter(this KeyValuePair<string, object> keyValuePair)
        {
            return new SqlParameter(keyValuePair.Key.AsSqlParameterName(), keyValuePair.Value);
        }
    }
}
