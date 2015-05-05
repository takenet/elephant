using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence.Sql
{
    internal static class SqlHelper
    {
        internal static string GetAndEqualsStatement(string[] columns)
        {
            return GetSeparateEqualsStatement(SqlTemplates.And, columns);
        }

        internal static string GetAndEqualsStatement(string[] columns, string[] parameters)
        {
            return GetSeparateEqualsStatement(SqlTemplates.And, columns, parameters);
        }

        internal static string GetCommaEqualsStatement(string[] columns)
        {
            return GetSeparateEqualsStatement(",", columns);
        }

        internal static string GetCommaEqualsStatement(string[] columns, string[] parameters)
        {
            return GetSeparateEqualsStatement(",", columns, parameters);
        }

        internal static string GetSeparateEqualsStatement(string separator, string[] columns)
        {
            return GetSeparateEqualsStatement(separator, columns, columns);
        }

        internal static string GetSeparateEqualsStatement(string separator, string[] columns, string[] parameters)
        {
            if (columns.Length == 0)
            {
                throw new ArgumentException("The columns are empty");
            }

            if (parameters.Length == 0)
            {
                throw new ArgumentException("The parameters are empty");
            }

            var filter = new StringBuilder();

            for (int i = 0; i < columns.Length; i++)
            {
                var column = columns[i];
                var parameter = parameters[i];

                filter.Append(
                    GetEqualsStatement(column, parameter));

                if (i + 1 < columns.Length)
                {
                    filter.AppendFormat(" {0} ", separator);
                }
            }

            return filter.ToString();
        }

        internal static string GetEqualsStatement(string column)
        {
            return GetEqualsStatement(column, column);
        }

        internal static string GetEqualsStatement(string column, string parameter)
        {
            return SqlTemplates.QueryEquals.Format(
                new
                {
                    column = column.AsSqlIdentifier(),
                    value = parameter.AsSqlParameterName()
                });
        }

        internal static string TranslateToSqlWhereClause<TEntity>(Expression<Func<TEntity, bool>> where)
        {
            var translator = new SqlExpressionTranslator();
            return translator.GetStatement(where);
        }
    }
}
