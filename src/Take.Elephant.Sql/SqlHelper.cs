using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace Take.Elephant.Sql
{
    public static class SqlHelper
    {        
        public static string GetAndEqualsStatement(IDatabaseDriver databaseDriver, IDictionary<string, object> filterValues)
        {
            if (filterValues == null) return databaseDriver.GetSqlStatementTemplate(SqlStatement.OneEqualsOne);
            return GetAndEqualsStatement(databaseDriver, filterValues.Keys.ToArray());
        }

        public static string GetAndEqualsStatement(IDatabaseDriver databaseDriver, string[] columns)
        {
            return GetAndEqualsStatement(databaseDriver, columns, columns);
        }

        public static string GetAndEqualsStatement(IDatabaseDriver databaseDriver, string[] columns, string[] parameters)
        {
            return columns.Length == 0 ?
                databaseDriver.GetSqlStatementTemplate(SqlStatement.OneEqualsOne) : 
                GetSeparateEqualsStatement(databaseDriver, databaseDriver.GetSqlStatementTemplate(SqlStatement.And), columns, parameters);
        }

        public static string GetCommaEqualsStatement(IDatabaseDriver databaseDriver, string[] columns)
        {
            return GetCommaEqualsStatement(databaseDriver, columns, columns);
        }

        public static string GetCommaEqualsStatement(IDatabaseDriver databaseDriver, string[] columns, string[] parameters)
        {
            return GetSeparateEqualsStatement(databaseDriver, ",", columns, parameters);
        }

        public static string GetSeparateEqualsStatement(IDatabaseDriver databaseDriver, string separator, string[] columns, string[] parameters)
        {
            return GetSeparateColumnsStatement(databaseDriver, separator, columns, parameters, GetEqualsStatement);
        }

        public static string GetEqualsStatement(IDatabaseDriver databaseDriver, string column, string parameter)
        {
            return databaseDriver.GetSqlStatementTemplate(SqlStatement.QueryEquals).Format(
                new
                {
                    column = databaseDriver.ParseIdentifier(column),
                    value = databaseDriver.ParseParameterName(parameter)
                });
        }

        public static string GetCommaValueAsColumnStatement(IDatabaseDriver databaseDriver, string[] columns)
        {
            return GetValueAsColumnStatement(databaseDriver, ",", columns);
        }

        public static string GetValueAsColumnStatement(IDatabaseDriver databaseDriver, string separator, string[] columns)
        {
            return GetValueAsColumnStatement(databaseDriver, separator, columns, columns);
        }

        public static string GetValueAsColumnStatement(IDatabaseDriver databaseDriver, string separator, string[] columns, string[] parameters)
        {
            return GetSeparateColumnsStatement(databaseDriver, separator, columns, parameters, GetValueAsColumnStatement);
        }

        public static string GetValueAsColumnStatement(IDatabaseDriver databaseDriver, string column, string parameter)
        {
            return databaseDriver.GetSqlStatementTemplate(SqlStatement.ValueAsColumn).Format(
                new
                {
                    value = databaseDriver.ParseParameterName(parameter),
                    column = databaseDriver.ParseIdentifier(column)
                });
        }

        public static string GetLiteralJoinConditionStatement(IDatabaseDriver databaseDriver, string[] columns, string sourceTableName, string targetTableName)
        {
            return GetSeparateColumnsStatement(
                databaseDriver,
                databaseDriver.GetSqlStatementTemplate(SqlStatement.And),
                columns,
                columns,
                (d, c, p) => databaseDriver.GetSqlStatementTemplate(SqlStatement.QueryEquals).Format(
                    new
                    {
                        column = $"{databaseDriver.ParseIdentifier(sourceTableName)}.{databaseDriver.ParseIdentifier(c)}",
                        value = $"{databaseDriver.ParseIdentifier(targetTableName)}.{databaseDriver.ParseIdentifier(c)}",
                    }));

        }

        public static string GetSeparateColumnsStatement(IDatabaseDriver databaseDriver, string separator, string[] columns, string[] parameters, Func<IDatabaseDriver, string, string, string> statement)
        {
            if (columns.Length == 0)
            {
                throw new ArgumentException("The columns are empty", nameof(columns));
            }

            if (parameters.Length == 0)
            {
                throw new ArgumentException("The parameters are empty", nameof(parameters));
            }

            var filter = new StringBuilder();

            for (int i = 0; i < columns.Length; i++)
            {
                var column = columns[i];
                var parameter = parameters[i];

                filter.Append(
                    statement(databaseDriver, column, parameter));

                if (i + 1 < columns.Length)
                {
                    filter.AppendFormat(" {0} ", separator);
                }
            }

            return filter.ToString();
        }

        public static SqlWhereStatement TranslateToSqlWhereClause<TEntity>(
            IDatabaseDriver databaseDriver, 
            Expression<Func<TEntity, bool>> where, 
            IDbTypeMapper dbTypeMapper,
            IDictionary<string, string> parameterReplacementDictionary = null)
        {
            if (where == null)
            {
                return new SqlWhereStatement(databaseDriver.GetSqlStatementTemplate(SqlStatement.OneEqualsOne), new Dictionary<string, object>());
            }
            
            var translator = new SqlExpressionTranslator(databaseDriver, dbTypeMapper, parameterReplacementDictionary);
            return translator.GetStatement(where);
        }
    }
}
