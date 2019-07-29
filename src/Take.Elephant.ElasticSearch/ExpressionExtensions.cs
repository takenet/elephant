using Nest;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace Take.Elephant.Elasticsearch
{
    /// <summary>
    /// Expression extension methods
    /// </summary>
    public static class ExpressionExtensions
    {
        /// <summary>
        /// Converts a Expression tree to a Elasticsearch Query DSL
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="expression"></param>
        /// <returns></returns>
        public static QueryContainer ParseToQueryContainer<T>(this Expression expression) where T : class
        {
            switch (expression.NodeType)
            {
                case ExpressionType.AndAlso:
                    return expression.AndExpression<T>();

                case ExpressionType.And:
                    return expression.AndExpression<T>();

                case ExpressionType.Or:
                    return expression.OrExpression<T>();

                case ExpressionType.OrElse:
                    return expression.OrExpression<T>();

                case ExpressionType.Equal:
                    return expression.ComparationExpression<T>();

                case ExpressionType.NotEqual:
                    return !expression.MatchValuesExpression<T>();

                case ExpressionType.Lambda:
                    var l = expression as LambdaExpression;
                    return l.Body.ParseToQueryContainer<T>();

                case ExpressionType.Not:
                    var a = expression as UnaryExpression;
                    return !a.Operand.ParseToQueryContainer<T>();

                case ExpressionType.Call:
                    return expression.CallExpression<T>();
            }

            throw new NotImplementedException($"{expression.GetType().ToString()} {expression.NodeType.ToString()}");
        }

        private static QueryContainer ComparationExpression<T>(this Expression expression) where T : class
        {
            var equal = expression as BinaryExpression;
            switch (equal.Left.NodeType)
            {
                case ExpressionType.MemberAccess:

                    if (equal.Right.NodeType == ExpressionType.Constant)
                    {
                        return equal.MatchValuesExpression<T>();
                    }
                    else if (equal.Right.NodeType == ExpressionType.Convert && equal.Right.GetValue() == null)
                    {
                        return !expression.FieldExistsExpression<T>();
                    }

                    break;

                case ExpressionType.Call:

                    var value = equal.Right.GetValue();
                    if (value.GetType() == typeof(bool))
                    {
                        return (bool)value ? equal.Left.CallExpression<T>() :
                            !equal.Left.CallExpression<T>();
                    }
                    else
                    {
                        return equal.MatchValuesExpression<T>();
                    }
            }

            throw new NotImplementedException($"{expression.GetType().ToString()} {expression.NodeType.ToString()}");
        }

        private static QueryContainer AndExpression<T>(this Expression expression) where T : class
        {
            var and = expression as BinaryExpression;
            return and.Left.ParseToQueryContainer<T>() && and.Right.ParseToQueryContainer<T>();
        }

        private static QueryContainer OrExpression<T>(this Expression expression) where T : class
        {
            var orExpression = expression as BinaryExpression;
            return orExpression.Left.ParseToQueryContainer<T>() || orExpression.Right.ParseToQueryContainer<T>();
        }

        private static QueryContainer CallExpression<T>(this Expression expression) where T : class
        {
            var callExpression = expression as MethodCallExpression;

            switch (callExpression.Method.Name)
            {
                case "Contains":
                    var wildcardField = callExpression.Object.GetField<T>();
                    var wildCardValue = callExpression.Arguments[0].GetValue();

                    return new QueryContainerDescriptor<T>()
                        .Wildcard(w => w.Field(wildcardField)
                        .Value($"*{wildCardValue.ToString().ToLowerInvariant()}*"));
            }

            throw new NotImplementedException($"{expression.GetType().ToString()} {expression.NodeType.ToString()}");
        }

        private static QueryContainer MatchValuesExpression<T>(this Expression expression)
            where T : class
        {
            var matchExpression = expression as BinaryExpression;
            var value = matchExpression.Right.GetValue();
            var field = matchExpression.Left.GetField<T>();

            if (value == null)
            {
                return !expression.FieldExistsExpression<T>();
            }

            return new QueryContainerDescriptor<T>()
                .MatchPhrase(w => w.Field(field).Query(value.ToString()));
        }

        private static QueryContainer FieldExistsExpression<T>(this Expression expression) where T : class
        {
            var matchExpression = expression as BinaryExpression;
            var field = matchExpression.Left.GetField<T>();

            return new QueryContainerDescriptor<T>()
                .Exists(f => f.Field(field));
        }

        private static string GetField<T>(this Expression expression) where T : class
        {
            if (expression.NodeType == ExpressionType.Call)
            {
                var callExpression = expression as MethodCallExpression;
                if (callExpression.Method.Name == "ToString")
                {
                    return callExpression.Object.GetField<T>();
                }
                throw new InvalidOperationException();
            }

            if (expression.NodeType == ExpressionType.Convert)
            {
                var unaryExpression = expression as UnaryExpression;
                return unaryExpression.Operand.GetField<T>();

            }

            return ((MemberExpression)expression).Member.Name.GetPropertyDataMemberName<T>();
        }

        private static object GetValue(this Expression expression)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.Constant:
                    return ((ConstantExpression)expression).Value;

                case ExpressionType.Convert:
                    return ((UnaryExpression)expression).Operand.GetValue();

                default:
                    throw new InvalidOperationException();

            }

        }
    }
}
