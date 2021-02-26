using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Take.Elephant.Sql
{
    public class SqlExpressionTranslator : ExpressionVisitor
    {
        public const string PARAMETER_COUNT_SEPARATOR = "$";

        private readonly StringBuilder _filter;
        private readonly IDictionary<string, object> _filterValues;
        private readonly IDatabaseDriver _databaseDriver;
        private readonly IDbTypeMapper _dbTypeMapper;
        private readonly IDictionary<string, string> _parameterReplacementDictionary;
        private readonly string _parameterPrefix;
        private readonly Queue<string> _parameterNames;

        private string _valuePrefix;
        private string _valueSuffix;
        private int _parameterCount;

        public SqlExpressionTranslator(
            IDatabaseDriver databaseDriver,
            IDbTypeMapper dbTypeMapper,
            IDictionary<string, string> parameterReplacementDictionary = null,
            string parameterPrefix = "Param")
        {
            _databaseDriver = databaseDriver;
            _dbTypeMapper = dbTypeMapper;
            _parameterReplacementDictionary = parameterReplacementDictionary;
            _parameterPrefix = parameterPrefix ?? throw new ArgumentNullException(nameof(parameterPrefix));
            _parameterNames = new Queue<string>();

            _filter = new StringBuilder();
            _filterValues = new Dictionary<string, object>();
        }

        public SqlWhereStatement GetStatement(Expression expression)
        {
            Visit(expression);
            return new SqlWhereStatement(_filter.ToString(), _filterValues);
        }

        protected override Expression VisitUnary(UnaryExpression node)
        {
            if (node.NodeType == ExpressionType.Not)
            {
                _filter.AppendFormat(" {0} ", _databaseDriver.GetSqlStatementTemplate(SqlStatement.Not));
            }

            return base.VisitUnary(node);
        }

        protected override Expression VisitBinary(BinaryExpression node)
        {
            _filter.Append("(");
            Visit(node.Left);

            string @operator;

            switch (node.NodeType)
            {
                case ExpressionType.Equal:
                    if (IsNullConstantExpression(node.Right))
                    {
                        @operator = _databaseDriver.GetSqlStatementTemplate(SqlStatement.Is);
                    }
                    else
                    {
                        @operator = _databaseDriver.GetSqlStatementTemplate(SqlStatement.Equal);
                    }
                    break;

                case ExpressionType.NotEqual:
                    if (IsNullConstantExpression(node.Right))
                    {
                        @operator = _databaseDriver.GetSqlStatementTemplate(SqlStatement.IsNot);
                    }
                    else
                    {
                        @operator = _databaseDriver.GetSqlStatementTemplate(SqlStatement.NotEqual);
                    }
                    break;

                case ExpressionType.GreaterThan:
                    @operator = _databaseDriver.GetSqlStatementTemplate(SqlStatement.GreaterThan);
                    break;

                case ExpressionType.GreaterThanOrEqual:
                    @operator = _databaseDriver.GetSqlStatementTemplate(SqlStatement.GreaterThanOrEqual);
                    break;

                case ExpressionType.LessThan:
                    @operator = _databaseDriver.GetSqlStatementTemplate(SqlStatement.LessThan);
                    break;

                case ExpressionType.LessThanOrEqual:
                    @operator = _databaseDriver.GetSqlStatementTemplate(SqlStatement.LessThanOrEqual);
                    break;

                case ExpressionType.And:
                case ExpressionType.AndAlso:
                    @operator = _databaseDriver.GetSqlStatementTemplate(SqlStatement.And);
                    break;

                case ExpressionType.Or:
                case ExpressionType.OrElse:
                    @operator = _databaseDriver.GetSqlStatementTemplate(SqlStatement.Or);
                    break;

                default:
                    throw new NotImplementedException(
                        $"BinaryExpression operator '{node.NodeType}' is not support at this time");
            }

            _filter.AppendFormat(" {0} ", @operator);
            Visit(node.Right);
            _filter.Append(")");

            return node;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression == null) throw new NotSupportedException($"The member '{node.Member.Name}' is not supported");

            object value;
            switch (node.Expression.NodeType)
            {
                case ExpressionType.Parameter:
                    var parameterName = node.Member.Name;
                    // Used for the KeyValuePair expressions, to replace the Key / Value in cases
                    // of non complex types on these properties
                    if (_parameterReplacementDictionary != null &&
                        _parameterReplacementDictionary.ContainsKey(parameterName))
                    {
                        parameterName = _parameterReplacementDictionary[parameterName];
                    }
                    _filter.Append(_databaseDriver.ParseIdentifier(parameterName));
                    _parameterNames.Enqueue(parameterName);
                    return node;

                case ExpressionType.MemberAccess:
                    var memberExpression = (MemberExpression)node.Expression;
                    Expression deepExpression = memberExpression;
                    while (deepExpression is MemberExpression)
                    {
                        deepExpression = ((MemberExpression)deepExpression).Expression;
                    }
                    if (deepExpression is ConstantExpression)
                    {
                        var deepConstantExpression = (ConstantExpression)deepExpression;
                        if (node.Member is PropertyInfo)
                        {
                            if (memberExpression.Member is FieldInfo)
                            {
                                var fieldInfoValue =
                                    ((FieldInfo)memberExpression.Member).GetValue(deepConstantExpression.Value);
                                value = ((PropertyInfo)node.Member).GetValue(fieldInfoValue, null);
                                _filter.Append(AddFilterValue(value, node.Type));
                                return node;
                            }

                            if (memberExpression.Member is PropertyInfo)
                            {
                                var objectMember = Expression.Convert(node, typeof(object));
                                var getterLambda = Expression.Lambda<Func<object>>(objectMember);
                                var getter = getterLambda.Compile();
                                value = getter();
                                _filter.Append(AddFilterValue(value, node.Type));
                                return node;
                            }
                        }
                        else
                        {
                            throw new NotSupportedException();
                        }
                    }

                    _filter.Append(_databaseDriver.ParseIdentifier(node.Member.Name));
                    return node;

                case ExpressionType.Constant:
                    var constantExpression = (ConstantExpression)node.Expression;
                    var member = node.Member;
                    value = null;
                    if (member is FieldInfo)
                    {
                        value = ((FieldInfo)member).GetValue(constantExpression.Value);
                    }
                    if (member is PropertyInfo)
                    {
                        value = ((PropertyInfo)member).GetValue(constantExpression.Value, null);
                    }

                    _filter.Append(AddFilterValue(value, node.Type));
                    return node;
            }

            throw new NotSupportedException($"The expression member '{node.Member.Name}' with node type '{node.Expression.NodeType}' is not supported");
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            _filter.Append(AddFilterValue(node.Value, node.Type));
            return node;
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.Name.Equals(nameof(object.Equals)))
            {
                Visit(Expression.Equal(node.Object, node.Arguments[0]));
                return node;
            }

            if (node.Method.Name.Equals(nameof(object.ToString)))
            {
                VisitMember((MemberExpression)node.Object);
                return node;
            }

            if (node.Method.Name.Equals(nameof(string.Contains)))
            {
                if (node.Method.IsStatic && node.Method.DeclaringType.Name.Equals(nameof(Enumerable)))
                {
                    var values = (((ConstantExpression)node.Arguments[0]).Value as IEnumerable).Cast<object>();
                    var expression = node.Arguments[1];

                    _filter.Append("(");
                    Visit(expression);
                    _filter.AppendFormat(" {0} (", _databaseDriver.GetSqlStatementTemplate(SqlStatement.In));
                    if (values == null || !values.Any())
                    {
                        _filter.Append(" null ");
                    }
                    else
                    {
                        foreach (var value in values)
                        {
                            VisitConstant(Expression.Constant(value));
                            _filter.Append(",");
                        }
                        _filter.Remove(_filter.Length - 1, 1);
                    }
                    _filter.Append("))");
                    return node;
                }

                _valuePrefix = "%";
                _valueSuffix = "%";
                return GenerateSqlLike(node);
            }

            if (node.Method.Name.Equals(nameof(string.StartsWith)))
            {
                _valuePrefix = null;
                _valueSuffix = "%";

                return GenerateSqlLike(node);
            }

            if (node.Method.Name.Equals(nameof(string.EndsWith)))
            {
                _valuePrefix = "%";
                _valueSuffix = null;
                return GenerateSqlLike(node);
            }

            throw new NotImplementedException(
                $"Translation not implemented for method {node.Method.Name} of type {node.Method.DeclaringType}");
        }

        private static bool IsNullConstantExpression(Expression expression) => expression is ConstantExpression constantExpression && constantExpression.Value == null;

        private Expression GenerateSqlLike(MethodCallExpression node)
        {
            var argument = node.Arguments[0];
            _filter.Append("(");
            Visit(node.Object);
            _filter.Append($" {_databaseDriver.GetSqlStatementTemplate(SqlStatement.Like)} ");
            Visit(argument);
            _filter.Append(")");
            _valuePrefix = _valueSuffix = null;
            return node;
        }

        private string AddFilterValue(object value, Type type)
        {
            if (value == null)
            {
                return _databaseDriver.GetSqlStatementTemplate(SqlStatement.Null);
            }

            var name = $"{_parameterPrefix}{_parameterCount}";
            if (_parameterNames.Any())
            {
                name = _parameterNames.Dequeue();
                if (_filterValues.ContainsKey(name))
                {
                    name = $"{name}{PARAMETER_COUNT_SEPARATOR}{_parameterCount}";
                }
            }

            _parameterCount++;

            var parameterName = _databaseDriver.ParseParameterName(name);
            var dbType = DbTypeMapper.GetDbType(type);
            if ((dbType == DbType.String || dbType == DbType.StringFixedLength) &&
                (_valuePrefix != null || _valueSuffix != null) &&
                value is string valueString)
            {
                var valueWithPrefixBuilder = new StringBuilder();
                if (_valuePrefix != null)
                {
                    valueWithPrefixBuilder.Append(_valuePrefix);
                }

                valueWithPrefixBuilder.Append(valueString);
                if (_valueSuffix != null)
                {
                    valueWithPrefixBuilder.Append(_valueSuffix);
                }

                value = valueWithPrefixBuilder.ToString();
            }

            var dbValue = _dbTypeMapper.ToDbType(value, dbType);
            _filterValues.Add(name, dbValue);
            return parameterName;
        }
    }

    public sealed class SqlWhereStatement
    {
        public SqlWhereStatement(string @where, IDictionary<string, object> filterValues)
        {
            Where = @where;
            FilterValues = filterValues;
        }

        public string Where { get; }

        public IDictionary<string, object> FilterValues { get; }
    }
}