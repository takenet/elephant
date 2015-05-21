using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Takenet.Elephant.Sql
{
    internal class SqlExpressionTranslator : ExpressionVisitor
    {
        private readonly StringBuilder _filter = new StringBuilder();
        private readonly IDictionary<string, string> _parameterReplacementDictionary;

        public SqlExpressionTranslator(IDictionary<string, string> parameterReplacementDictionary = null)
        {
            _parameterReplacementDictionary = parameterReplacementDictionary;
        }

        public string GetStatement(Expression expression)
        {
            Visit(expression);
            return _filter.ToString();
        }

        #region ExpressionVisitor Members

        protected override Expression VisitBinary(BinaryExpression node)
        {
            _filter.Append("(");
            this.Visit(node.Left);

            string @operator;

            switch (node.NodeType)
            {
                case ExpressionType.Equal:
                    @operator = SqlTemplates.Equal;
                    break;

                case ExpressionType.NotEqual:
                    @operator = SqlTemplates.NotEqual;
                    break;

                case ExpressionType.And:
                case ExpressionType.AndAlso:
                    @operator = SqlTemplates.And;
                    break;

                case ExpressionType.Or:
                case ExpressionType.OrElse:
                    @operator = SqlTemplates.Or;
                    break;

                default:
                    throw new NotImplementedException(
                        $"BinaryExpression operator '{node.NodeType}' is not support at this time");
            }

            _filter.AppendFormat(" {0} ", @operator);
            this.Visit(node.Right);
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
                    _filter.Append(parameterName.AsSqlIdentifier());
                    return node;

                case ExpressionType.MemberAccess:
                    var memberExpression = (MemberExpression) node.Expression;
                    Expression deepExpression = memberExpression;
                    while (deepExpression is MemberExpression)
                    {
                        deepExpression = ((MemberExpression) deepExpression).Expression;
                    }                    
                    if (deepExpression is ConstantExpression)
                    {
                        var deepConstantExpression = (ConstantExpression)deepExpression;                        
                        if (node.Member is PropertyInfo)
                        {
                            if (memberExpression.Member is FieldInfo)
                            {
                                var fieldInfoValue =
                                    ((FieldInfo) memberExpression.Member).GetValue(deepConstantExpression.Value);
                                value = ((PropertyInfo) node.Member).GetValue(fieldInfoValue, null);
                                _filter.Append(ConvertSqlLiteral(value, node.Type));
                                return node;
                            }

                            if (memberExpression.Member is PropertyInfo)
                            {
                                var objectMember = Expression.Convert(node, typeof(object));
                                var getterLambda = Expression.Lambda<Func<object>>(objectMember);
                                var getter = getterLambda.Compile();
                                value = getter();
                                _filter.Append(ConvertSqlLiteral(value, node.Type));
                                return node;
                            }
                        }
                        else
                        {
                            throw new NotSupportedException();
                        }
                    }

                    _filter.Append(node.Member.Name.AsSqlIdentifier());
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

                    _filter.Append(ConvertSqlLiteral(value, node.Type));
                    return node;
            }

            throw new NotSupportedException($"The expression member '{node.Member.Name}' with node type '{node.Expression.NodeType}' is not supported");
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            _filter.Append(ConvertSqlLiteral(node.Value, node.Type));
            return node;
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.Name.Equals("Equals"))
            {
                this.Visit(BinaryExpression.Equal(node.Object, node.Arguments[0]));
            }
            else if (node.Method.Name.Equals("Contains") && node.Method.IsStatic && node.Method.DeclaringType.Name.Equals("Enumerable"))
            {
                var values = ((ConstantExpression)node.Arguments[0]).Value as IEnumerable<object>;
                var expression = node.Arguments[1];
                _filter.Append("(");
                this.Visit(expression);
                _filter.AppendFormat(" {0} (", SqlTemplates.In);
                foreach (var value in values)
                {
                    this.VisitConstant(Expression.Constant(value));
                    _filter.Append(",");
                }
                _filter.Remove(_filter.Length - 1, 1);
                _filter.Append("))");
            }
            else
            {
                throw new NotImplementedException(
                    $"Translation not implemented for method {node.Method.Name} of type {node.Method.DeclaringType}");
            }

            return node;
        }

        #endregion

        #region Private Methods

        private static string ConvertSqlLiteral(object value, Type type)
        {
            var dbType = TypeMapper.GetDbType(type);

            if (dbType == DbType.String ||
                dbType == DbType.StringFixedLength ||
                dbType == DbType.Guid ||
                dbType == DbType.Date ||
                dbType == DbType.DateTime ||
                dbType == DbType.DateTime2 ||
                dbType == DbType.DateTimeOffset)
            {
                return $"'{value}'";
            }

            return value.ToString();
        }

        #endregion
    }
}
