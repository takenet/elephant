using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence.Sql
{
    internal class ExpressionTranslator : ExpressionVisitor
    {
        private readonly StringBuilder _filter = new StringBuilder();

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

            var @operator = string.Empty;

            switch (node.NodeType)
            {
                case ExpressionType.Equal:
                    @operator = SqlTemplates.Equal;
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
                    throw new NotImplementedException(string.Format("BinaryExpression operator '{0}' is not support at this time", node.NodeType));
            }

            _filter.AppendFormat(" {0} ", @operator);
            this.Visit(node.Right);
            _filter.Append(")");

            return node;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression == null)
            {
                throw new NotSupportedException(string.Format("The member '{0}' is not supported", node.Member.Name));
            }

            switch (node.Expression.NodeType)
            {
                case ExpressionType.Parameter:
                    _filter.Append(node.Member.Name.AsSqlIdentifier());
                    return node;

                case ExpressionType.Constant:
                    var constantExpression = (ConstantExpression)node.Expression;
                    var member = node.Member;
                    object value = null;
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


            throw new NotSupportedException(string.Format("The member '{0}' is not supported", node.Member.Name));

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
                throw new NotImplementedException(string.Format("Translation not implemented for method {0} of type {1}", node.Method.Name, node.Method.DeclaringType));
            }

            return node;
        }

        #endregion

        #region Private Methods

        private string ConvertSqlLiteral(object value, Type type)
        {
            var dbType = TypeMapper.GetDbType(type);

            if (dbType == DbType.String ||
                dbType == DbType.StringFixedLength)
            {
                return string.Format("'{0}'", value.ToString());
            }

            return value.ToString();
        }

        #endregion
    }
}
