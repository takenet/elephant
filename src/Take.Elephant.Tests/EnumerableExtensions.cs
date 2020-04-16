using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Take.Elephant.Tests
{
    public static class EnumerableExtensions
    {
        public static Expression<Func<Item, bool>> GetContainsExpressionForGuidProperty(this IEnumerable<Guid> values)
        {
            var parameter = Expression.Parameter(typeof(Item), "g");
            var memberIdentiyExpression = Expression.Lambda<Func<Item, bool>>(
                                            Expression.Call(
                                                typeof(Enumerable), "Contains",
                                                new[] { typeof(Guid) },
                                                Expression.Constant(values),
                                                Expression.Property(parameter, "GuidProperty")),
                                            parameter);
            return memberIdentiyExpression;
        }
    }
}
