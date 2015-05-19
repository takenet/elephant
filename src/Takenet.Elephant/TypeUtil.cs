using System;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;

namespace Takenet.Elephant
{
    public static class TypeUtil
    {
        private static readonly ConcurrentDictionary<Type, Func<string, object>> _typeParseFuncDictionary;

        static TypeUtil()
        {
            _typeParseFuncDictionary = new ConcurrentDictionary<Type, Func<string, object>>();
        }

        /// <summary>
        /// Gets the Parse static method of a Type as a func.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public static Func<string, T> GetParseFunc<T>()
        {
            var type = typeof(T);

            var parseMethod = typeof(T)
                .GetMethod("Parse", BindingFlags.Static | BindingFlags.Public, null, new[] { typeof(string) }, null);

            if (parseMethod == null) throw new ArgumentException($"The type '{type}' doesn't contains a static 'Parse' method");            
            if (parseMethod.ReturnType != type) throw new ArgumentException("The Parse method has an invalid return type");            

            var parseFuncType = typeof(Func<,>).MakeGenericType(typeof(string), type);
            return (Func<string, T>)Delegate.CreateDelegate(parseFuncType, parseMethod);
        }

        /// <summary>
        /// Gets the Parse static  method of a Type as a func.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public static Func<string, object> GetParseFuncForType(Type type)
        {
            if (type == null) throw new ArgumentNullException(nameof(type));
                        
            Func<string, object> parseFunc;
            if (_typeParseFuncDictionary.TryGetValue(type, out parseFunc)) return parseFunc;
            try
            {
                var getParseFuncMethod = typeof(TypeUtil)
                    .GetMethod("GetParseFunc", BindingFlags.Static | BindingFlags.Public)
                    .MakeGenericMethod(type);

                var genericGetParseFunc = getParseFuncMethod.Invoke(null, null);

                var parseFuncAdapterMethod = typeof(TypeUtil)
                    .GetMethod("ParseFuncAdapter", BindingFlags.Static | BindingFlags.NonPublic)
                    .MakeGenericMethod(type);

                parseFunc = (Func<string, object>)parseFuncAdapterMethod.Invoke(null, new[] { genericGetParseFunc });
                _typeParseFuncDictionary.TryAdd(type, parseFunc);
            }
            catch (TargetInvocationException ex)
            {
                throw ex.InnerException;
            }

            return parseFunc;
        }

        /// <summary>
        /// Build a delegate to get a property value of a class.
        /// </summary>
        /// <a href="http://stackoverflow.com/questions/10820453/reflection-performance-create-delegate-properties-c"/>
        /// <param name="methodInfo"></param>
        /// <returns></returns>
        public static Func<object, object> BuildGetAccessor(PropertyInfo propertyInfo)
        {
            return BuildGetAccessor(propertyInfo.GetGetMethod());
        }

        /// <summary>
        /// Build a delegate to get a property value of a class.
        /// </summary>
        /// <a href="http://stackoverflow.com/questions/10820453/reflection-performance-create-delegate-properties-c"/>
        /// <param name="methodInfo"></param>
        /// <returns></returns>
        public static Func<object, object> BuildGetAccessor(MethodInfo methodInfo)
        {
            if (methodInfo == null) throw new ArgumentNullException(nameof(methodInfo));            
            var obj = Expression.Parameter(typeof(object), "o");

            Expression<Func<object, object>> expr =
                Expression.Lambda<Func<object, object>>(
                    Expression.Convert(
                        Expression.Call(
                            Expression.Convert(obj, methodInfo.DeclaringType),
                            methodInfo),
                        typeof(object)),
                    obj);

            return expr.Compile();
        }

        /// <summary>
        /// Build a delegate to set a property value of a class.
        /// </summary>
        /// <a href="http://stackoverflow.com/questions/10820453/reflection-performance-create-delegate-properties-c"/>
        /// <param name="methodInfo"></param>
        /// <returns></returns>
        public static Action<object, object> BuildSetAccessor(PropertyInfo propertyInfo)
        {
            return BuildSetAccessor(propertyInfo.GetSetMethod());
        }

        /// <summary>
        /// Build a delegate to set a property value of a class.
        /// </summary>
        /// <a href="http://stackoverflow.com/questions/10820453/reflection-performance-create-delegate-properties-c"/>
        /// <param name="methodInfo"></param>
        /// <returns></returns>
        public static Action<object, object> BuildSetAccessor(MethodInfo methodInfo)
        {
            if (methodInfo == null) throw new ArgumentNullException(nameof(methodInfo));
            

            var obj = Expression.Parameter(typeof(object), "o");
            var value = Expression.Parameter(typeof(object));

            Expression<Action<object, object>> expr =
                Expression.Lambda<Action<object, object>>(
                    Expression.Call(
                        Expression.Convert(obj, methodInfo.DeclaringType),
                        methodInfo,
                        Expression.Convert(value, methodInfo.GetParameters()[0].ParameterType)),
                    obj,
                    value);

            return expr.Compile();
        }
    }
}
