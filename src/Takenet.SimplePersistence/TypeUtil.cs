using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence
{
    public static class TypeUtil
    {
        private static ConcurrentDictionary<Type, Func<string, object>> _typeParseFuncDictionary;

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

            if (parseMethod == null)
            {
                throw new ArgumentException(string.Format("The type '{0}' doesn't contains a static 'Parse' method", type));
            }

            if (parseMethod.ReturnType != type)
            {
                throw new ArgumentException("The Parse method has an invalid return type");
            }

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
            if (type == null)
            {
                throw new ArgumentNullException("type");
            }

            Func<string, object> parseFunc;

            if (!_typeParseFuncDictionary.TryGetValue(type, out parseFunc))
            {
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
            }

            return parseFunc;
        }
    }
}
