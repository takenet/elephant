using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Takenet.SimplePersistence
{
    public class TypeDictionaryConverter<T> : IDictionaryConverter<T>
    {        
        private static readonly Dictionary<string, Func<object, object>> _getFuncsDictionary;
        private static readonly Dictionary<string, Action<object, object>> _setActionsDictionary;
        private static readonly bool _isSimpleType;
        private const string VALUE_KEY = "Value";        
        
        static TypeDictionaryConverter()
        {
            var type = typeof (T);
            _isSimpleType = type.IsSimpleType();

            if (!_isSimpleType)
            {
                var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public);
                _getFuncsDictionary = properties.ToDictionary(p => p.Name, TypeUtil.BuildGetAccessor);
                _setActionsDictionary = properties.ToDictionary(p => p.Name, TypeUtil.BuildSetAccessor);
            }
        }

        private readonly Func<T> _valueFactory;

        public TypeDictionaryConverter(Func<T> valueFactory)
        {
            _valueFactory = valueFactory;
        }

        public T FromDictionary(IDictionary<string, object> dictionary)
        {
            if (dictionary == null) throw new ArgumentNullException(nameof(dictionary));
            if (_isSimpleType)
            {
                return (T)dictionary[VALUE_KEY];
            }
            var value = _valueFactory();
            foreach (var item in dictionary)
            {
                Action<object, object> setAction;
                if (!_setActionsDictionary.TryGetValue(item.Key, out setAction)) continue;
                setAction(value, item.Value);
            }
            return value;
        }

        public IDictionary<string, object> ToDictionary(T value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            if (_isSimpleType)
            {
                return new Dictionary<string, object>()
                {
                    {VALUE_KEY, value}
                };
            }
            return _getFuncsDictionary.ToDictionary(i => i.Key, i => i.Value(value));
        }
    }
}