using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Take.Elephant
{
    public class DictionaryConverter<T> : IDictionaryConverter<T>
    {
        private const string VALUE_KEY = "Value";
        private static readonly bool _isSimpleType;

        private readonly IDictionary<string, Type> _propertyDictionary;
        private readonly Dictionary<string, Func<object, object>> _getFuncsDictionary;
        private readonly Dictionary<string, Action<object, object>> _setActionsDictionary;        
        private readonly Func<T> _valueFactory;
        private readonly bool _emitNullValues;
                
        static DictionaryConverter()
        {
            var type = typeof (T);
            _isSimpleType = type.IsSimpleType();
        }

        public DictionaryConverter(bool emitNullValues = false)
            : this(Activator.CreateInstance<T>, emitNullValues)
        {
            
        }

        public DictionaryConverter(Func<PropertyInfo, bool> propertyFilter, bool emitNullValues = false)
            : this(Activator.CreateInstance<T>, propertyFilter, emitNullValues)
        {

        }

        public DictionaryConverter(Func<T> valueFactory, bool emitNullValues = false)
            : this(valueFactory, p => true, emitNullValues)
        {
            
        }

        public DictionaryConverter(Func<T> valueFactory, Func<PropertyInfo, bool> propertyFilter, bool emitNullValues = false)
        {
            _valueFactory = valueFactory;
            _emitNullValues = emitNullValues;
                        
            if (!_isSimpleType)
            {
                var properties = typeof (T)
                    .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                    .Where(propertyFilter)
                    .Where(p => p.CanRead && p.CanWrite)
                    .ToArray();
                _propertyDictionary = properties.ToDictionary(p => p.Name, p => p.PropertyType);
                _getFuncsDictionary = properties.ToDictionary(p => p.Name, TypeUtil.BuildGetAccessor);
                _setActionsDictionary = properties.ToDictionary(p => p.Name, TypeUtil.BuildSetAccessor);
            }
        }

        public IEnumerable<string> Properties => _propertyDictionary.Keys;

        public T FromDictionary(IDictionary<string, object> dictionary)
        {
            if (dictionary == null) throw new ArgumentNullException(nameof(dictionary));

            if (_isSimpleType)
            {
                object objectValue;
                dictionary.TryGetValue(VALUE_KEY, out objectValue);
                if (objectValue != null) return (T) objectValue;
                return default(T);
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
            if (_isSimpleType)
            {
                if (value == null || value.Equals(TypeUtil.GetDefaultValue<T>())) return new Dictionary<string, object>();
                return new Dictionary<string, object>()
                {
                    {VALUE_KEY, value}
                };
            }
            if (value == null) throw new ArgumentNullException(nameof(value));
            return _getFuncsDictionary
                .ToDictionary(i => i.Key, i => i.Value(value))
                .Where(i => _emitNullValues || i.Value != null)
                .ToDictionary(i => i.Key, i => i.Value);
        }
    }
}