using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Takenet.Elephant
{
    public class DictionaryConverter<T> : IDictionaryConverter<T>
    {
        private readonly IDictionary<string, Type> _propertyDictionary;
        private readonly Dictionary<string, Func<object, object>> _getFuncsDictionary;
        private readonly Dictionary<string, Action<object, object>> _setActionsDictionary;

        private static readonly bool _isSimpleType;
        private const string VALUE_KEY = "Value";
        
        static DictionaryConverter()
        {
            var type = typeof (T);
            _isSimpleType = type.IsSimpleType();
        }

        private readonly Func<T> _valueFactory;
        private readonly bool _emitDefaultValues;

        public DictionaryConverter(bool emitDefaultValues = false)
            : this(Activator.CreateInstance<T>, emitDefaultValues)
        {
            
        }

        public DictionaryConverter(Func<PropertyInfo, bool> propertyFilter, bool emitDefaultValues = false)
            : this(Activator.CreateInstance<T>, propertyFilter, emitDefaultValues)
        {

        }

        public DictionaryConverter(Func<T> valueFactory, bool emitDefaultValues = false)
            : this(valueFactory, p => true, emitDefaultValues)
        {
            
        }

        public DictionaryConverter(Func<T> valueFactory, Func<PropertyInfo, bool> propertyFilter, bool emitDefaultValues = false)
        {
            _valueFactory = valueFactory;
            _emitDefaultValues = emitDefaultValues;
                        
            if (!_isSimpleType)
            {
                var properties = typeof (T)
                    .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                    .Where(propertyFilter)
                    .ToArray();

                if (properties.Any(p => !p.CanRead))
                {
                    throw new ArgumentException($"One or more properties of '{typeof(T).Name}' are not readable. Make sure these properties are public or they by using a property filter.");
                }

                if (properties.Any(p => !p.CanWrite))
                {
                    throw new ArgumentException($"One or more properties of '{typeof(T).Name}' are not writable. Make sure these properties are public or they by using a property filter.");
                }
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
                .Where(i => _emitDefaultValues || !i.Value.IsDefaultValueOfType(_propertyDictionary[i.Key]))
                .ToDictionary(i => i.Key, i => i.Value);
        }
    }
}