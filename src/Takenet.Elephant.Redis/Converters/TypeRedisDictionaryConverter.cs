using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using StackExchange.Redis;

namespace Takenet.Elephant.Redis.Converters
{
    public class TypeRedisDictionaryConverter<T> : IRedisDictionaryConverter<T> where T : class
    {
        private readonly Func<T> _valueFactory;
        private readonly bool _emitNullValues;
        private readonly IDictionary<string, Type> _propertyDictionary;
        private readonly IDictionary<string, Func<T, object>> _propertyGetFuncDictionary;
        private readonly IDictionary<string, Action<T, object>> _propertySetActionDictionary;

        public TypeRedisDictionaryConverter(bool emitNullValues = false)
            : this(p => true, emitNullValues)
        {

        }

        public TypeRedisDictionaryConverter(Func<PropertyInfo, bool> propertyFilter, bool emitNullValues = false)
            : this(propertyFilter, Activator.CreateInstance<T>, emitNullValues)
        {

        }

        public TypeRedisDictionaryConverter(Func<PropertyInfo, bool> propertyFilter, Func<T> valueFactory, bool emitNullValues = false)
            : this(typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(propertyFilter).ToArray(), valueFactory, emitNullValues)
        {

        }

        protected TypeRedisDictionaryConverter(PropertyInfo[] properties, Func<T> valueFactory, bool emitNullValues)
        {            
            _propertyDictionary = properties.ToDictionary(p => p.Name, p => p.PropertyType);
            _propertyGetFuncDictionary = new Dictionary<string, Func<T, object>>();
            _propertySetActionDictionary = new Dictionary<string, Action<T, object>>();

            foreach (var property in properties)
            {
                _propertyGetFuncDictionary.Add(
                    property.Name,
                    TypeUtil.BuildGetAccessor(property));                

                _propertySetActionDictionary.Add(
                    property.Name,
                    TypeUtil.BuildSetAccessor(property));
            }

            Properties = _propertyDictionary.Keys;
            _valueFactory = valueFactory;
            _emitNullValues = emitNullValues;
        }

        public IEnumerable<string> Properties { get; }

        public T FromDictionary(IDictionary<string, RedisValue> dictionary)
        {
            var value = _valueFactory();
            foreach (var item in dictionary)
            {
                Action<T, object> setAction;
                if (!_propertySetActionDictionary.TryGetValue(item.Key, out setAction)) continue;
                setAction(value, item.Value.Cast(_propertyDictionary[item.Key]));
            }
            return value;
        }

        public IDictionary<string, RedisValue> ToDictionary(T value)
        {
            return _propertyGetFuncDictionary
                .ToDictionary(
                    p => p.Key,
                    p => p.Value(value))
                .Where(
                    p => _emitNullValues || p.Value != null)
                .ToDictionary(
                    p => p.Key,
                    p => p.Value.ToRedisValue());
        }
    }
}
