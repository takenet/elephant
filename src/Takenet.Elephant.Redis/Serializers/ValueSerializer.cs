using System;

namespace Takenet.Elephant.Redis.Serializers
{
    /// <summary>
    /// Provides serialization using the type's ToString() and static Parse(string value) methods.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class ValueSerializer<T> : ISerializer<T>
    {
        private readonly bool _valueToLower;
        private static readonly Func<string, T> _parseFunc;

        static ValueSerializer()
        {
            try
            {
                _parseFunc = TypeUtil.GetParseFunc<T>();
            }
            catch (ArgumentException ex)
            {
                throw new NotSupportedException("The type must define a static 'Parse(string)' method", ex);
            }
        }
       
        public virtual string Serialize(T value)
        {
            return value.ToString();
        }

        public virtual T Deserialize(string value)
        {
            return _parseFunc(value);
        }
    }
}