using System;

namespace Takenet.Elephant.Redis.Serializers
{
    public class ValueSerializer<T> : ISerializer<T>
    {
        private readonly bool _valueToLower;
        private static readonly Func<string, T> _parseFunc;

        static ValueSerializer()
        {
            _parseFunc = TypeUtil.GetParseFunc<T>();                        
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
