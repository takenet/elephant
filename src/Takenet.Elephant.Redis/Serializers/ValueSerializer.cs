using System;

namespace Takenet.Elephant.Redis.Serializers
{
    public class ValueSerializer<T> : ISerializer<T>
    {
        private static Func<string, T> _parseFunc;

        static ValueSerializer()
        {
            _parseFunc = TypeUtil.GetParseFunc<T>();                        
        } 

        public string Serialize(T value)
        {
            return value.ToString();
        }

        public T Deserialize(string value)
        {
            return _parseFunc(value);
        }
    }
}
