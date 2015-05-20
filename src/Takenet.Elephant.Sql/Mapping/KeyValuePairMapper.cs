using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Takenet.Elephant.Sql.Mapping
{
    public class KeyValuePairMapper<TKey, TValue> : IMapper<KeyValuePair<TKey, TValue>>
    {
        private readonly IMapper<TKey> _keyMapper;
        private readonly IMapper<TValue> _valueMapper;

        public KeyValuePairMapper(IMapper<TKey> keyMapper, IMapper<TValue> valueMapper)
        {
            _keyMapper = keyMapper;
            _valueMapper = valueMapper;
        }

        public IDictionary<string, object> GetColumnValues(KeyValuePair<TKey, TValue> value, string[] columns = null, bool emitDefaultValues = false)
        {
            return _keyMapper
                .GetColumnValues(value.Key, columns, emitDefaultValues)
                .Union(
                    _valueMapper.GetColumnValues(value.Value, columns, emitDefaultValues))
                .ToDictionary(k => k.Key, v => v.Value);
        }

        public KeyValuePair<TKey, TValue> Create(IDataRecord record, string[] columns)
        {            
            return new KeyValuePair<TKey, TValue>(
                _keyMapper.Create(record, columns), 
                _valueMapper.Create(record, columns));            
        }
    }
}