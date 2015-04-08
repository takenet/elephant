using System.Linq;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Takenet.SimplePersistence.Redis
{
    /// <summary>
    /// Implements the <see cref="IMap{TKey,TValue}"/> interface using Redis hash data structure.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public class RedisHashMap<TKey, TValue> : MapBase<TKey, TValue>, IPropertyMap<TKey, TValue>
    {
        private readonly IDictionaryConverter<TValue> _dictionaryConverter;

        #region Constructor

        public RedisHashMap(string mapName, IDictionaryConverter<TValue> dictionaryConverter, string configuration)
            : base(mapName, configuration)
        {
            _dictionaryConverter = dictionaryConverter;
        }

        #endregion

        #region IMap<TKey,TValue> Members

        public override async Task<bool> TryAddAsync(TKey key, TValue value, bool overwrite = false)
        {
            if (!overwrite &&
                await ContainsKeyAsync(key))
            {
                return false;
            }

            await MergeAsync(key, value);
            return true;
        }

        public override async Task<TValue> GetValueOrDefaultAsync(TKey key)
        {
            var database = GetDatabase();
            var hashEntries = await database.HashGetAllAsync(GetRedisKey(key));
            if (hashEntries != null &&
                hashEntries.Length > 0)
            {
                var entriesDictionary = hashEntries.ToDictionary(t => (string)t.Name, t => (object)t.Value);
                return _dictionaryConverter.FromDictionary(entriesDictionary);
            }

            return default(TValue);
        }

        public override Task<bool> TryRemoveAsync(TKey key)
        {
            var database = GetDatabase();
            return database.KeyDeleteAsync(GetRedisKey(key));
        }

        public override Task<bool> ContainsKeyAsync(TKey key)
        {
            var database = GetDatabase();
            return database.KeyExistsAsync(GetRedisKey(key));
        }

        #endregion

        #region IPropertyMap<TKey,TValue> Members

        public async Task SetPropertyValueAsync<TProperty>(TKey key, string propertyName, TProperty propertyValue)
        {
            var database = GetDatabase();
            await database.HashSetAsync(GetRedisKey(key), propertyName, propertyValue.ToRedisValue(), When.Always);
        }

        public Task MergeAsync(TKey key, TValue value)
        {
            var database = GetDatabase();

            var dictionary = _dictionaryConverter.ToDictionary(value);
            var hashEntries = dictionary
                .Select(i => new HashEntry(i.Key, i.Value.ToRedisValue()))
                .ToArray();

            return database.HashSetAsync(GetRedisKey(key), hashEntries);
        }

        public async Task<TProperty> GetPropertyValueOrDefaultAsync<TProperty>(TKey key, string propertyName)
        {
            var database = GetDatabase();

            var redisValue = await database.HashGetAsync(GetRedisKey(key), propertyName);
            if (!redisValue.IsNull)
            {
                return redisValue.Cast<TProperty>();
            }

            return default(TProperty);
        }

        #endregion            
    }
}
