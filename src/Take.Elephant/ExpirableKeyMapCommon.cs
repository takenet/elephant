using System;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant
{
    public static class ExpirableKeyMapCommon
    {
        public static Task<bool> TryAddWithRelativeExpirationAsync<TKey, TValue>(
            IExpirableKeyMap<TKey, TValue> map,
            TKey key,
            TValue value,
            TimeSpan expiration = default,
            bool overwrite = false,
            CancellationToken cancellationToken = default)
        {
            return map.TryAddWithAbsoluteExpirationAsync(key, value,
                DateTimeOffset.UtcNow.Add(expiration), overwrite, cancellationToken);
        }

        public static async Task<bool> TryAddWithAbsoluteExpirationAsync<TKey, TValue>(
            IExpirableKeyMap<TKey, TValue> map,
            TKey key,
            TValue value,
            DateTimeOffset expiration = default,
            bool overwrite = false,
            CancellationToken cancellationToken = default)
        {
            var added = await map.TryAddAsync(key, value, overwrite, cancellationToken);

            if (added)
            {
                await map.SetAbsoluteKeyExpirationAsync(key, expiration);
            }

            return added;
        }
    }
}