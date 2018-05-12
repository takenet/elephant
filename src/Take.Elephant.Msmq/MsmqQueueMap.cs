using System;
using System.Collections.Generic;
using System.Linq;
using System.Messaging;
using System.Text;
using System.Threading.Tasks;

namespace Take.Elephant.Msmq
{
    public class MsmqQueueMap<TKey, TItem> : IBlockingQueueMap<TKey, TItem>, IQueueMap<TKey, TItem>
    {
        public const string PATH_TEMPLATE_KEY_PLACEHOLDER = "{key}";

        private readonly string _pathTemplate;
        private readonly ISerializer<TItem> _serializer;
        private readonly IMessageFormatter _messageFormatter;
        private readonly bool _recoverable;

        public MsmqQueueMap(string pathTemplate, ISerializer<TItem> serializer = null,
            IMessageFormatter messageFormatter = null, bool recoverable = true)
        {
            if (pathTemplate == null) throw new ArgumentNullException(nameof(pathTemplate));
            if (!pathTemplate.Contains(PATH_TEMPLATE_KEY_PLACEHOLDER)) throw new ArgumentException($"The path template must contain the key placeholder value '{PATH_TEMPLATE_KEY_PLACEHOLDER}'", nameof(pathTemplate));
            _pathTemplate = pathTemplate;
            _serializer = serializer;
            _messageFormatter = messageFormatter;
            _recoverable = recoverable;
        }

        public virtual Task<bool> TryAddAsync(TKey key, IBlockingQueue<TItem> value, bool overwrite = false) => TryAddAsync(key, (IQueue<TItem>)value, overwrite);

        public virtual async Task<bool> TryAddAsync(TKey key, IQueue<TItem> value, bool overwrite = false)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (value == null) throw new ArgumentNullException(nameof(value));

            var internalQueue = value as InternalQueue;
            if (internalQueue != null) return internalQueue.Key.Equals(key) && overwrite;

            var queuePath = GetQueuePath(key, _pathTemplate);            
            if (MessageQueue.Exists(queuePath) && !overwrite) return false;

            internalQueue = CreateQueue(key);

            var queue = await CloneAsync(value).ConfigureAwait(false);
            while (await queue.GetLengthAsync().ConfigureAwait(false) > 0)
            {
                var item = await queue.DequeueOrDefaultAsync().ConfigureAwait(false);
                await internalQueue.EnqueueAsync(item).ConfigureAwait(false);
            }

            return true;
        }

        async Task<IQueue<TItem>> IMap<TKey, IQueue<TItem>>.GetValueOrDefaultAsync(TKey key) => await GetValueOrDefaultAsync(key).ConfigureAwait(false);

        public virtual Task<IBlockingQueue<TItem>> GetValueOrDefaultAsync(TKey key)
        {            
            if (MessageQueue.Exists(GetQueuePath(key, _pathTemplate)))
            {
                return Task.FromResult<IBlockingQueue<TItem>>(CreateQueue(key));
            }

            return Task.FromResult<IBlockingQueue<TItem>>(null);            
        }

        public Task<IQueue<TItem>> GetValueOrEmptyAsync(TKey key)
        {
            return Task.FromResult<IQueue<TItem>>(CreateQueue(key));
        }

        public virtual Task<bool> TryRemoveAsync(TKey key)
        {
            var queuePath = GetQueuePath(key, _pathTemplate);
            if (MessageQueue.Exists(queuePath))
            {
                MessageQueue.Delete(queuePath);
                return Task.FromResult(true);
            }

            return Task.FromResult(false);
        }

        public virtual Task<bool> ContainsKeyAsync(TKey key)
        {
            var queuePath = GetQueuePath(key, _pathTemplate);
            return MessageQueue.Exists(queuePath).AsCompletedTask();
        }

        protected virtual string GetQueuePath(TKey key, string pathTemplate)
        {
            return _pathTemplate.Replace(PATH_TEMPLATE_KEY_PLACEHOLDER, KeyToString(key));
        }

        protected virtual string KeyToString(TKey key)
        {
            return key.ToString();
        }

        protected virtual InternalQueue CreateQueue(TKey key)
        {
            return new InternalQueue(key, GetQueuePath(key, _pathTemplate), _serializer, _messageFormatter, _recoverable);
        }

        private static async Task<IQueue<TItem>> CloneAsync(IQueue<TItem> queue)
        {
            var cloneable = queue as ICloneable;
            if (cloneable != null) return (IQueue<TItem>)cloneable.Clone();

            var clone = new Memory.Queue<TItem>();
            await queue.CopyToAsync(clone).ConfigureAwait(false);
            return clone;
        }

        protected class InternalQueue : MsmqQueue<TItem>
        {
            public TKey Key { get; }

            public InternalQueue(TKey key, string path, ISerializer<TItem> serializer = null, IMessageFormatter messageFormatter = null, bool recoverable = true) 
                : base(path, serializer, messageFormatter, recoverable)
            {
                Key = key;
            }
        }


    }
}
