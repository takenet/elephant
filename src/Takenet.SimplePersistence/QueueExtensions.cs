using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence
{
    public static class QueueExtensions
    {
        /// <summary>
        /// Copies the content of the source queue to the specified destination.
        /// </summary>
        /// <typeparam name="TItem">The type of the item.</typeparam>
        /// <param name="source">The source queue.</param>
        /// <param name="destination">The destination queue.</param>
        /// <returns></returns>
        public static async Task CopyToAsync<TItem>(this IQueue<TItem> source, IQueue<TItem> destination)
        {            
            var temp = new Memory.Queue<TItem>();
            await MoveToAsync(source, destination, temp).ConfigureAwait(false);
            await MoveToAsync(temp, source).ConfigureAwait(false);
        }

        /// <summary>
        /// Moves the content of the source queue to the specified destinations.
        /// </summary>
        /// <typeparam name="TItem">The type of the item.</typeparam>
        /// <param name="source">The source.</param>
        /// <param name="destination">The destination queue.</param>
        /// <param name="destination2">The alternative destination queue.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException">
        /// </exception>
        public static async Task MoveToAsync<TItem>(this IQueue<TItem> source, IQueue<TItem> destination, IQueue<TItem> destination2 = null)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (destination == null) throw new ArgumentNullException(nameof(destination));
            while (await source.GetLengthAsync().ConfigureAwait(false) > 0)
            {
                var item = await source.DequeueOrDefaultAsync().ConfigureAwait(false);
                await destination.EnqueueAsync(item).ConfigureAwait(false);
                if (destination2 != null) await destination2.EnqueueAsync(item).ConfigureAwait(false);                
            }
        }
    }
}
