using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence
{
    public static class AsyncEnumerableExtensions
    {
        // <summary>
        // Asynchronously executes the provided action on each element of the <see cref="IDbAsyncEnumerable" />.
        // </summary>
        // <param name="action"> The action to be executed. </param>
        // <param name="cancellationToken"> The token to monitor for cancellation requests. </param>
        // <returns> A Task representing the asynchronous operation. </returns>
        public static async Task ForEachAsync(
            this IAsyncEnumerable source, Action<object> action, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");
            if (action == null) throw new ArgumentNullException("action");

            cancellationToken.ThrowIfCancellationRequested();

            using (var enumerator = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                if (await enumerator.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    Task<bool> moveNextTask;
                    do
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        var current = enumerator.Current;
                        moveNextTask = enumerator.MoveNextAsync(cancellationToken);
                        action(current);
                    }
                    while (await moveNextTask.ConfigureAwait(continueOnCapturedContext: false));
                }
            }
        }

        // <summary>
        // Asynchronously executes the provided action on each element of the <see cref="IAsyncEnumerable{T}" />.
        // </summary>
        // <param name="action"> The action to be executed. </param>
        // <param name="cancellationToken"> The token to monitor for cancellation requests. </param>
        // <returns> A Task representing the asynchronous operation. </returns>
        public static async Task ForEachAsync<T>(
            this IAsyncEnumerable<T> source, Action<T> action, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");
            if (action == null) throw new ArgumentNullException("action");

            await ForEachAsync(await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false), action, cancellationToken);
        }

        private static async Task ForEachAsync<T>(
            IAsyncEnumerator<T> enumerator, Action<T> action, CancellationToken cancellationToken)
        {
            using (enumerator)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (await enumerator.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    Task<bool> moveNextTask;
                    do
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        var current = enumerator.Current;
                        moveNextTask = enumerator.MoveNextAsync(cancellationToken);
                        action(current);
                    }
                    while (await moveNextTask.ConfigureAwait(continueOnCapturedContext: false));
                }
            }
        }

        // <summary>
        // Asynchronously creates a <see cref="List{T}" /> from the <see cref="IDbAsyncEnumerable" />.
        // </summary>
        // <typeparam name="T"> The type that the elements will be cast to. </typeparam>
        // <returns>
        // A <see cref="Task" /> containing a <see cref="List{T}" /> that contains elements from the input sequence.
        // </returns>
        public static Task<List<T>> ToListAsync<T>(this IAsyncEnumerable source)
        {
            if (source == null) throw new ArgumentNullException("source");

            return source.ToListAsync<T>(CancellationToken.None);
        }

        // <summary>
        // Asynchronously creates a <see cref="List{T}" /> from the <see cref="IDbAsyncEnumerable" />.
        // </summary>
        // <typeparam name="T"> The type that the elements will be cast to. </typeparam>
        // <param name="cancellationToken"> The token to monitor for cancellation requests. </param>
        // <returns>
        // A <see cref="Task" /> containing a <see cref="List{T}" /> that contains elements from the input sequence.
        // </returns>
        public static async Task<List<T>> ToListAsync<T>(this IAsyncEnumerable source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            var list = new List<T>();
            await source.ForEachAsync(e => list.Add((T)e), cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
            return list;
        }

        // <summary>
        // Asynchronously creates a <see cref="List{T}" /> from the <see cref="IAsyncEnumerable{T}" />.
        // </summary>
        // <returns>
        // A <see cref="Task" /> containing a <see cref="List{T}" /> that contains elements from the input sequence.
        // </returns>
        [SuppressMessage("Microsoft.Design", "CA1006:DoNotNestGenericTypesInMemberSignatures")]
        public static Task<List<T>> ToListAsync<T>(this IAsyncEnumerable<T> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            return source.ToListAsync(CancellationToken.None);
        }

        // <summary>
        // Asynchronously creates a <see cref="List{T}" /> from the <see cref="IAsyncEnumerable{T}" />.
        // </summary>
        // <param name="cancellationToken"> The token to monitor for cancellation requests. </param>
        // <returns>
        // A <see cref="Task" /> containing a <see cref="List{T}" /> that contains elements from the input sequence.
        // </returns>
        [SuppressMessage("Microsoft.Design", "CA1006:DoNotNestGenericTypesInMemberSignatures")]
        public static Task<List<T>> ToListAsync<T>(this IAsyncEnumerable<T> source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            var tcs = new TaskCompletionSource<List<T>>();
            var list = new List<T>();
            source.ForEachAsync(list.Add, cancellationToken).ContinueWith(
                t =>
                {
                    if (t.IsFaulted)
                    {
                        tcs.TrySetException(t.Exception.InnerExceptions);
                    }
                    else if (t.IsCanceled)
                    {
                        tcs.TrySetCanceled();
                    }
                    else
                    {
                        tcs.TrySetResult(list);
                    }
                }, TaskContinuationOptions.ExecuteSynchronously);

            return tcs.Task;
        }

        // <summary>
        // Asynchronously creates a T[] from an <see cref="IAsyncEnumerable{T}" /> by enumerating it asynchronously.
        // </summary>
        // <typeparam name="T">
        // The type of the elements of <paramref name="source" /> .
        // </typeparam>
        // <returns>
        // A <see cref="Task" /> containing a T[] that contains elements from the input sequence.
        // </returns>
        [SuppressMessage("Microsoft.Design", "CA1006:DoNotNestGenericTypesInMemberSignatures")]
        public static Task<T[]> ToArrayAsync<T>(this IAsyncEnumerable<T> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            return source.ToArrayAsync(CancellationToken.None);
        }

        // <summary>
        // Asynchronously creates a T[] from an <see cref="IAsyncEnumerable{T}" /> by enumerating it asynchronously.
        // </summary>
        // <typeparam name="T">
        // The type of the elements of <paramref name="source" /> .
        // </typeparam>
        // <param name="cancellationToken"> The token to monitor for cancellation requests. </param>
        // <returns>
        // A <see cref="Task" /> containing a T[] that contains elements from the input sequence.
        // </returns>
        [SuppressMessage("Microsoft.Design", "CA1006:DoNotNestGenericTypesInMemberSignatures")]
        public static async Task<T[]> ToArrayAsync<T>(this IAsyncEnumerable<T> source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            var list = await source.ToListAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
            return list.ToArray();
        }

        // <summary>
        // Asynchronously creates a <see cref="Dictionary{TKey, TSource}" /> from an <see cref="IAsyncEnumerable{TSource}" />
        // by enumerating it asynchronously according to a specified key selector function.
        // </summary>
        // <typeparam name="TSource">
        // The type of the elements of <paramref name="source" /> .
        // </typeparam>
        // <typeparam name="TKey">
        // The type of the key returned by <paramref name="keySelector" /> .
        // </typeparam>
        // <param name="keySelector"> A function to extract a key from each element. </param>
        // <returns>
        // A <see cref="Task" /> containing a <see cref="Dictionary{TKey, TSource}" /> that contains selected keys and values.
        // </returns>
        [SuppressMessage("Microsoft.Design", "CA1006:DoNotNestGenericTypesInMemberSignatures")]
        public static Task<Dictionary<TKey, TSource>> ToDictionaryAsync<TSource, TKey>(
            this IAsyncEnumerable<TSource> source, Func<TSource, TKey> keySelector)
        {
            if (source == null) throw new ArgumentNullException("source");
            if (keySelector == null) throw new ArgumentNullException("keySelector");

            return ToDictionaryAsync(source, keySelector, IdentityFunction<TSource>.Instance, null, CancellationToken.None);
        }

        // <summary>
        // Asynchronously creates a <see cref="Dictionary{TKey, TSource}" /> from an <see cref="IAsyncEnumerable{TSource}" />
        // by enumerating it asynchronously according to a specified key selector function.
        // </summary>
        // <typeparam name="TSource">
        // The type of the elements of <paramref name="source" /> .
        // </typeparam>
        // <typeparam name="TKey">
        // The type of the key returned by <paramref name="keySelector" /> .
        // </typeparam>
        // <param name="keySelector"> A function to extract a key from each element. </param>
        // <param name="cancellationToken"> The token to monitor for cancellation requests. </param>
        // <returns>
        // A <see cref="Task" /> containing a <see cref="Dictionary{TKey, TSource}" /> that contains selected keys and values.
        // </returns>
        [SuppressMessage("Microsoft.Design", "CA1006:DoNotNestGenericTypesInMemberSignatures")]
        public static Task<Dictionary<TKey, TSource>> ToDictionaryAsync<TSource, TKey>(
            this IAsyncEnumerable<TSource> source, Func<TSource, TKey> keySelector, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");
            if (keySelector == null) throw new ArgumentNullException("keySelector");

            return ToDictionaryAsync(source, keySelector, IdentityFunction<TSource>.Instance, null, cancellationToken);
        }

        // <summary>
        // Asynchronously creates a <see cref="Dictionary{TKey, TSource}" /> from an <see cref="IAsyncEnumerable{TSource}" />
        // by enumerating it asynchronously according to a specified key selector function and a comparer.
        // </summary>
        // <typeparam name="TSource">
        // The type of the elements of <paramref name="source" /> .
        // </typeparam>
        // <typeparam name="TKey">
        // The type of the key returned by <paramref name="keySelector" /> .
        // </typeparam>
        // <param name="keySelector"> A function to extract a key from each element. </param>
        // <param name="comparer">
        // An <see cref="IEqualityComparer{TKey}" /> to compare keys.
        // </param>
        // <returns>
        // A <see cref="Task" /> containing a <see cref="Dictionary{TKey, TSource}" /> that contains selected keys and values.
        // </returns>
        [SuppressMessage("Microsoft.Design", "CA1006:DoNotNestGenericTypesInMemberSignatures")]
        public static Task<Dictionary<TKey, TSource>> ToDictionaryAsync<TSource, TKey>(
            this IAsyncEnumerable<TSource> source, Func<TSource, TKey> keySelector, IEqualityComparer<TKey> comparer)
        {
            if (source == null) throw new ArgumentNullException("source");
            if (keySelector == null) throw new ArgumentNullException("keySelector");

            return ToDictionaryAsync(source, keySelector, IdentityFunction<TSource>.Instance, comparer, CancellationToken.None);
        }

        // <summary>
        // Asynchronously creates a <see cref="Dictionary{TKey, TSource}" /> from an <see cref="IAsyncEnumerable{TSource}" />
        // by enumerating it asynchronously according to a specified key selector function and a comparer.
        // </summary>
        // <typeparam name="TSource">
        // The type of the elements of <paramref name="source" /> .
        // </typeparam>
        // <typeparam name="TKey">
        // The type of the key returned by <paramref name="keySelector" /> .
        // </typeparam>
        // <param name="keySelector"> A function to extract a key from each element. </param>
        // <param name="comparer">
        // An <see cref="IEqualityComparer{TKey}" /> to compare keys.
        // </param>
        // <param name="cancellationToken"> The token to monitor for cancellation requests. </param>
        // <returns>
        // A <see cref="Task" /> containing a <see cref="Dictionary{TKey, TSource}" /> that contains selected keys and values.
        // </returns>
        [SuppressMessage("Microsoft.Design", "CA1006:DoNotNestGenericTypesInMemberSignatures")]
        public static Task<Dictionary<TKey, TSource>> ToDictionaryAsync<TSource, TKey>(
            this IAsyncEnumerable<TSource> source, Func<TSource, TKey> keySelector, IEqualityComparer<TKey> comparer,
            CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");
            if (keySelector == null) throw new ArgumentNullException("keySelector");

            return ToDictionaryAsync(source, keySelector, IdentityFunction<TSource>.Instance, comparer, cancellationToken);
        }

        // <summary>
        // Asynchronously creates a <see cref="Dictionary{TKey, TElement}" /> from an <see cref="IAsyncEnumerable{TSource}" />
        // by enumerating it asynchronously according to a specified key selector and an element selector function.
        // </summary>
        // <typeparam name="TSource">
        // The type of the elements of <paramref name="source" /> .
        // </typeparam>
        // <typeparam name="TKey">
        // The type of the key returned by <paramref name="keySelector" /> .
        // </typeparam>
        // <typeparam name="TElement">
        // The type of the value returned by <paramref name="elementSelector" /> .
        // </typeparam>
        // <param name="keySelector"> A function to extract a key from each element. </param>
        // <param name="elementSelector"> A transform function to produce a result element value from each element. </param>
        // <returns>
        // A <see cref="Task" /> containing a <see cref="Dictionary{TKey, TElement}" /> that contains values of type
        // <typeparamref
        //     name="TElement" />
        // selected from the input sequence.
        // </returns>
        [SuppressMessage("Microsoft.Design", "CA1006:DoNotNestGenericTypesInMemberSignatures")]
        public static Task<Dictionary<TKey, TElement>> ToDictionaryAsync<TSource, TKey, TElement>(
            this IAsyncEnumerable<TSource> source, Func<TSource, TKey> keySelector, Func<TSource, TElement> elementSelector)
        {
            if (source == null) throw new ArgumentNullException("source");
            if (keySelector == null) throw new ArgumentNullException("keySelector");
            if (keySelector == null) throw new ArgumentNullException("elementSelector");

            return ToDictionaryAsync(source, keySelector, elementSelector, null, CancellationToken.None);
        }

        // <summary>
        // Asynchronously creates a <see cref="Dictionary{TKey, TElement}" /> from an <see cref="IAsyncEnumerable{TSource}" />
        // by enumerating it asynchronously according to a specified key selector and an element selector function.
        // </summary>
        // <typeparam name="TSource">
        // The type of the elements of <paramref name="source" /> .
        // </typeparam>
        // <typeparam name="TKey">
        // The type of the key returned by <paramref name="keySelector" /> .
        // </typeparam>
        // <typeparam name="TElement">
        // The type of the value returned by <paramref name="elementSelector" /> .
        // </typeparam>
        // <param name="keySelector"> A function to extract a key from each element. </param>
        // <param name="elementSelector"> A transform function to produce a result element value from each element. </param>
        // <param name="cancellationToken"> The token to monitor for cancellation requests. </param>
        // <returns>
        // A <see cref="Task" /> containing a <see cref="Dictionary{TKey, TElement}" /> that contains values of type
        // <typeparamref
        //     name="TElement" />
        // selected from the input sequence.
        // </returns>
        [SuppressMessage("Microsoft.Design", "CA1006:DoNotNestGenericTypesInMemberSignatures")]
        public static Task<Dictionary<TKey, TElement>> ToDictionaryAsync<TSource, TKey, TElement>(
            this IAsyncEnumerable<TSource> source, Func<TSource, TKey> keySelector, Func<TSource, TElement> elementSelector,
            CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");
            if (keySelector == null) throw new ArgumentNullException("keySelector");
            if (keySelector == null) throw new ArgumentNullException("elementSelector");

            return ToDictionaryAsync(source, keySelector, elementSelector, null, cancellationToken);
        }

        // <summary>
        // Asynchronously creates a <see cref="Dictionary{TKey, TElement}" /> from an <see cref="IAsyncEnumerable{TSource}" />
        // by enumerating it asynchronously according to a specified key selector function, a comparer, and an element selector function.
        // </summary>
        // <typeparam name="TSource">
        // The type of the elements of <paramref name="source" /> .
        // </typeparam>
        // <typeparam name="TKey">
        // The type of the key returned by <paramref name="keySelector" /> .
        // </typeparam>
        // <typeparam name="TElement">
        // The type of the value returned by <paramref name="elementSelector" /> .
        // </typeparam>
        // <param name="keySelector"> A function to extract a key from each element. </param>
        // <param name="elementSelector"> A transform function to produce a result element value from each element. </param>
        // <param name="comparer">
        // An <see cref="IEqualityComparer{TKey}" /> to compare keys.
        // </param>
        // <returns>
        // A <see cref="Task" /> containing a <see cref="Dictionary{TKey, TElement}" /> that contains values of type
        // <typeparamref
        //     name="TElement" />
        // selected from the input sequence.
        // </returns>
        [SuppressMessage("Microsoft.Design", "CA1006:DoNotNestGenericTypesInMemberSignatures")]
        public static Task<Dictionary<TKey, TElement>> ToDictionaryAsync<TSource, TKey, TElement>(
            this IAsyncEnumerable<TSource> source, Func<TSource, TKey> keySelector, Func<TSource, TElement> elementSelector,
            IEqualityComparer<TKey> comparer)
        {
            if (source == null) throw new ArgumentNullException("source");
            if (keySelector == null) throw new ArgumentNullException("keySelector");
            if (keySelector == null) throw new ArgumentNullException("elementSelector");

            return ToDictionaryAsync(source, keySelector, elementSelector, comparer, CancellationToken.None);
        }

        // <summary>
        // Asynchronously creates a <see cref="Dictionary{TKey, TElement}" /> from an <see cref="IAsyncEnumerable{TSource}" />
        // by enumerating it asynchronously according to a specified key selector function, a comparer, and an element selector function.
        // </summary>
        // <typeparam name="TSource">
        // The type of the elements of <paramref name="source" /> .
        // </typeparam>
        // <typeparam name="TKey">
        // The type of the key returned by <paramref name="keySelector" /> .
        // </typeparam>
        // <typeparam name="TElement">
        // The type of the value returned by <paramref name="elementSelector" /> .
        // </typeparam>
        // <param name="keySelector"> A function to extract a key from each element. </param>
        // <param name="elementSelector"> A transform function to produce a result element value from each element. </param>
        // <param name="comparer">
        // An <see cref="IEqualityComparer{TKey}" /> to compare keys.
        // </param>
        // <param name="cancellationToken"> The token to monitor for cancellation requests. </param>
        // <returns>
        // A <see cref="Task" /> containing a <see cref="Dictionary{TKey, TElement}" /> that contains values of type
        // <typeparamref
        //     name="TElement" />
        // selected from the input sequence.
        // </returns>
        [SuppressMessage("Microsoft.Design", "CA1006:DoNotNestGenericTypesInMemberSignatures")]
        public static async Task<Dictionary<TKey, TElement>> ToDictionaryAsync<TSource, TKey, TElement>(
            this IAsyncEnumerable<TSource> source, Func<TSource, TKey> keySelector, Func<TSource, TElement> elementSelector,
            IEqualityComparer<TKey> comparer, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");
            if (keySelector == null) throw new ArgumentNullException("keySelector");
            if (keySelector == null) throw new ArgumentNullException("elementSelector");

            var d = new Dictionary<TKey, TElement>(comparer);
            await
                source.ForEachAsync(element => d.Add(keySelector(element), elementSelector(element)), cancellationToken).ConfigureAwait(
                    continueOnCapturedContext: false);
            return d;
        }


        public static Task<TSource> FirstAsync<TSource>(this IAsyncEnumerable<TSource> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            return source.FirstAsync(CancellationToken.None);
        }

        public static Task<TSource> FirstAsync<TSource>(this IAsyncEnumerable<TSource> source, Func<TSource, bool> predicate)
        {
            if (source == null) throw new ArgumentNullException("source");
            if (predicate == null) throw new ArgumentNullException("predicate");

            return source.FirstAsync(predicate, CancellationToken.None);
        }

        public static async Task<TSource> FirstAsync<TSource>(
            this IAsyncEnumerable<TSource> source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                if (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    return e.Current;
                }
            }

            throw new InvalidOperationException("The sequence is empty");
        }

        public static Task<object> FirstAsync(
            this IAsyncEnumerable source)
        {
            return FirstAsync(source, CancellationToken.None);
        }

        public static async Task<object> FirstAsync(
            this IAsyncEnumerable source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                if (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    return e.Current;
                }
            }

            throw new InvalidOperationException("The sequence is empty");
        }

        public static async Task<TSource> FirstAsync<TSource>(
            this IAsyncEnumerable<TSource> source, Func<TSource, bool> predicate, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");
            if (predicate == null) throw new ArgumentNullException("predicate");

            cancellationToken.ThrowIfCancellationRequested();

            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                if (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    if (predicate(e.Current))
                    {
                        return e.Current;
                    }
                }
            }

            throw new InvalidOperationException("No match was found");
        }

        public static Task<object> FirstOrDefaultAsync(this IAsyncEnumerable source)
        {
            return FirstOrDefaultAsync(source, CancellationToken.None);
        }

        public async static Task<object> FirstOrDefaultAsync(this IAsyncEnumerable source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                if (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    return e.Current;
                }
            }

            return null;
        }

        public static Task<TSource> FirstOrDefaultAsync<TSource>(this IAsyncEnumerable<TSource> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            return source.FirstOrDefaultAsync(CancellationToken.None);
        }

        public static Task<TSource> FirstOrDefaultAsync<TSource>(this IAsyncEnumerable<TSource> source, Func<TSource, bool> predicate)
        {
            if (source == null) throw new ArgumentNullException("source");

            return source.FirstOrDefaultAsync(predicate, CancellationToken.None);
        }

        public static async Task<TSource> FirstOrDefaultAsync<TSource>(
            this IAsyncEnumerable<TSource> source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                if (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    return e.Current;
                }
            }

            return default(TSource);
        }

        public static async Task<TSource> FirstOrDefaultAsync<TSource>(
            this IAsyncEnumerable<TSource> source, Func<TSource, bool> predicate, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                if (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    if (predicate(e.Current))
                    {
                        return e.Current;
                    }
                }
            }

            return default(TSource);
        }

        public static Task<TSource> SingleAsync<TSource>(this IAsyncEnumerable<TSource> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            return source.SingleAsync(CancellationToken.None);
        }

        public static Task<object> SingleAsync(
            this IAsyncEnumerable source)
        {
            return source.SingleAsync(CancellationToken.None);
        }

        public static async Task<object> SingleAsync(
            this IAsyncEnumerable source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                if (!await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    throw new InvalidOperationException("The sequence is empty");
                }

                cancellationToken.ThrowIfCancellationRequested();

                var result = e.Current;

                if (!await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    return result;
                }
            }

            throw new InvalidOperationException("There's more than one element on the sequence");
        }

        public static async Task<TSource> SingleAsync<TSource>(
            this IAsyncEnumerable<TSource> source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                if (!await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    throw new InvalidOperationException("The sequence is empty");
                }

                cancellationToken.ThrowIfCancellationRequested();

                var result = e.Current;

                if (!await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    return result;
                }
            }

            throw new InvalidOperationException("There's more than one element on the sequence");
        }

        public static Task<TSource> SingleAsync<TSource>(
            this IAsyncEnumerable<TSource> source,
            Func<TSource, bool> predicate)
        {
            if (source == null) throw new ArgumentNullException("source");
            if (predicate == null) throw new ArgumentNullException("predicate");

            return source.SingleAsync(predicate, CancellationToken.None);
        }

        public static async Task<TSource> SingleAsync<TSource>(
            this IAsyncEnumerable<TSource> source, Func<TSource, bool> predicate, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");
            if (predicate == null) throw new ArgumentNullException("predicate");

            cancellationToken.ThrowIfCancellationRequested();

            var result = default(TSource);
            long count = 0;
            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    if (predicate(e.Current))
                    {
                        result = e.Current;
                        checked
                        {
                            count++;
                        }
                    }
                }
            }

            switch (count)
            {
                case 0:
                    throw new InvalidOperationException("No match was found");
                case 1:
                    return result;
            }

            throw new InvalidOperationException("More than one match was found");
        }

        public static Task<TSource> SingleOrDefaultAsync<TSource>(this IAsyncEnumerable<TSource> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            return source.SingleOrDefaultAsync(CancellationToken.None);
        }

        public static Task<object> SingleOrDefaultAsync(
            this IAsyncEnumerable source)
        {
            return SingleOrDefaultAsync(source, CancellationToken.None);
        }

        public static async Task<object> SingleOrDefaultAsync(
            this IAsyncEnumerable source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                if (!await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    return null;
                }

                cancellationToken.ThrowIfCancellationRequested();

                var result = e.Current;

                if (!await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    return result;
                }
            }

            throw new InvalidOperationException("More than one element was found");
        }

        public static async Task<TSource> SingleOrDefaultAsync<TSource>(
            this IAsyncEnumerable<TSource> source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                if (!await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    return default(TSource);
                }

                cancellationToken.ThrowIfCancellationRequested();

                var result = e.Current;

                if (!await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    return result;
                }
            }

            throw new InvalidOperationException("More than one element was found");
        }

        public static Task<TSource> SingleOrDefaultAsync<TSource>(
            this IAsyncEnumerable<TSource> source,
            Func<TSource, bool> predicate)
        {
            if (source == null) throw new ArgumentNullException("source");
            if (predicate == null) throw new ArgumentNullException("predicate");

            return source.SingleOrDefaultAsync(predicate, CancellationToken.None);
        }

        public static async Task<TSource> SingleOrDefaultAsync<TSource>(
            this IAsyncEnumerable<TSource> source, Func<TSource, bool> predicate, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");
            if (predicate == null) throw new ArgumentNullException("predicate");

            cancellationToken.ThrowIfCancellationRequested();

            var result = default(TSource);
            long count = 0;
            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    if (predicate(e.Current))
                    {
                        result = e.Current;
                        checked
                        {
                            count++;
                        }
                    }
                }
            }

            if (count < 2)
            {
                return result;
            }

            throw new InvalidOperationException("More than one match was found");
        }

        public static Task<bool> ContainsAsync<TSource>(this IAsyncEnumerable<TSource> source, TSource value)
        {
            if (source == null) throw new ArgumentNullException("source");

            return source.ContainsAsync(value, CancellationToken.None);
        }

        public static async Task<bool> ContainsAsync<TSource>(
            this IAsyncEnumerable<TSource> source, TSource value, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    if (EqualityComparer<TSource>.Default.Equals(e.Current, value))
                    {
                        return true;
                    }

                    cancellationToken.ThrowIfCancellationRequested();
                }
            }

            return false;
        }

        public static Task<bool> AnyAsync<TSource>(this IAsyncEnumerable<TSource> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            return source.AnyAsync(CancellationToken.None);
        }

        public static async Task<bool> AnyAsync<TSource>(this IAsyncEnumerable<TSource> source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                if (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    return true;
                }
            }

            return false;
        }

        public static Task<bool> AnyAsync<TSource>(
            this IAsyncEnumerable<TSource> source, Func<TSource, bool> predicate)
        {
            if (source == null) throw new ArgumentNullException("source");
            if (predicate == null) throw new ArgumentNullException("predicate");

            return source.AnyAsync(predicate, CancellationToken.None);
        }

        public static async Task<bool> AnyAsync<TSource>(
            this IAsyncEnumerable<TSource> source, Func<TSource, bool> predicate, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");
            if (predicate == null) throw new ArgumentNullException("predicate");

            cancellationToken.ThrowIfCancellationRequested();

            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    if (predicate(e.Current))
                    {
                        return true;
                    }

                    cancellationToken.ThrowIfCancellationRequested();
                }
            }

            return false;
        }

        public static Task<bool> AllAsync<TSource>(
            this IAsyncEnumerable<TSource> source, Func<TSource, bool> predicate)
        {
            if (source == null) throw new ArgumentNullException("source");
            if (predicate == null) throw new ArgumentNullException("predicate");

            return source.AllAsync(predicate, CancellationToken.None);
        }

        public static async Task<bool> AllAsync<TSource>(
            this IAsyncEnumerable<TSource> source, Func<TSource, bool> predicate, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");
            if (predicate == null) throw new ArgumentNullException("predicate");

            cancellationToken.ThrowIfCancellationRequested();

            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    if (!predicate(e.Current))
                    {
                        return false;
                    }

                    cancellationToken.ThrowIfCancellationRequested();
                }
            }

            return true;
        }

        public static Task<int> CountAsync<TSource>(this IAsyncEnumerable<TSource> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            return source.CountAsync(CancellationToken.None);
        }

        public static async Task<int> CountAsync<TSource>(this IAsyncEnumerable<TSource> source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            var count = 0;

            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                checked
                {
                    while (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        count++;
                    }
                }
            }

            return count;
        }

        public static Task<int> CountAsync<TSource>(
            this IAsyncEnumerable<TSource> source, Func<TSource, bool> predicate)
        {
            if (source == null) throw new ArgumentNullException("source");
            if (predicate == null) throw new ArgumentNullException("predicate");

            return source.CountAsync(predicate, CancellationToken.None);
        }

        public static async Task<int> CountAsync<TSource>(
            this IAsyncEnumerable<TSource> source, Func<TSource, bool> predicate, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");
            if (predicate == null) throw new ArgumentNullException("predicate");

            cancellationToken.ThrowIfCancellationRequested();

            var count = 0;

            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                checked
                {
                    while (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        if (predicate(e.Current))
                        {
                            count++;
                        }
                    }
                }
            }

            return count;
        }

        public static Task<long> LongCountAsync<TSource>(this IAsyncEnumerable<TSource> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            return source.LongCountAsync(CancellationToken.None);
        }

        public static async Task<long> LongCountAsync<TSource>(
            this IAsyncEnumerable<TSource> source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            long count = 0;

            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                checked
                {
                    while (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        count++;
                    }
                }
            }

            return count;
        }

        public static Task<long> LongCountAsync<TSource>(
            this IAsyncEnumerable<TSource> source, Func<TSource, bool> predicate)
        {
            if (source == null) throw new ArgumentNullException("source");
            if (predicate == null) throw new ArgumentNullException("predicate");

            return source.LongCountAsync(predicate, CancellationToken.None);
        }

        public static async Task<long> LongCountAsync<TSource>(
            this IAsyncEnumerable<TSource> source, Func<TSource, bool> predicate, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");
            if (predicate == null) throw new ArgumentNullException("predicate");

            cancellationToken.ThrowIfCancellationRequested();

            long count = 0;

            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                checked
                {
                    while (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        if (predicate(e.Current))
                        {
                            count++;
                        }
                    }
                }
            }

            return count;
        }

        public static Task<TSource> MinAsync<TSource>(this IAsyncEnumerable<TSource> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            return source.MinAsync(CancellationToken.None);
        }

        public static async Task<TSource> MinAsync<TSource>(this IAsyncEnumerable<TSource> source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            var comparer = Comparer<TSource>.Default;
            var value = default(TSource);
            if (value == null)
            {
                using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
                {
                    while (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        if (e.Current != null
                            && (value == null || comparer.Compare(e.Current, value) < 0))
                        {
                            value = e.Current;
                        }
                    }
                }

                return value;
            }
            else
            {
                var hasValue = false;

                using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
                {
                    while (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        if (hasValue)
                        {
                            if (comparer.Compare(e.Current, value) < 0)
                            {
                                value = e.Current;
                            }
                        }
                        else
                        {
                            value = e.Current;
                            hasValue = true;
                        }
                    }
                }

                if (hasValue)
                {
                    return value;
                }
                throw new InvalidOperationException("The sequence was empty");
            }
        }

        public static Task<TSource> MaxAsync<TSource>(this IAsyncEnumerable<TSource> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            return source.MaxAsync(CancellationToken.None);
        }

        public static async Task<TSource> MaxAsync<TSource>(this IAsyncEnumerable<TSource> source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            var comparer = Comparer<TSource>.Default;
            var value = default(TSource);
            if (value == null)
            {
                using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
                {
                    while (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        if (e.Current != null
                            && (value == null || comparer.Compare(e.Current, value) > 0))
                        {
                            value = e.Current;
                        }
                    }
                }

                return value;
            }
            else
            {
                var hasValue = false;

                using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
                {
                    while (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        if (hasValue)
                        {
                            if (comparer.Compare(e.Current, value) > 0)
                            {
                                value = e.Current;
                            }
                        }
                        else
                        {
                            value = e.Current;
                            hasValue = true;
                        }
                    }
                }

                if (hasValue)
                {
                    return value;
                }
                throw new InvalidOperationException("The sequence was empty");
            }
        }

        public static Task<int> SumAsync(this IAsyncEnumerable<int> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            return source.SumAsync(CancellationToken.None);
        }

        public static async Task<int> SumAsync(this IAsyncEnumerable<int> source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            long sum = 0;
            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    checked
                    {
                        sum += e.Current;
                    }
                }
            }

            return (int)sum;
        }

        public static Task<int?> SumAsync(this IAsyncEnumerable<int?> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            return source.SumAsync(CancellationToken.None);
        }

        public static async Task<int?> SumAsync(this IAsyncEnumerable<int?> source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            long sum = 0;
            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    checked
                    {
                        if (e.Current.HasValue)
                        {
                            sum += e.Current.GetValueOrDefault();
                        }
                    }
                }
            }

            return (int)sum;
        }

        public static Task<long> SumAsync(this IAsyncEnumerable<long> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            return source.SumAsync(CancellationToken.None);
        }

        public static async Task<long> SumAsync(this IAsyncEnumerable<long> source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            long sum = 0;
            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    checked
                    {
                        sum += e.Current;
                    }
                }
            }

            return sum;
        }

        public static Task<long?> SumAsync(this IAsyncEnumerable<long?> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            return source.SumAsync(CancellationToken.None);
        }

        public static async Task<long?> SumAsync(this IAsyncEnumerable<long?> source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            long sum = 0;
            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    checked
                    {
                        if (e.Current.HasValue)
                        {
                            sum += e.Current.GetValueOrDefault();
                        }
                    }
                }
            }

            return sum;
        }

        public static Task<float> SumAsync(this IAsyncEnumerable<float> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            return source.SumAsync(CancellationToken.None);
        }

        public static async Task<float> SumAsync(this IAsyncEnumerable<float> source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            double sum = 0;
            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    checked
                    {
                        sum += e.Current;
                    }
                }
            }

            return (float)sum;
        }

        public static Task<float?> SumAsync(this IAsyncEnumerable<float?> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            return source.SumAsync(CancellationToken.None);
        }

        public static async Task<float?> SumAsync(this IAsyncEnumerable<float?> source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            double sum = 0;
            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    checked
                    {
                        if (e.Current.HasValue)
                        {
                            sum += e.Current.GetValueOrDefault();
                        }
                    }
                }
            }

            return (float)sum;
        }

        public static Task<double> SumAsync(this IAsyncEnumerable<double> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            return source.SumAsync(CancellationToken.None);
        }

        public static async Task<double> SumAsync(this IAsyncEnumerable<double> source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            double sum = 0;
            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    checked
                    {
                        sum += e.Current;
                    }
                }
            }

            return sum;
        }

        public static Task<double?> SumAsync(this IAsyncEnumerable<double?> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            return source.SumAsync(CancellationToken.None);
        }

        public static async Task<double?> SumAsync(this IAsyncEnumerable<double?> source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            double sum = 0;
            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    checked
                    {
                        if (e.Current.HasValue)
                        {
                            sum += e.Current.GetValueOrDefault();
                        }
                    }
                }
            }

            return sum;
        }

        public static Task<decimal> SumAsync(this IAsyncEnumerable<decimal> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            return source.SumAsync(CancellationToken.None);
        }

        public static async Task<decimal> SumAsync(this IAsyncEnumerable<decimal> source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            decimal sum = 0;
            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    checked
                    {
                        sum += e.Current;
                    }
                }
            }

            return sum;
        }

        public static Task<decimal?> SumAsync(this IAsyncEnumerable<decimal?> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            return source.SumAsync(CancellationToken.None);
        }

        public static async Task<decimal?> SumAsync(this IAsyncEnumerable<decimal?> source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            decimal sum = 0;
            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    checked
                    {
                        if (e.Current.HasValue)
                        {
                            sum += e.Current.GetValueOrDefault();
                        }
                    }
                }
            }

            return sum;
        }

        public static Task<double> AverageAsync(this IAsyncEnumerable<int> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            return source.AverageAsync(CancellationToken.None);
        }

        public static async Task<double> AverageAsync(this IAsyncEnumerable<int> source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            long sum = 0;
            long count = 0;
            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    checked
                    {
                        sum += e.Current;
                        count++;
                    }
                }
            }

            if (count > 0)
            {
                return (double)sum / count;
            }
            throw new InvalidOperationException("The sequence was empty");
        }

        public static Task<double?> AverageAsync(this IAsyncEnumerable<int?> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            return source.AverageAsync(CancellationToken.None);
        }

        public static async Task<double?> AverageAsync(this IAsyncEnumerable<int?> source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            long sum = 0;
            long count = 0;
            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    checked
                    {
                        if (e.Current.HasValue)
                        {
                            sum += e.Current.GetValueOrDefault();
                            count++;
                        }
                    }
                }
            }

            if (count > 0)
            {
                return (double)sum / count;
            }
            throw new InvalidOperationException("The sequence was empty");
        }

        public static Task<double> AverageAsync(this IAsyncEnumerable<long> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            return source.AverageAsync(CancellationToken.None);
        }

        public static async Task<double> AverageAsync(this IAsyncEnumerable<long> source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            long sum = 0;
            long count = 0;
            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    checked
                    {
                        sum += e.Current;
                        count++;
                    }
                }
            }

            if (count > 0)
            {
                return (double)sum / count;
            }
            throw new InvalidOperationException("The sequence was empty");
        }

        public static Task<double?> AverageAsync(this IAsyncEnumerable<long?> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            return source.AverageAsync(CancellationToken.None);
        }

        public static async Task<double?> AverageAsync(this IAsyncEnumerable<long?> source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            long sum = 0;
            long count = 0;
            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    checked
                    {
                        if (e.Current.HasValue)
                        {
                            sum += e.Current.GetValueOrDefault();
                            count++;
                        }
                    }
                }
            }

            if (count > 0)
            {
                return (double)sum / count;
            }
            throw new InvalidOperationException("The sequence was empty");
        }

        public static Task<float> AverageAsync(this IAsyncEnumerable<float> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            return source.AverageAsync(CancellationToken.None);
        }

        public static async Task<float> AverageAsync(this IAsyncEnumerable<float> source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            double sum = 0;
            long count = 0;
            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    checked
                    {
                        sum += e.Current;
                        count++;
                    }
                }
            }

            if (count > 0)
            {
                return (float)(sum / count);
            }
            throw new InvalidOperationException("The sequence was empty");
        }

        public static Task<float?> AverageAsync(this IAsyncEnumerable<float?> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            return source.AverageAsync(CancellationToken.None);
        }

        public static async Task<float?> AverageAsync(this IAsyncEnumerable<float?> source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            double sum = 0;
            long count = 0;
            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    checked
                    {
                        if (e.Current.HasValue)
                        {
                            sum += e.Current.GetValueOrDefault();
                            count++;
                        }
                    }
                }
            }

            if (count > 0)
            {
                return (float)(sum / count);
            }
            throw new InvalidOperationException("The sequence was empty");
        }

        public static Task<double> AverageAsync(this IAsyncEnumerable<double> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            return source.AverageAsync(CancellationToken.None);
        }

        public static async Task<double> AverageAsync(this IAsyncEnumerable<double> source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            double sum = 0;
            long count = 0;
            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    checked
                    {
                        sum += e.Current;
                        count++;
                    }
                }
            }

            if (count > 0)
            {
                return (float)(sum / count);
            }
            throw new InvalidOperationException("The sequence was empty");
        }

        public static Task<double?> AverageAsync(this IAsyncEnumerable<double?> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            return source.AverageAsync(CancellationToken.None);
        }

        public static async Task<double?> AverageAsync(this IAsyncEnumerable<double?> source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            double sum = 0;
            long count = 0;
            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    checked
                    {
                        if (e.Current.HasValue)
                        {
                            sum += e.Current.GetValueOrDefault();
                            count++;
                        }
                    }
                }
            }

            if (count > 0)
            {
                return (float)(sum / count);
            }
            throw new InvalidOperationException("The sequence was empty");
        }

        public static Task<decimal> AverageAsync(this IAsyncEnumerable<decimal> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            return source.AverageAsync(CancellationToken.None);
        }

        public static async Task<decimal> AverageAsync(this IAsyncEnumerable<decimal> source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            decimal sum = 0;
            long count = 0;
            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    checked
                    {
                        sum += e.Current;
                        count++;
                    }
                }
            }

            if (count > 0)
            {
                return sum / count;
            }
            throw new InvalidOperationException("The sequence was empty");
        }

        public static Task<decimal?> AverageAsync(this IAsyncEnumerable<decimal?> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            return source.AverageAsync(CancellationToken.None);
        }

        public static async Task<decimal?> AverageAsync(this IAsyncEnumerable<decimal?> source, CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException("source");

            cancellationToken.ThrowIfCancellationRequested();

            decimal sum = 0;
            long count = 0;
            using (var e = await source.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await e.MoveNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    checked
                    {
                        if (e.Current.HasValue)
                        {
                            sum += e.Current.GetValueOrDefault();
                            count++;
                        }
                    }
                }
            }

            if (count > 0)
            {
                return sum / count;
            }
            throw new InvalidOperationException("The sequence was empty");
        }

        #region Nested classes

        private static class IdentityFunction<TElement>
        {
            public static Func<TElement, TElement> Instance
            {
                get { return x => x; }
            }
        }

        #endregion
    }
}
