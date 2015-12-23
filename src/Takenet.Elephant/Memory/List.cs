using System;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Takenet.Elephant.Memory
{
    /// <summary>
    /// Implements the <see cref="ILIst{T}"/> interface using the <see cref="System.Collections.Generic.List{T}"/> class.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class List<T> : Collection<T>, IList<T>
    {
        private readonly System.Collections.Generic.List<T> _list;

        public List()
            : this(new System.Collections.Generic.List<T>())
        {
            
        }

        private List(System.Collections.Generic.List<T> list)
            : base(list)
        {
            _list = list;
        }

        public Task AddAsync(T value)
        {
            _list.Add(value);
            return TaskUtil.CompletedTask;
        }

        public Task<long> RemoveAllAsync(T value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            return Task.FromResult(_list.RemoveAll(i => i.Equals(value))).ContinueWith(t => (long)t.Result);
        }
    }
}
