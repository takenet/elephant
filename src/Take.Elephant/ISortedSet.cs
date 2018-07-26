using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Take.Elephant
{
    /// <summary>
    /// Defines a set of unique items, associated with scores, that is used to take the sorted set ordered, 
    /// from the smallest to the greatest score. 
    /// The items are unique, scores may be repeated.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface ISortedSet<T> : IPriorityBlockingQueue<T>, ICollection<T>
    {}
}
