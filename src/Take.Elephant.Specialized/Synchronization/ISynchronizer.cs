using System.Threading.Tasks;

namespace Take.Elephant.Specialized.Synchronization
{
    public interface ISynchronizer<in T>
    {
        Task SynchronizeAsync(T source, T target);
    }
}