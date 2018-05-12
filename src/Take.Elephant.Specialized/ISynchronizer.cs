using System.Threading.Tasks;

namespace Take.Elephant.Specialized
{
    public interface ISynchronizer<in T>
    {
        Task SynchronizeAsync(T source, T target);
    }
}