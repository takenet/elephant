using System.Threading.Tasks;

namespace Takenet.Elephant
{
    public class TaskUtil
    {
        /// <summary>
        /// Provides a completed task.
        /// </summary>
        public readonly static Task CompletedTask = Task.FromResult<object>(null);
    }
}
