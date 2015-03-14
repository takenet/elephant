using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence
{
    public class TaskUtil
    {
        /// <summary>
        /// Provides a completed task.
        /// </summary>
        public readonly static Task CompletedTask = Task.FromResult<object>(null);
    }
}
