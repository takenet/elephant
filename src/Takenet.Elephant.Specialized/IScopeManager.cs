using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized
{
    public interface IScope
    {
        string Name { get; }

        Task DisposeAsync();
    }
}
