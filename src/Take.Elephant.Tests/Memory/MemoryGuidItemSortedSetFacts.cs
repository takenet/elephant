﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Take.Elephant.Tests.Memory
{
    public class MemoryGuidItemSortedSetFacts : GuidItemSortedSetFacts
    {
        public override ISortedSet<Guid> Create()
        {
            return new Elephant.Memory.SortedSet<Guid>();
        }
    }
}
