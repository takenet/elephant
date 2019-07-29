using System;
using System.Collections.Generic;
using System.Text;

namespace Take.Elephant.ElasticSearch
{
    public class ElasticSearchConfigurations : IElasticSearchConfiguration
    {
        public string Hostname { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public string DefaultIndex { get; set; }
    }
}
