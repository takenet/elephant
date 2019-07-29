using System;
using System.Collections.Generic;
using System.Text;

namespace Take.Elephant.Elasticsearch
{
    public class ElasticsearchConfigurations : IElasticsearchConfiguration
    {
        /// <summary>
        /// Elasticsearch hostname
        /// </summary>
        public string Hostname { get; set; }

        /// <summary>
        /// Elasticsearch username
        /// </summary>
        public string Username { get; set; }

        /// <summary>
        /// Elasticsearch password
        /// </summary>
        public string Password { get; set; }

        /// <summary>
        /// Elasticsearch default index
        /// </summary>
        public string DefaultIndex { get; set; }
    }
}
