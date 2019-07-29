using System;
using System.Collections.Generic;
using System.Text;

namespace Take.Elephant.ElasticSearch
{
    //string host, string username, string password, string defaultIndex
    public interface IElasticsearchConfiguration
    {
        string Hostname { get; }
        string Username { get; }
        string Password { get; }
        string DefaultIndex { get; }

    }
}
