using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;
using System;
using Take.Elephant.Benchmark.Kafka;

namespace Take.Elephant.Benchmark
{
    class Program
    {
        static void Main(string[] args) {
            BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
        }
    }

}
