using FrozenArrow.Query;

namespace FrozenArrow.Profiling.Scenarios;

/// <summary>
/// Compares parallel vs sequential execution for various operations.
/// </summary>
public sealed class ParallelComparisonScenario : BaseScenario
{
    public override string Name => "ParallelComparison";
    public override string Description => "Sequential vs parallel execution";

    private double _sequentialTime;
    private double _parallelTime;
    private double _result;

    public override object? RunIteration()
    {
        // Sequential: filter + sum
        var querySeq = Data.AsQueryable();
        ((ArrowQueryProvider)querySeq.Provider).ParallelOptions = new ParallelQueryOptions
        {
            EnableParallelExecution = false
        };
        var sw = System.Diagnostics.Stopwatch.StartNew();
        _result = querySeq.Where(x => x.Age > 25 && x.IsActive).Sum(x => x.Salary);
        sw.Stop();
        _sequentialTime = sw.Elapsed.TotalMicroseconds;

        // Parallel: same query
        var queryPar = Data.AsQueryable();
        ((ArrowQueryProvider)queryPar.Provider).ParallelOptions = new ParallelQueryOptions
        {
            EnableParallelExecution = true,
            ParallelThreshold = 1000 // Force parallel
        };
        sw.Restart();
        var parallelResult = queryPar.Where(x => x.Age > 25 && x.IsActive).Sum(x => x.Salary);
        sw.Stop();
        _parallelTime = sw.Elapsed.TotalMicroseconds;

        return _result + parallelResult;
    }

    public override (object? Result, Dictionary<string, double> PhaseMicroseconds) RunIterationWithPhases()
    {
        CurrentPhases.Clear();

        StartPhase("Sequential");
        var querySeq = Data.AsQueryable();
        ((ArrowQueryProvider)querySeq.Provider).ParallelOptions = new ParallelQueryOptions
        {
            EnableParallelExecution = false
        };
        _result = querySeq.Where(x => x.Age > 25 && x.IsActive).Sum(x => x.Salary);
        EndPhase("Sequential");
        _sequentialTime = CurrentPhases["Sequential"];

        StartPhase("Parallel");
        var queryPar = Data.AsQueryable();
        ((ArrowQueryProvider)queryPar.Provider).ParallelOptions = new ParallelQueryOptions
        {
            EnableParallelExecution = true,
            ParallelThreshold = 1000
        };
        var parallelResult = queryPar.Where(x => x.Age > 25 && x.IsActive).Sum(x => x.Salary);
        EndPhase("Parallel");
        _parallelTime = CurrentPhases["Parallel"];

        return (_result + parallelResult, new Dictionary<string, double>(CurrentPhases));
    }

    public override Dictionary<string, string> GetMetadata()
    {
        var speedup = _sequentialTime / _parallelTime;
        return new()
        {
            ["SequentialTime"] = $"{_sequentialTime:F1} ?s",
            ["ParallelTime"] = $"{_parallelTime:F1} ?s",
            ["Speedup"] = $"{speedup:F2}x",
            ["ProcessorCount"] = $"{Environment.ProcessorCount} cores"
        };
    }
}
