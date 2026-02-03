using FrozenArrow.Query;

namespace FrozenArrow.Profiling.Scenarios;

/// <summary>
/// Profiles result enumeration and materialization.
/// </summary>
public sealed class EnumerationScenario : BaseScenario
{
    public override string Name => "Enumeration";
    public override string Description => "Result materialization (ToList, foreach)";

    private int _toListCount;
    private int _foreachCount;
    private int _firstResult;

    public override object? RunIteration()
    {
        var query = Data.AsQueryable();
        if (!Config.EnableParallel)
        {
            ((ArrowQueryProvider)query.Provider).ParallelOptions = new ParallelQueryOptions
            {
                EnableParallelExecution = false
            };
        }

        // ToList with filter (~30% of rows)
        var list = query.Where(x => x.Age > 40).ToList();
        _toListCount = list.Count;

        // Foreach enumeration
        _foreachCount = 0;
        foreach (var item in query.Where(x => x.Age > 50))
        {
            _foreachCount++;
        }

        // First element
        _firstResult = query.Where(x => x.Age > 25).First().Id;

        return _toListCount + _foreachCount + _firstResult;
    }

    public override (object? Result, Dictionary<string, double> PhaseMicroseconds) RunIterationWithPhases()
    {
        CurrentPhases.Clear();
        var query = Data.AsQueryable();
        if (!Config.EnableParallel)
        {
            ((ArrowQueryProvider)query.Provider).ParallelOptions = new ParallelQueryOptions
            {
                EnableParallelExecution = false
            };
        }

        StartPhase("ToList");
        var list = query.Where(x => x.Age > 40).ToList();
        _toListCount = list.Count;
        EndPhase("ToList");

        StartPhase("Foreach");
        _foreachCount = 0;
        foreach (var item in query.Where(x => x.Age > 50))
        {
            _foreachCount++;
        }
        EndPhase("Foreach");

        StartPhase("First");
        _firstResult = query.Where(x => x.Age > 25).First().Id;
        EndPhase("First");

        return (_toListCount + _foreachCount + _firstResult, new Dictionary<string, double>(CurrentPhases));
    }

    public override Dictionary<string, string> GetMetadata() => new()
    {
        ["ToListCount"] = $"{_toListCount:N0} items",
        ["ForeachCount"] = $"{_foreachCount:N0} items",
        ["FirstId"] = $"{_firstResult}"
    };
}
