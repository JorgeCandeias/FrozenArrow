using FrozenArrow.Query;

namespace FrozenArrow.Profiling.Scenarios;

/// <summary>
/// Profiles fused execution (filter + aggregate in single pass).
/// Compares fused vs non-fused execution paths.
/// </summary>
public sealed class FusedExecutionScenario : BaseScenario
{
    public override string Name => "FusedExecution";
    public override string Description => "Fused filter+aggregate vs separate passes";

    private double _fusedResult;
    private int _matchCount;

    public override object? RunIteration()
    {
        // Fused path: Where + Sum (enabled by default for large datasets)
        var query1 = Data.AsQueryable();
        ((ArrowQueryProvider)query1.Provider).ParallelOptions = new ParallelQueryOptions
        {
            EnableParallelExecution = Config.EnableParallel
        };
        _fusedResult = query1.Where(x => x.Age > 30 && x.IsActive).Sum(x => x.Salary);

        // For comparison: filter + count
        var query2 = Data.AsQueryable();
        ((ArrowQueryProvider)query2.Provider).ParallelOptions = new ParallelQueryOptions
        {
            EnableParallelExecution = Config.EnableParallel
        };
        _matchCount = query2.Where(x => x.Age > 30 && x.IsActive).Count();

        return _fusedResult + _matchCount;
    }

    public override (object? Result, Dictionary<string, double> PhaseMicroseconds) RunIterationWithPhases()
    {
        CurrentPhases.Clear();

        StartPhase("FusedFilterSum");
        var query1 = Data.AsQueryable();
        ((ArrowQueryProvider)query1.Provider).ParallelOptions = new ParallelQueryOptions
        {
            EnableParallelExecution = Config.EnableParallel
        };
        _fusedResult = query1.Where(x => x.Age > 30 && x.IsActive).Sum(x => x.Salary);
        EndPhase("FusedFilterSum");

        StartPhase("FilterCount");
        var query2 = Data.AsQueryable();
        ((ArrowQueryProvider)query2.Provider).ParallelOptions = new ParallelQueryOptions
        {
            EnableParallelExecution = Config.EnableParallel
        };
        _matchCount = query2.Where(x => x.Age > 30 && x.IsActive).Count();
        EndPhase("FilterCount");

        return (_fusedResult + _matchCount, new Dictionary<string, double>(CurrentPhases));
    }

    public override Dictionary<string, string> GetMetadata() => new()
    {
        ["FusedSum"] = $"{_fusedResult:N2}",
        ["MatchCount"] = $"{_matchCount:N0} rows matched"
    };
}
