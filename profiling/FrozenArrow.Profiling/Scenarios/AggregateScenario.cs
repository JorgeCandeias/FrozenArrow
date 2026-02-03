using FrozenArrow.Query;

namespace FrozenArrow.Profiling.Scenarios;

/// <summary>
/// Profiles aggregate operations (Sum, Average, Min, Max).
/// </summary>
public sealed class AggregateScenario : BaseScenario
{
    public override string Name => "Aggregate";
    public override string Description => "Sum, Average, Min, Max aggregations";

    private double _sumResult;
    private double _avgResult;
    private double _minResult;
    private double _maxResult;

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

        // Sum
        _sumResult = query.Sum(x => x.Salary);

        // Average
        _avgResult = query.Average(x => x.Salary);

        // Min
        _minResult = query.Min(x => x.Salary);

        // Max
        _maxResult = query.Max(x => x.Salary);

        return _sumResult + _avgResult + _minResult + _maxResult;
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

        StartPhase("Sum");
        _sumResult = query.Sum(x => x.Salary);
        EndPhase("Sum");

        StartPhase("Average");
        _avgResult = query.Average(x => x.Salary);
        EndPhase("Average");

        StartPhase("Min");
        _minResult = query.Min(x => x.Salary);
        EndPhase("Min");

        StartPhase("Max");
        _maxResult = query.Max(x => x.Salary);
        EndPhase("Max");

        return (_sumResult + _avgResult + _minResult + _maxResult, new Dictionary<string, double>(CurrentPhases));
    }

    public override Dictionary<string, string> GetMetadata() => new()
    {
        ["Sum"] = $"{_sumResult:N2}",
        ["Average"] = $"{_avgResult:N2}",
        ["Min"] = $"{_minResult:N2}",
        ["Max"] = $"{_maxResult:N2}"
    };
}
