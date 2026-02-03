using FrozenArrow.Query;

namespace FrozenArrow.Profiling.Scenarios;

/// <summary>
/// Profiles predicate evaluation (SIMD vs scalar paths).
/// </summary>
public sealed class PredicateEvaluationScenario : BaseScenario
{
    public override string Name => "PredicateEvaluation";
    public override string Description => "Predicate evaluation with SIMD optimization";

    private int _int32Count;
    private int _doubleCount;
    private int _boolCount;
    private int _multiCount;

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

        // Int32 comparison (uses SIMD)
        _int32Count = query.Where(x => x.Age > 40).Count();

        // Double comparison (uses SIMD)
        _doubleCount = query.Where(x => x.Salary > 80000).Count();

        // Boolean check
        _boolCount = query.Where(x => x.IsActive).Count();

        // Multiple predicates
        _multiCount = query.Where(x => x.Age > 30 && x.Salary > 60000 && x.IsActive).Count();

        return _int32Count + _doubleCount + _boolCount + _multiCount;
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

        StartPhase("Int32Predicate");
        _int32Count = query.Where(x => x.Age > 40).Count();
        EndPhase("Int32Predicate");

        StartPhase("DoublePredicate");
        _doubleCount = query.Where(x => x.Salary > 80000).Count();
        EndPhase("DoublePredicate");

        StartPhase("BoolPredicate");
        _boolCount = query.Where(x => x.IsActive).Count();
        EndPhase("BoolPredicate");

        StartPhase("MultiPredicate");
        _multiCount = query.Where(x => x.Age > 30 && x.Salary > 60000 && x.IsActive).Count();
        EndPhase("MultiPredicate");

        return (_int32Count + _doubleCount + _boolCount + _multiCount, new Dictionary<string, double>(CurrentPhases));
    }

    public override Dictionary<string, string> GetMetadata() => new()
    {
        ["Int32Match"] = $"{_int32Count:N0} ({_int32Count * 100.0 / Config.RowCount:F1}%)",
        ["DoubleMatch"] = $"{_doubleCount:N0} ({_doubleCount * 100.0 / Config.RowCount:F1}%)",
        ["BoolMatch"] = $"{_boolCount:N0} ({_boolCount * 100.0 / Config.RowCount:F1}%)",
        ["MultiMatch"] = $"{_multiCount:N0} ({_multiCount * 100.0 / Config.RowCount:F1}%)"
    };
}
