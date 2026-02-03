using FrozenArrow.Query;

namespace FrozenArrow.Profiling.Scenarios;

/// <summary>
/// Profiles GroupBy operations with aggregations.
/// </summary>
public sealed class GroupByScenario : BaseScenario
{
    public override string Name => "GroupBy";
    public override string Description => "GroupBy + aggregations";

    private int _groupCount;
    private double _totalSalary;

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

        // GroupBy DepartmentId and count
        var countByDept = query
            .GroupBy(x => x.DepartmentId)
            .Select(g => new { DepartmentId = g.Key, Count = g.Count() })
            .ToList();

        _groupCount = countByDept.Count;

        // GroupBy DepartmentId with Sum
        var salaryByDept = query
            .GroupBy(x => x.DepartmentId)
            .Select(g => new { DepartmentId = g.Key, TotalSalary = g.Sum(x => x.Salary) })
            .ToList();

        _totalSalary = salaryByDept.Sum(x => x.TotalSalary);

        return _groupCount + _totalSalary;
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

        StartPhase("GroupByCount");
        var countByDept = query
            .GroupBy(x => x.DepartmentId)
            .Select(g => new { DepartmentId = g.Key, Count = g.Count() })
            .ToList();
        _groupCount = countByDept.Count;
        EndPhase("GroupByCount");

        StartPhase("GroupBySum");
        var salaryByDept = query
            .GroupBy(x => x.DepartmentId)
            .Select(g => new { DepartmentId = g.Key, TotalSalary = g.Sum(x => x.Salary) })
            .ToList();
        _totalSalary = salaryByDept.Sum(x => x.TotalSalary);
        EndPhase("GroupBySum");

        return (_groupCount + _totalSalary, new Dictionary<string, double>(CurrentPhases));
    }

    public override Dictionary<string, string> GetMetadata() => new()
    {
        ["GroupCount"] = $"{_groupCount} groups",
        ["TotalSalary"] = $"{_totalSalary:N2}"
    };
}
