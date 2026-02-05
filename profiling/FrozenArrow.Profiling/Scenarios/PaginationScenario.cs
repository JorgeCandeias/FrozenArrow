using FrozenArrow.Query;

namespace FrozenArrow.Profiling.Scenarios;

/// <summary>
/// Profiling scenario for pagination operations (Take, Skip, Skip+Take).
/// Tests the performance of limiting result sets for pagination use cases.
/// </summary>
public class PaginationScenario : BaseScenario
{
    public override string Name => "Pagination";
    public override string Description => "Take/Skip operations for result set pagination";

    public override object? RunIteration()
    {
        // Scenario 1: Take only - Limit to first 1000 results
        StartPhase("Take Only");
        var takeResults = Data.AsQueryable()
            .Where(x => x.Age > 25)
            .Take(1000)
            .ToList();
        EndPhase("Take Only");

        // Scenario 2: Skip only - Skip first 10000 results
        StartPhase("Skip Only");
        var skipResults = Data.AsQueryable()
            .Where(x => x.Age > 25)
            .Skip(10000)
            .ToList();
        EndPhase("Skip Only");

        // Scenario 3: Skip + Take - Classic pagination (page 100, size 100)
        StartPhase("Skip + Take");
        var pageResults = Data.AsQueryable()
            .Where(x => x.Age > 25)
            .Skip(10000)
            .Take(100)
            .ToList();
        EndPhase("Skip + Take");

        // Scenario 4: Large Skip - Deep pagination
        StartPhase("Large Skip");
        var deepPageResults = Data.AsQueryable()
            .Where(x => x.Age > 25)
            .Skip(50000)
            .Take(10)
            .ToList();
        EndPhase("Large Skip");

        // Scenario 5: Take with Count - Limit then count
        StartPhase("Take with Count");
        var takeCount = Data.AsQueryable()
            .Where(x => x.IsActive)
            .Take(5000)
            .Count();
        EndPhase("Take with Count");

        // Scenario 6: Skip + Take with Count - Page count
        StartPhase("Skip + Take with Count");
        var pageCount = Data.AsQueryable()
            .Where(x => x.IsActive)
            .Skip(10000)
            .Take(100)
            .Count();
        EndPhase("Skip + Take with Count");

        // Scenario 7: Take with First - Early termination
        StartPhase("Take with First");
        var firstItem = Data.AsQueryable()
            .Where(x => x.Age > 50)
            .Take(100)
            .First();
        EndPhase("Take with First");

        // Scenario 8: Skip with First - Skip then get first
        StartPhase("Skip with First");
        var skippedFirst = Data.AsQueryable()
            .Where(x => x.Age > 30)
            .Skip(1000)
            .First();
        EndPhase("Skip with First");

        // Scenario 9: Highly selective Take - Take from small filtered set
        StartPhase("Highly Selective Take");
        var selectiveTake = Data.AsQueryable()
            .Where(x => x.Age > 70)
            .Take(10)
            .ToList();
        EndPhase("Highly Selective Take");

        // Scenario 10: No results Take - Take from empty result set
        StartPhase("Empty Results Take");
        var emptyTake = Data.AsQueryable()
            .Where(x => x.Age > 200)
            .Take(100)
            .ToList();
        EndPhase("Empty Results Take");

        // Sanity checks
        if (takeResults.Count != 1000)
            throw new InvalidOperationException($"Expected 1000 Take results, got {takeResults.Count}");
        if (pageResults.Count != 100)
            throw new InvalidOperationException($"Expected 100 page results, got {pageResults.Count}");
        if (deepPageResults.Count > 10)
            throw new InvalidOperationException($"Expected at most 10 deep page results, got {deepPageResults.Count}");
        if (takeCount > 5000)
            throw new InvalidOperationException($"Take count should not exceed 5000, got {takeCount}");
        if (pageCount > 100)
            throw new InvalidOperationException($"Page count should not exceed 100, got {pageCount}");
        if (emptyTake.Count != 0)
            throw new InvalidOperationException($"Expected 0 empty results, got {emptyTake.Count}");

        return takeResults.Count + skipResults.Count + pageResults.Count;
    }
}
