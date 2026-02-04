using FrozenArrow.Query;
using System.Diagnostics;

namespace FrozenArrow.Profiling.Scenarios;

/// <summary>
/// Profiles pooled materialization optimizations including ArrayPool usage
/// and zero-allocation index-based access patterns.
/// </summary>
public sealed class PooledMaterializationScenario : BaseScenario
{
    public override string Name => "PooledMaterialization";
    public override string Description => "Pooled vs standard materialization comparison";

    private int _arrayPooledCount;
    private int _toListCount;
    private int _getIndicesCount;
    private long _arrayPooledAlloc;
    private long _toListAlloc;
    private long _getIndicesAlloc;

    public override object? RunIteration()
    {
        var query = Data.AsQueryable().Where(x => x.Age > 40);
        
        // Test 1: ToArrayPooled (new optimized path)
        var arrayPooled = query.ToArrayPooled();
        _arrayPooledCount = arrayPooled.Length;

        // Test 2: Traditional ToList
        var list = query.ToList();
        _toListCount = list.Count;

        // Test 3: Zero-allocation GetIndices
        var indices = query.GetIndices();
        _getIndicesCount = indices.Length;

        return _arrayPooledCount + _toListCount + _getIndicesCount;
    }

    public override (object? Result, Dictionary<string, double> PhaseMicroseconds) RunIterationWithPhases()
    {
        CurrentPhases.Clear();
        var query = Data.AsQueryable().Where(x => x.Age > 40);

        // Phase 1: ToArrayPooled (optimized)
        StartPhase("ToArrayPooled");
        var gcBefore1 = GC.GetTotalAllocatedBytes(precise: true);
        var arrayPooled = query.ToArrayPooled();
        _arrayPooledAlloc = GC.GetTotalAllocatedBytes(precise: true) - gcBefore1;
        _arrayPooledCount = arrayPooled.Length;
        EndPhase("ToArrayPooled");

        // Phase 2: Traditional ToList
        StartPhase("ToList");
        var gcBefore2 = GC.GetTotalAllocatedBytes(precise: true);
        var list = query.ToList();
        _toListAlloc = GC.GetTotalAllocatedBytes(precise: true) - gcBefore2;
        _toListCount = list.Count;
        EndPhase("ToList");

        // Phase 3: Zero-allocation GetIndices
        StartPhase("GetIndices");
        var gcBefore3 = GC.GetTotalAllocatedBytes(precise: true);
        var indices = query.GetIndices();
        _getIndicesAlloc = GC.GetTotalAllocatedBytes(precise: true) - gcBefore3;
        _getIndicesCount = indices.Length;
        EndPhase("GetIndices");

        return (_arrayPooledCount + _toListCount + _getIndicesCount, 
                new Dictionary<string, double>(CurrentPhases));
    }

    public override Dictionary<string, string> GetMetadata() => new()
    {
        ["ToArrayPooled_Count"] = $"{_arrayPooledCount:N0} items",
        ["ToArrayPooled_Allocated"] = FormatBytes(_arrayPooledAlloc),
        ["ToList_Count"] = $"{_toListCount:N0} items",
        ["ToList_Allocated"] = FormatBytes(_toListAlloc),
        ["GetIndices_Count"] = $"{_getIndicesCount:N0} items",
        ["GetIndices_Allocated"] = FormatBytes(_getIndicesAlloc),
        ["Savings_vs_ToList"] = _toListAlloc > 0 
            ? $"{100.0 * (1.0 - (double)_arrayPooledAlloc / _toListAlloc):F1}%"
            : "N/A"
    };

    private static string FormatBytes(long bytes)
    {
        if (bytes < 1024) return $"{bytes} B";
        if (bytes < 1024 * 1024) return $"{bytes / 1024.0:F1} KB";
        return $"{bytes / (1024.0 * 1024):F1} MB";
    }
}
