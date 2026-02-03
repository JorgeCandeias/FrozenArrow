using System.Numerics;
using FrozenArrow.Query;

namespace FrozenArrow.Profiling.Scenarios;

/// <summary>
/// Profiles SelectionBitmap operations directly.
/// Useful for understanding bitmap overhead vs actual computation.
/// </summary>
public sealed class BitmapOperationsScenario : BaseScenario
{
    public override string Name => "BitmapOperations";
    public override string Description => "SelectionBitmap create, popcount, iterate";

    private int _count;
    private int _iteratedCount;

    public override object? RunIteration()
    {
        // Create bitmap
        using var bitmap = SelectionBitmap.Create(Config.RowCount, initialValue: true);

        // Clear some bits (simulate filtering)
        for (int i = 0; i < Config.RowCount; i += 3)
        {
            bitmap.Clear(i);
        }

        // Count set bits (popcount)
        _count = bitmap.CountSet();

        // Iterate selected indices
        _iteratedCount = 0;
        foreach (var _ in bitmap.GetSelectedIndices())
        {
            _iteratedCount++;
        }

        return _count + _iteratedCount;
    }

    public override (object? Result, Dictionary<string, double> PhaseMicroseconds) RunIterationWithPhases()
    {
        CurrentPhases.Clear();

        StartPhase("Create");
        using var bitmap = SelectionBitmap.Create(Config.RowCount, initialValue: true);
        EndPhase("Create");

        StartPhase("ClearBits");
        for (int i = 0; i < Config.RowCount; i += 3)
        {
            bitmap.Clear(i);
        }
        EndPhase("ClearBits");

        StartPhase("PopCount");
        _count = bitmap.CountSet();
        EndPhase("PopCount");

        StartPhase("IterateIndices");
        _iteratedCount = 0;
        foreach (var _ in bitmap.GetSelectedIndices())
        {
            _iteratedCount++;
        }
        EndPhase("IterateIndices");

        return (_count + _iteratedCount, new Dictionary<string, double>(CurrentPhases));
    }

    public override Dictionary<string, string> GetMetadata()
    {
        var bitmapSizeKB = (Config.RowCount + 63) / 64 * 8 / 1024.0;
        return new()
        {
            ["CountSet"] = $"{_count:N0}",
            ["IteratedCount"] = $"{_iteratedCount:N0}",
            ["BitmapSize"] = $"{bitmapSizeKB:F1} KB",
            ["HardwarePopCount"] = "true", // BitOperations.PopCount uses hardware when available
            ["SIMD"] = $"AVX2={System.Runtime.Intrinsics.X86.Avx2.IsSupported}, AVX512={System.Runtime.Intrinsics.X86.Avx512F.IsSupported}"
        };
    }
}
