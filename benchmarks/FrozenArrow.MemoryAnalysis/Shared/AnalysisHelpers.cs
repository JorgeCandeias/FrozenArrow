using System.Diagnostics;

namespace FrozenArrow.MemoryAnalysis.Shared;

/// <summary>
/// Common utilities for memory analysis.
/// </summary>
public static class AnalysisHelpers
{
    /// <summary>
    /// Forces a full garbage collection with compaction.
    /// </summary>
    public static void ForceGC()
    {
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, blocking: true, compacting: true);
        GC.WaitForPendingFinalizers();
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, blocking: true, compacting: true);
    }

    /// <summary>
    /// Gets the current process private memory size.
    /// </summary>
    public static long GetProcessMemory()
    {
        using var process = Process.GetCurrentProcess();
        process.Refresh();
        return process.PrivateMemorySize64;
    }

    /// <summary>
    /// Formats bytes into a human-readable string.
    /// </summary>
    public static string FormatBytes(long bytes)
    {
        if (bytes < 0) return "N/A";
        if (bytes < 1024) return $"{bytes} B";
        if (bytes < 1024 * 1024) return $"{bytes / 1024.0:F1} KB";
        return $"{bytes / (1024.0 * 1024.0):F1} MB";
    }

    /// <summary>
    /// Formats a ratio as a comparison string.
    /// </summary>
    public static string FormatRatio(double ratio)
    {
        if (ratio < 1.0)
            return $"{(1.0 - ratio) * 100:F0}% smaller";
        else if (ratio > 1.0)
            return $"{ratio:F1}x larger";
        else
            return "same";
    }

    /// <summary>
    /// Prints a section header.
    /// </summary>
    public static void PrintHeader(string title)
    {
        Console.WriteLine();
        Console.WriteLine($"+{new string('=', 78)}+");
        Console.WriteLine($"| {title,-76} |");
        Console.WriteLine($"+{new string('=', 78)}+");
        Console.WriteLine();
    }

    /// <summary>
    /// Prints a sub-section header.
    /// </summary>
    public static void PrintSubHeader(string title)
    {
        Console.WriteLine();
        Console.WriteLine(title);
        Console.WriteLine(new string('=', title.Length));
        Console.WriteLine();
    }
}
