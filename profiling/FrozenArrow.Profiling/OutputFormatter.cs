using System.Text;
using System.Text.Json;

namespace FrozenArrow.Profiling;

/// <summary>
/// Formats profiling results for output.
/// </summary>
public sealed class OutputFormatter
{
    private readonly ProfilingConfig _config;

    public OutputFormatter(ProfilingConfig config)
    {
        _config = config;
    }

    public string Format(List<ProfilingResult> results)
    {
        return _config.OutputFormat switch
        {
            OutputFormat.Table => FormatTable(results),
            OutputFormat.Json => FormatJson(results),
            OutputFormat.Csv => FormatCsv(results),
            OutputFormat.Markdown => FormatMarkdown(results),
            _ => FormatTable(results)
        };
    }

    private string FormatTable(List<ProfilingResult> results)
    {
        var sb = new StringBuilder();
        sb.AppendLine();
        sb.AppendLine("???????????????????????????????????????????????????????????????????????????????????????????????????????");
        sb.AppendLine("  PROFILING RESULTS");
        sb.AppendLine("???????????????????????????????????????????????????????????????????????????????????????????????????????");
        sb.AppendLine();
        sb.AppendLine(string.Format("  {0,-30} {1,12} {2,10} {3,10} {4,10} {5,10} {6,12}",
            "Scenario", "Median (?s)", "Min", "Max", "StdDev", "M rows/s", "Alloc"));
        sb.AppendLine(string.Format("  {0} {1} {2} {3} {4} {5} {6}",
            new string('-', 30), new string('-', 12), new string('-', 10), new string('-', 10), new string('-', 10), new string('-', 10), new string('-', 12)));

        foreach (var r in results)
        {
            var alloc = r.AllocatedBytes.HasValue ? FormatBytes(r.AllocatedBytes.Value) : "N/A";
            sb.AppendLine(string.Format("  {0,-30} {1,12:F1} {2,10:F1} {3,10:F1} {4,10:F1} {5,10:F2} {6,12}",
                r.ScenarioName, r.MedianMicroseconds, r.MinMicroseconds, r.MaxMicroseconds, r.StdDevMicroseconds, r.MillionRowsPerSecond, alloc));
        }

        sb.AppendLine();

        // Show phase details if verbose
        if (_config.Verbose)
        {
            foreach (var r in results.Where(r => r.PhaseDetails is not null))
            {
                sb.AppendLine($"  Phase breakdown for: {r.ScenarioName}");
                sb.AppendLine($"  {new string('-', 60)}");
                foreach (var (name, phase) in r.PhaseDetails!.OrderByDescending(p => p.Value.PercentageOfTotal))
                {
                    sb.AppendLine($"    {name,-25} {phase.AverageMicroseconds,10:F1} ?s ({phase.PercentageOfTotal,5:F1}%)");
                }
                sb.AppendLine();
            }
        }

        // Show metadata
        foreach (var r in results.Where(r => r.Metadata is not null && r.Metadata.Count > 0))
        {
            sb.AppendLine($"  Notes for {r.ScenarioName}:");
            foreach (var (key, value) in r.Metadata!)
            {
                sb.AppendLine($"    {key}: {value}");
            }
            sb.AppendLine();
        }

        sb.AppendLine("???????????????????????????????????????????????????????????????????????????????????????????????????????");

        return sb.ToString();
    }

    private static string FormatJson(List<ProfilingResult> results)
    {
        return JsonSerializer.Serialize(results, new JsonSerializerOptions
        {
            WriteIndented = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        });
    }

    private static string FormatCsv(List<ProfilingResult> results)
    {
        var sb = new StringBuilder();
        sb.AppendLine("Scenario,RowCount,MedianMicroseconds,MinMicroseconds,MaxMicroseconds,StdDevMicroseconds,MillionRowsPerSecond,AllocatedBytes");

        foreach (var r in results)
        {
            sb.AppendLine($"{r.ScenarioName},{r.RowCount},{r.MedianMicroseconds:F2},{r.MinMicroseconds:F2},{r.MaxMicroseconds:F2},{r.StdDevMicroseconds:F2},{r.MillionRowsPerSecond:F4},{r.AllocatedBytes ?? 0}");
        }

        return sb.ToString();
    }

    private static string FormatMarkdown(List<ProfilingResult> results)
    {
        var sb = new StringBuilder();
        sb.AppendLine("## Profiling Results");
        sb.AppendLine();
        sb.AppendLine($"Generated: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
        sb.AppendLine();
        sb.AppendLine("| Scenario | Median (?s) | Min | Max | StdDev | M rows/s | Allocated |");
        sb.AppendLine("|----------|-------------|-----|-----|--------|----------|-----------|");

        foreach (var r in results)
        {
            var alloc = r.AllocatedBytes.HasValue ? FormatBytes(r.AllocatedBytes.Value) : "N/A";
            sb.AppendLine($"| {r.ScenarioName} | {r.MedianMicroseconds:F1} | {r.MinMicroseconds:F1} | {r.MaxMicroseconds:F1} | {r.StdDevMicroseconds:F1} | {r.MillionRowsPerSecond:F2} | {alloc} |");
        }

        return sb.ToString();
    }

    private static string FormatBytes(long bytes)
    {
        if (bytes < 1024) return $"{bytes} B";
        if (bytes < 1024 * 1024) return $"{bytes / 1024.0:F1} KB";
        return $"{bytes / (1024.0 * 1024.0):F1} MB";
    }
}
