using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;

namespace ArrowCollection.Benchmarks;

/// <summary>
/// Benchmarks for extreme scenario: 200-property record type with 1 million items.
/// This tests ArrowCollection's performance with wide tables containing:
/// - 10 string properties with low cardinality (100 distinct values each)
/// - 5 DateTime properties with high cardinality (timestamps)
/// - 62 int properties with high cardinality
/// - 62 double properties with high cardinality
/// - 61 decimal properties with high cardinality
/// </summary>
[ShortRunJob]
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
public class HeavyRecordBenchmarks
{
    private List<HeavyBenchmarkItem> _sourceItems = null!;
    private List<HeavyBenchmarkItem> _list = null!;
    private ArrowCollection<HeavyBenchmarkItem> _collection = null!;

    private const int ItemCount = 1_000_000;

    [GlobalSetup]
    public void Setup()
    {
        Console.WriteLine($"Generating {ItemCount:N0} heavy items (200 properties each)...");
        _sourceItems = HeavyBenchmarkItemFactory.Generate(ItemCount);
        
        Console.WriteLine("Building List<T> for enumeration benchmarks...");
        _list = [.. _sourceItems];
        
        Console.WriteLine("Building ArrowCollection<T> for enumeration benchmarks...");
        _collection = _sourceItems.ToArrowCollection();
        
        Console.WriteLine("Setup complete.");
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _collection?.Dispose();
    }

    #region Construction Benchmarks

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Heavy", "Construction")]
    public List<HeavyBenchmarkItem> List_Construction_Heavy_1M()
    {
        return [.. _sourceItems];
    }

    [Benchmark]
    [BenchmarkCategory("Heavy", "Construction")]
    public ArrowCollection<HeavyBenchmarkItem> Arrow_Construction_Heavy_1M()
    {
        var collection = _sourceItems.ToArrowCollection();
        collection.Dispose();
        return collection;
    }

    #endregion

    #region Enumeration Benchmarks

    [Benchmark]
    [BenchmarkCategory("Heavy", "Enumeration")]
    public long List_Enumeration_Heavy_1M()
    {
        long sum = 0;
        foreach (var item in _list)
        {
            // Access a few fields to simulate realistic enumeration
            sum += item.Int001 + item.Int031 + item.Int062;
        }
        return sum;
    }

    [Benchmark]
    [BenchmarkCategory("Heavy", "Enumeration")]
    public long Arrow_Enumeration_Heavy_1M()
    {
        long sum = 0;
        foreach (var item in _collection)
        {
            // Access a few fields to simulate realistic enumeration
            sum += item.Int001 + item.Int031 + item.Int062;
        }
        return sum;
    }

    #endregion

    #region Full Field Access Benchmarks

    [Benchmark]
    [BenchmarkCategory("Heavy", "FullAccess")]
    public double List_FullFieldAccess_Heavy_1M()
    {
        double sum = 0;
        foreach (var item in _list)
        {
            // Access all numeric fields
            sum += item.Int001 + item.Int002 + item.Int003 + item.Int004 + item.Int005;
            sum += item.Double001 + item.Double002 + item.Double003 + item.Double004 + item.Double005;
            sum += (double)(item.Decimal001 + item.Decimal002 + item.Decimal003 + item.Decimal004 + item.Decimal005);
        }
        return sum;
    }

    [Benchmark]
    [BenchmarkCategory("Heavy", "FullAccess")]
    public double Arrow_FullFieldAccess_Heavy_1M()
    {
        double sum = 0;
        foreach (var item in _collection)
        {
            // Access all numeric fields
            sum += item.Int001 + item.Int002 + item.Int003 + item.Int004 + item.Int005;
            sum += item.Double001 + item.Double002 + item.Double003 + item.Double004 + item.Double005;
            sum += (double)(item.Decimal001 + item.Decimal002 + item.Decimal003 + item.Decimal004 + item.Decimal005);
        }
        return sum;
    }

    #endregion

    #region String Access Benchmarks

    [Benchmark]
    [BenchmarkCategory("Heavy", "StringAccess")]
    public int List_StringAccess_Heavy_1M()
    {
        int totalLength = 0;
        foreach (var item in _list)
        {
            // Access all string fields
            totalLength += item.String01.Length + item.String02.Length + item.String03.Length;
            totalLength += item.String04.Length + item.String05.Length + item.String06.Length;
            totalLength += item.String07.Length + item.String08.Length + item.String09.Length + item.String10.Length;
        }
        return totalLength;
    }

    [Benchmark]
    [BenchmarkCategory("Heavy", "StringAccess")]
    public int Arrow_StringAccess_Heavy_1M()
    {
        int totalLength = 0;
        foreach (var item in _collection)
        {
            // Access all string fields
            totalLength += item.String01.Length + item.String02.Length + item.String03.Length;
            totalLength += item.String04.Length + item.String05.Length + item.String06.Length;
            totalLength += item.String07.Length + item.String08.Length + item.String09.Length + item.String10.Length;
        }
        return totalLength;
    }

    #endregion

    #region DateTime Access Benchmarks

    [Benchmark]
    [BenchmarkCategory("Heavy", "DateTimeAccess")]
    public long List_DateTimeAccess_Heavy_1M()
    {
        long totalTicks = 0;
        foreach (var item in _list)
        {
            totalTicks += item.Timestamp01.Ticks + item.Timestamp02.Ticks + item.Timestamp03.Ticks;
            totalTicks += item.Timestamp04.Ticks + item.Timestamp05.Ticks;
        }
        return totalTicks;
    }

    [Benchmark]
    [BenchmarkCategory("Heavy", "DateTimeAccess")]
    public long Arrow_DateTimeAccess_Heavy_1M()
    {
        long totalTicks = 0;
        foreach (var item in _collection)
        {
            totalTicks += item.Timestamp01.Ticks + item.Timestamp02.Ticks + item.Timestamp03.Ticks;
            totalTicks += item.Timestamp04.Ticks + item.Timestamp05.Ticks;
        }
        return totalTicks;
    }

    #endregion
}
