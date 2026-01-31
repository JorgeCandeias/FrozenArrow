using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;

namespace ArrowCollection.Benchmarks;

[ShortRunJob]
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
public class ArrowCollectionBenchmarks
{
    private List<BenchmarkItem> _items10K = null!;
    private List<BenchmarkItem> _items100K = null!;
    private List<BenchmarkItem> _items1M = null!;

    private List<BenchmarkItem> _list10K = null!;
    private List<BenchmarkItem> _list100K = null!;
    private List<BenchmarkItem> _list1M = null!;

    private ArrowCollection<BenchmarkItem> _collection10K = null!;
    private ArrowCollection<BenchmarkItem> _collection100K = null!;
    private ArrowCollection<BenchmarkItem> _collection1M = null!;

    [GlobalSetup]
    public void Setup()
    {
        _items10K = GenerateItems(10_000);
        _items100K = GenerateItems(100_000);
        _items1M = GenerateItems(1_000_000);

        // Pre-built lists for enumeration benchmarks
        _list10K = [.. _items10K];
        _list100K = [.. _items100K];
        _list1M = [.. _items1M];

        // Pre-built collection collections for enumeration benchmarks
        _collection10K = _items10K.ToArrowCollection();
        _collection100K = _items100K.ToArrowCollection();
        _collection1M = _items1M.ToArrowCollection();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _collection10K?.Dispose();
        _collection100K?.Dispose();
        _collection1M?.Dispose();
    }

    private static List<BenchmarkItem> GenerateItems(int count)
    {
        var items = new List<BenchmarkItem>(count);
        var baseDate = DateTime.UtcNow;
        
        // Generate distinct strings upfront to avoid duplication in memory
        var category1 = Enumerable.Range(0, 10).Select(i => $"Item_{i}").ToArray();
        var category2 = Enumerable.Range(0, 100).Select(i => $"Item_{i}").ToArray();
        var category3 = Enumerable.Range(0, 1000).Select(i => $"Item_{i}").ToArray();

        for (int i = 0; i < count; i++)
        {
            items.Add(new BenchmarkItem
            {
                Id = i,
                Category1 = category1[i % category1.Length],
                Category2 = category2[i % category2.Length],
                Category3 = category3[i % category3.Length],
                Value = i * 1.5,
                IsActive = i % 2 == 0,
                CreatedAt = baseDate.AddSeconds(-i)
            });
        }
        return items;
    }

    #region Construction Benchmarks - 10K Items

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Construction", "10K")]
    public List<BenchmarkItem> List_Construction_10K()
    {
        return [.. _items10K];
    }

    [Benchmark]
    [BenchmarkCategory("Construction", "10K")]
    public ArrowCollection<BenchmarkItem> Colly_Construction_10K()
    {
        var collection = _items10K.ToArrowCollection();
        collection.Dispose();
        return collection;
    }

    #endregion

    #region Construction Benchmarks - 100K Items

    [Benchmark]
    [BenchmarkCategory("Construction", "100K")]
    public List<BenchmarkItem> List_Construction_100K()
    {
        return [.. _items100K];
    }

    [Benchmark]
    [BenchmarkCategory("Construction", "100K")]
    public ArrowCollection<BenchmarkItem> Colly_Construction_100K()
    {
        var collection = _items100K.ToArrowCollection();
        collection.Dispose();
        return collection;
    }

    #endregion

    #region Construction Benchmarks - 1M Items

    [Benchmark]
    [BenchmarkCategory("Construction", "1M")]
    public List<BenchmarkItem> List_Construction_1M()
    {
        return [.. _items1M];
    }

    [Benchmark]
    [BenchmarkCategory("Construction", "1M")]
    public ArrowCollection<BenchmarkItem> Colly_Construction_1M()
    {
        var collection = _items1M.ToArrowCollection();
        collection.Dispose();
        return collection;
    }

    #endregion

    #region Enumeration Benchmarks - 10K Items

    [Benchmark]
    [BenchmarkCategory("Enumeration", "10K")]
    public int List_Enumeration_10K()
    {
        int count = 0;
        foreach (var item in _list10K)
        {
            count += item.Id;
        }
        return count;
    }

    [Benchmark]
    [BenchmarkCategory("Enumeration", "10K")]
    public int Colly_Enumeration_10K()
    {
        int count = 0;
        foreach (var item in _collection10K)
        {
            count += item.Id;
        }
        return count;
    }

    #endregion

    #region Enumeration Benchmarks - 100K Items

    [Benchmark]
    [BenchmarkCategory("Enumeration", "100K")]
    public int List_Enumeration_100K()
    {
        int count = 0;
        foreach (var item in _list100K)
        {
            count += item.Id;
        }
        return count;
    }

    [Benchmark]
    [BenchmarkCategory("Enumeration", "100K")]
    public int Colly_Enumeration_100K()
    {
        int count = 0;
        foreach (var item in _collection100K)
        {
            count += item.Id;
        }
        return count;
    }

    #endregion

    #region Enumeration Benchmarks - 1M Items

    [Benchmark]
    [BenchmarkCategory("Enumeration", "1M")]
    public int List_Enumeration_1M()
    {
        int count = 0;
        foreach (var item in _list1M)
        {
            count += item.Id;
        }
        return count;
    }

    [Benchmark]
    [BenchmarkCategory("Enumeration", "1M")]
    public int Colly_Enumeration_1M()
    {
        int count = 0;
        foreach (var item in _collection1M)
        {
            count += item.Id;
        }
        return count;
    }

    #endregion
}

[ArrowRecord]
public sealed record BenchmarkItem
{
    [ArrowArray]
    public int Id { get; set; }
    [ArrowArray]
    public string Category1 { get; set; } = "";
    [ArrowArray]
    public string Category2 { get; set; } = "";
    [ArrowArray]
    public string Category3 { get; set; } = "";
    [ArrowArray]
    public double Value { get; set; }
    [ArrowArray]
    public bool IsActive { get; set; }
    [ArrowArray]
    public DateTime CreatedAt { get; set; }
}


