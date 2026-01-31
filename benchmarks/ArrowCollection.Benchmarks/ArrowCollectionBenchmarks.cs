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
                HalfValue = (Half)(i * 0.5f),
                BinaryData = [(byte)(i % 256), (byte)((i >> 8) % 256), (byte)((i >> 16) % 256)],
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

/// <summary>
/// Benchmarks comparing class vs struct performance in ArrowCollection.
/// </summary>
[ShortRunJob]
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
public class StructVsClassBenchmarks
{
    private List<BenchmarkItem> _classItems = null!;
    private List<BenchmarkStruct> _structItems = null!;
    private List<BenchmarkReadonlyStruct> _readonlyStructItems = null!;

    private ArrowCollection<BenchmarkItem> _classCollection = null!;
    private ArrowCollection<BenchmarkStruct> _structCollection = null!;
    private ArrowCollection<BenchmarkReadonlyStruct> _readonlyStructCollection = null!;

    private List<BenchmarkItem> _classList = null!;
    private List<BenchmarkStruct> _structList = null!;
    private List<BenchmarkReadonlyStruct> _readonlyStructList = null!;

    private const int ItemCount = 100_000;

    [GlobalSetup]
    public void Setup()
    {
        var baseDate = DateTime.UtcNow;
        var categories = Enumerable.Range(0, 100).Select(i => $"Category_{i}").ToArray();

        _classItems = [.. Enumerable.Range(0, ItemCount).Select(i => new BenchmarkItem
        {
            Id = i,
            Category1 = categories[i % categories.Length],
            Category2 = categories[(i + 33) % categories.Length],
            Category3 = categories[(i + 67) % categories.Length],
            Value = i * 1.5,
            HalfValue = (Half)(i * 0.5f),
            BinaryData = [(byte)(i % 256), (byte)((i >> 8) % 256)],
            IsActive = i % 2 == 0,
            CreatedAt = baseDate.AddSeconds(-i)
        })];

        _structItems = [.. Enumerable.Range(0, ItemCount).Select(i => new BenchmarkStruct
        {
            Id = i,
            Category1 = categories[i % categories.Length],
            Category2 = categories[(i + 33) % categories.Length],
            Category3 = categories[(i + 67) % categories.Length],
            Value = i * 1.5,
            HalfValue = (Half)(i * 0.5f),
            BinaryData = [(byte)(i % 256), (byte)((i >> 8) % 256)],
            IsActive = i % 2 == 0,
            CreatedAt = baseDate.AddSeconds(-i)
        })];

        _readonlyStructItems = [.. Enumerable.Range(0, ItemCount).Select(i => new BenchmarkReadonlyStruct
        {
            Id = i,
            Category1 = categories[i % categories.Length],
            Category2 = categories[(i + 33) % categories.Length],
            Category3 = categories[(i + 67) % categories.Length],
            Value = i * 1.5,
            HalfValue = (Half)(i * 0.5f),
            BinaryData = [(byte)(i % 256), (byte)((i >> 8) % 256)],
            IsActive = i % 2 == 0,
            CreatedAt = baseDate.AddSeconds(-i)
        })];

        // Pre-build collections for enumeration benchmarks
        _classCollection = _classItems.ToArrowCollection();
        _structCollection = _structItems.ToArrowCollection();
        _readonlyStructCollection = _readonlyStructItems.ToArrowCollection();

        // Pre-build lists for comparison
        _classList = [.. _classItems];
        _structList = [.. _structItems];
        _readonlyStructList = [.. _readonlyStructItems];
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _classCollection?.Dispose();
        _structCollection?.Dispose();
        _readonlyStructCollection?.Dispose();
    }

    #region Construction Benchmarks

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Construction")]
    public ArrowCollection<BenchmarkItem> Class_Construction()
    {
        var collection = _classItems.ToArrowCollection();
        collection.Dispose();
        return collection;
    }

    [Benchmark]
    [BenchmarkCategory("Construction")]
    public ArrowCollection<BenchmarkStruct> Struct_Construction()
    {
        var collection = _structItems.ToArrowCollection();
        collection.Dispose();
        return collection;
    }

    [Benchmark]
    [BenchmarkCategory("Construction")]
    public ArrowCollection<BenchmarkReadonlyStruct> ReadonlyStruct_Construction()
    {
        var collection = _readonlyStructItems.ToArrowCollection();
        collection.Dispose();
        return collection;
    }

    #endregion

    #region Enumeration Benchmarks

    [Benchmark]
    [BenchmarkCategory("Enumeration")]
    public int Class_Enumeration()
    {
        int count = 0;
        foreach (var item in _classCollection)
        {
            count += item.Id;
        }
        return count;
    }

    [Benchmark]
    [BenchmarkCategory("Enumeration")]
    public int Struct_Enumeration()
    {
        int count = 0;
        foreach (var item in _structCollection)
        {
            count += item.Id;
        }
        return count;
    }

    [Benchmark]
    [BenchmarkCategory("Enumeration")]
    public int ReadonlyStruct_Enumeration()
    {
        int count = 0;
        foreach (var item in _readonlyStructCollection)
        {
            count += item.Id;
        }
        return count;
    }

    #endregion

    #region List vs Arrow Comparison

    [Benchmark]
    [BenchmarkCategory("ListComparison")]
    public int List_Class_Enumeration()
    {
        int count = 0;
        foreach (var item in _classList)
        {
            count += item.Id;
        }
        return count;
    }

    [Benchmark]
    [BenchmarkCategory("ListComparison")]
    public int List_Struct_Enumeration()
    {
        int count = 0;
        foreach (var item in _structList)
        {
            count += item.Id;
        }
        return count;
    }

    [Benchmark]
    [BenchmarkCategory("ListComparison")]
    public int List_ReadonlyStruct_Enumeration()
    {
        int count = 0;
        foreach (var item in _readonlyStructList)
        {
            count += item.Id;
        }
        return count;
    }

    #endregion
}

[ArrowRecord]
public class BenchmarkItem
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
    public Half HalfValue { get; set; }
    [ArrowArray]
    public byte[] BinaryData { get; set; } = [];
    [ArrowArray]
    public bool IsActive { get; set; }
    [ArrowArray]
    public DateTime CreatedAt { get; set; }
}

[ArrowRecord]
public struct BenchmarkStruct
{
    [ArrowArray]
    public int Id { get; set; }
    [ArrowArray]
    public string Category1 { get; set; }
    [ArrowArray]
    public string Category2 { get; set; }
    [ArrowArray]
    public string Category3 { get; set; }
    [ArrowArray]
    public double Value { get; set; }
    [ArrowArray]
    public Half HalfValue { get; set; }
    [ArrowArray]
    public byte[] BinaryData { get; set; }
    [ArrowArray]
    public bool IsActive { get; set; }
    [ArrowArray]
    public DateTime CreatedAt { get; set; }
}

[ArrowRecord]
public readonly struct BenchmarkReadonlyStruct
{
    [ArrowArray]
    public int Id { get; init; }
    [ArrowArray]
    public string Category1 { get; init; }
    [ArrowArray]
    public string Category2 { get; init; }
    [ArrowArray]
    public string Category3 { get; init; }
    [ArrowArray]
    public double Value { get; init; }
    [ArrowArray]
    public Half HalfValue { get; init; }
    [ArrowArray]
    public byte[] BinaryData { get; init; }
    [ArrowArray]
    public bool IsActive { get; init; }
    [ArrowArray]
    public DateTime CreatedAt { get; init; }
}


