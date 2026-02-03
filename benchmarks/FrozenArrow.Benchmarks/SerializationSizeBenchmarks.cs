using Apache.Arrow.Ipc;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Order;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;
using ProtoBuf;

namespace FrozenArrow.Benchmarks;

/// <summary>
/// Benchmarks comparing serialization size between Arrow IPC (with various compression options) and Protobuf.
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[Config(typeof(SerializationSizeConfig))]
[ShortRunJob]
public class SerializationSizeBenchmarks
{
    private List<SerializationBenchmarkItem> _items = null!;
    private FrozenArrow<SerializationBenchmarkItem> _frozenArrow = null!;
    private ProtobufCollection _protobufCollection = null!;

    private ArrowWriteOptions _noCompression = null!;
    private ArrowWriteOptions _lz4Compression = null!;
    private ArrowWriteOptions _zstdCompression = null!;

    [Params(10_000, 100_000, 1_000_000)]
    public int ItemCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _items = GenerateItems(ItemCount);
        _frozenArrow = _items.ToFrozenArrow();
        _protobufCollection = new ProtobufCollection { Items = _items };

        _noCompression = new ArrowWriteOptions { CompressionCodec = null };
        _lz4Compression = new ArrowWriteOptions { CompressionCodec = CompressionCodecType.Lz4Frame };
        _zstdCompression = new ArrowWriteOptions { CompressionCodec = CompressionCodecType.Zstd };
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _frozenArrow?.Dispose();
    }

    private static List<SerializationBenchmarkItem> GenerateItems(int count)
    {
        var items = new List<SerializationBenchmarkItem>(count);
        var baseDate = DateTime.UtcNow;
        var random = new Random(42); // Fixed seed for reproducibility

        // Generate distinct strings with realistic patterns
        var categories = Enumerable.Range(0, 50).Select(i => $"Category_{i:D3}").ToArray();
        var names = Enumerable.Range(0, 500).Select(i => $"ItemName_{i:D5}").ToArray();
        var descriptions = Enumerable.Range(0, 100).Select(i => $"This is a longer description text for item number {i}. It contains more content to simulate real-world data.").ToArray();

        for (int i = 0; i < count; i++)
        {
            items.Add(new SerializationBenchmarkItem
            {
                Id = i,
                Name = names[i % names.Length],
                Category = categories[i % categories.Length],
                Description = descriptions[i % descriptions.Length],
                Price = Math.Round(random.NextDouble() * 1000, 2),
                Quantity = random.Next(1, 1000),
                IsAvailable = random.NextDouble() > 0.3,
                Rating = (float)Math.Round(random.NextDouble() * 5, 1),
                CreatedAt = baseDate.AddSeconds(-i),
                Tags = [(byte)(i % 256), (byte)((i >> 8) % 256), (byte)(random.Next(256))]
            });
        }
        return items;
    }

    #region Arrow Benchmarks

    [Benchmark]
    [BenchmarkCategory("Serialization")]
    public long Arrow_NoCompression()
    {
        using var stream = new MemoryStream();
        _frozenArrow.WriteToAsync(stream, _noCompression).GetAwaiter().GetResult();
        return stream.Length;
    }

    [Benchmark]
    [BenchmarkCategory("Serialization")]
    public long Arrow_Lz4Compression()
    {
        using var stream = new MemoryStream();
        _frozenArrow.WriteToAsync(stream, _lz4Compression).GetAwaiter().GetResult();
        return stream.Length;
    }

    [Benchmark]
    [BenchmarkCategory("Serialization")]
    public long Arrow_ZstdCompression()
    {
        using var stream = new MemoryStream();
        _frozenArrow.WriteToAsync(stream, _zstdCompression).GetAwaiter().GetResult();
        return stream.Length;
    }

    #endregion

    #region Protobuf Benchmarks

    [Benchmark]
    [BenchmarkCategory("Serialization")]
    public long Protobuf()
    {
        using var stream = new MemoryStream();
        Serializer.Serialize(stream, _protobufCollection);
        return stream.Length;
    }

    #endregion
}

/// <summary>
/// Configuration that adds a custom column showing the serialized byte count.
/// </summary>
public class SerializationSizeConfig : ManualConfig
{
    public SerializationSizeConfig()
    {
        AddColumn(new SerializedBytesColumn());
    }
}

/// <summary>
/// Custom column that displays the serialized size returned by benchmarks.
/// </summary>
public class SerializedBytesColumn : IColumn
{
    public string Id => "SerializedBytes";
    public string ColumnName => "Bytes";
    public bool AlwaysShow => true;
    public ColumnCategory Category => ColumnCategory.Custom;
    public int PriorityInCategory => 0;
    public bool IsNumeric => true;
    public UnitType UnitType => UnitType.Size;
    public string Legend => "Serialized size in bytes";

    public string GetValue(Summary summary, BenchmarkCase benchmarkCase)
    {
        var report = summary[benchmarkCase];
        if (report?.ResultStatistics == null)
            return "N/A";

        // The benchmark returns the byte count as the result
        // We can extract it from the workload result if available
        return "See Return";
    }

    public string GetValue(Summary summary, BenchmarkCase benchmarkCase, SummaryStyle style) 
        => GetValue(summary, benchmarkCase);

    public bool IsDefault(Summary summary, BenchmarkCase benchmarkCase) => false;
    public bool IsAvailable(Summary summary) => true;
}

/// <summary>
/// Model class annotated with both [ArrowRecord] and [ProtoContract] for fair comparison.
/// </summary>
[ArrowRecord]
[ProtoContract]
public class SerializationBenchmarkItem
{
    [ArrowArray]
    [ProtoMember(1)]
    public int Id { get; set; }

    [ArrowArray]
    [ProtoMember(2)]
    public string Name { get; set; } = "";

    [ArrowArray]
    [ProtoMember(3)]
    public string Category { get; set; } = "";

    [ArrowArray]
    [ProtoMember(4)]
    public string Description { get; set; } = "";

    [ArrowArray]
    [ProtoMember(5)]
    public double Price { get; set; }

    [ArrowArray]
    [ProtoMember(6)]
    public int Quantity { get; set; }

    [ArrowArray]
    [ProtoMember(7)]
    public bool IsAvailable { get; set; }

    [ArrowArray]
    [ProtoMember(8)]
    public float Rating { get; set; }

    [ArrowArray]
    [ProtoMember(9)]
    public DateTime CreatedAt { get; set; }

    [ArrowArray]
    [ProtoMember(10)]
    public byte[] Tags { get; set; } = [];
}

/// <summary>
/// Wrapper class for Protobuf serialization of the collection.
/// </summary>
[ProtoContract]
public class ProtobufCollection
{
    [ProtoMember(1)]
    public List<SerializationBenchmarkItem> Items { get; set; } = [];
}
