using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Apache.Arrow;
using Apache.Arrow.Memory;
using FrozenArrow.Query;

namespace FrozenArrow.Benchmarks.Internals;

/// <summary>
/// Benchmarks for dictionary-encoded string predicate evaluation.
/// Measures the performance improvement of evaluating predicates once per dictionary entry
/// vs. evaluating per row.
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[ShortRunJob]
public class DictionaryEncodedStringBenchmarks
{
    private RecordBatch _lowCardinalityBatch = null!;
    private RecordBatch _mediumCardinalityBatch = null!;
    private RecordBatch _primitiveStringBatch = null!;
    private StringEqualityPredicate _equalityPredicate = null!;
    private StringOperationPredicate _startsWithPredicate = null!;

    [Params(100_000, 1_000_000)]
    public int RowCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var allocator = new NativeMemoryAllocator();

        // Low cardinality: 10 unique values (90% compression)
        _lowCardinalityBatch = CreateDictionaryEncodedBatch(RowCount, 10, allocator);

        // Medium cardinality: 100 unique values (99% of rows will match in dictionary)
        _mediumCardinalityBatch = CreateDictionaryEncodedBatch(RowCount, 100, allocator);

        // Primitive string array (no dictionary encoding)
        _primitiveStringBatch = CreatePrimitiveStringBatch(RowCount, 10, allocator);

        // Predicates
        _equalityPredicate = new StringEqualityPredicate(
            columnName: "Category",
            columnIndex: 0,
            value: "Category_5", // Will match ~10% of rows
            negate: false,
            comparison: StringComparison.Ordinal);

        _startsWithPredicate = new StringOperationPredicate(
            columnName: "Category",
            columnIndex: 0,
            pattern: "Category_",
            operation: StringOperation.StartsWith,
            comparison: StringComparison.Ordinal);
    }

    private static RecordBatch CreateDictionaryEncodedBatch(int rowCount, int uniqueValues, MemoryAllocator allocator)
    {
        var values = new List<string?>(rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            // Cycle through unique values
            values.Add($"Category_{i % uniqueValues}");
        }

        var statistics = new ColumnStatistics
        {
            ColumnName = "Category",
            ValueType = typeof(string),
            DistinctCount = uniqueValues,
            TotalCount = rowCount
        };

        var array = DictionaryArrayBuilder.BuildStringArray(values, statistics, allocator);
        var field = new Field("Category", array.Data.DataType, nullable: false);
        var schema = new Schema([field], null);

        return new RecordBatch(schema, [array], rowCount);
    }

    private static RecordBatch CreatePrimitiveStringBatch(int rowCount, int uniqueValues, MemoryAllocator allocator)
    {
        var builder = new StringArray.Builder();
        for (int i = 0; i < rowCount; i++)
        {
            builder.Append($"Category_{i % uniqueValues}");
        }

        var array = builder.Build(allocator);
        var field = new Field("Category", array.Data.DataType, nullable: false);
        var schema = new Schema([field], null);

        return new RecordBatch(schema, [array], rowCount);
    }

    #region Equality Predicates

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Equality")]
    public int Primitive_Equality_Filter()
    {
        var bitmap = SelectionBitmap.Create(_primitiveStringBatch.Length);
        try
        {
            _equalityPredicate.Evaluate(_primitiveStringBatch, ref bitmap);
            return bitmap.CountSet();
        }
        finally
        {
            bitmap.Dispose();
        }
    }

    [Benchmark]
    [BenchmarkCategory("Equality")]
    public int LowCardinality_Equality_Filter()
    {
        var bitmap = SelectionBitmap.Create(_lowCardinalityBatch.Length);
        try
        {
            _equalityPredicate.Evaluate(_lowCardinalityBatch, ref bitmap);
            return bitmap.CountSet();
        }
        finally
        {
            bitmap.Dispose();
        }
    }

    [Benchmark]
    [BenchmarkCategory("Equality")]
    public int MediumCardinality_Equality_Filter()
    {
        var bitmap = SelectionBitmap.Create(_mediumCardinalityBatch.Length);
        try
        {
            _equalityPredicate.Evaluate(_mediumCardinalityBatch, ref bitmap);
            return bitmap.CountSet();
        }
        finally
        {
            bitmap.Dispose();
        }
    }

    #endregion

    #region StartsWith Predicates

    [Benchmark]
    [BenchmarkCategory("StartsWith")]
    public int Primitive_StartsWith_Filter()
    {
        var bitmap = SelectionBitmap.Create(_primitiveStringBatch.Length);
        try
        {
            _startsWithPredicate.Evaluate(_primitiveStringBatch, ref bitmap);
            return bitmap.CountSet();
        }
        finally
        {
            bitmap.Dispose();
        }
    }

    [Benchmark]
    [BenchmarkCategory("StartsWith")]
    public int LowCardinality_StartsWith_Filter()
    {
        var bitmap = SelectionBitmap.Create(_lowCardinalityBatch.Length);
        try
        {
            _startsWithPredicate.Evaluate(_lowCardinalityBatch, ref bitmap);
            return bitmap.CountSet();
        }
        finally
        {
            bitmap.Dispose();
        }
    }

    [Benchmark]
    [BenchmarkCategory("StartsWith")]
    public int MediumCardinality_StartsWith_Filter()
    {
        var bitmap = SelectionBitmap.Create(_mediumCardinalityBatch.Length);
        try
        {
            _startsWithPredicate.Evaluate(_mediumCardinalityBatch, ref bitmap);
            return bitmap.CountSet();
        }
        finally
        {
            bitmap.Dispose();
        }
    }

    #endregion

    [GlobalCleanup]
    public void Cleanup()
    {
        _lowCardinalityBatch?.Dispose();
        _mediumCardinalityBatch?.Dispose();
        _primitiveStringBatch?.Dispose();
    }
}
