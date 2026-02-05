using FrozenArrow.Query;
using System.Runtime.Intrinsics;

namespace FrozenArrow.Tests.Advanced;

/// <summary>
/// Tests for SIMD vectorization boundary conditions and edge cases.
/// Ensures SIMD operations handle all data sizes correctly, especially at vector boundaries.
/// </summary>
public class SimdBoundaryTests
{
    [ArrowRecord]
    public record SimdTestRecord
    {
        [ArrowArray(Name = "Id")]
        public int Id { get; init; }

        [ArrowArray(Name = "Value")]
        public int Value { get; init; }

        [ArrowArray(Name = "Score")]
        public double Score { get; init; }
    }

    private static FrozenArrow<SimdTestRecord> CreateTestData(int rowCount)
    {
        var records = new List<SimdTestRecord>(rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            records.Add(new SimdTestRecord
            {
                Id = i,
                Value = i % 1000,
                Score = i / 100.0
            });
        }
        return records.ToFrozenArrow();
    }

    [Theory]
    [InlineData(1)]      // Single element
    [InlineData(2)]      // Below AVX2 vector size (4 ints)
    [InlineData(3)]      
    [InlineData(4)]      // Exactly AVX2 vector size
    [InlineData(5)]      // Just above AVX2 vector size
    [InlineData(7)]      // Prime number
    [InlineData(8)]      // Exactly AVX2 vector size (8 doubles)
    [InlineData(15)]     // Just below power of 2
    [InlineData(16)]     // Power of 2
    [InlineData(17)]     // Just above power of 2
    [InlineData(31)]     
    [InlineData(32)]     // Larger power of 2
    [InlineData(33)]
    [InlineData(63)]
    [InlineData(64)]     // Bitmap block boundary
    [InlineData(65)]
    public void SimdBoundary_SmallDataSizes_HandledCorrectly(int rowCount)
    {
        // Test that SIMD operations handle small data sizes correctly
        // These sizes test vector boundary conditions
        
        // Arrange
        var data = CreateTestData(rowCount);

        // Act
        var count = data.AsQueryable()
            .Where(x => x.Value > 500)
            .Count();

        var expectedCount = Enumerable.Range(0, rowCount)
            .Count(i => i % 1000 > 500);

        // Assert
        Assert.Equal(expectedCount, count);
    }

    [Theory]
    [InlineData(255)]    // Just below 256 (common vector processing size)
    [InlineData(256)]    // Common SIMD chunk size
    [InlineData(257)]    
    [InlineData(511)]
    [InlineData(512)]    // Larger SIMD chunk
    [InlineData(513)]
    [InlineData(1023)]
    [InlineData(1024)]   // KB boundary
    [InlineData(1025)]
    public void SimdBoundary_MediumDataSizes_HandledCorrectly(int rowCount)
    {
        // Test medium-sized data that exercises multiple SIMD vectors
        
        // Arrange
        var data = CreateTestData(rowCount);

        // Act
        var results = data.AsQueryable()
            .Where(x => x.Value > 250 && x.Value < 750)
            .ToList();

        var expectedCount = Enumerable.Range(0, rowCount)
            .Count(i => i % 1000 > 250 && i % 1000 < 750);

        // Assert
        Assert.Equal(expectedCount, results.Count);
        Assert.All(results, r => Assert.True(r.Value > 250 && r.Value < 750));
    }

    [Theory]
    [InlineData(16383)]  // Just before chunk size
    [InlineData(16384)]  // Default chunk size (16KB rows)
    [InlineData(16385)]  // Just after chunk size
    [InlineData(32767)]
    [InlineData(32768)]  // 2x chunk size
    [InlineData(32769)]
    public void SimdBoundary_ChunkSizeBoundaries_HandledCorrectly(int rowCount)
    {
        // Test data sizes around parallel chunk boundaries
        
        // Arrange
        var data = CreateTestData(rowCount);

        // Act
        var sum = data.AsQueryable()
            .Where(x => x.Value > 100)
            .Sum(x => x.Value);

        var expectedSum = Enumerable.Range(0, rowCount)
            .Where(i => i % 1000 > 100)
            .Sum(i => i % 1000);

        // Assert
        Assert.Equal(expectedSum, sum);
    }

    [Fact]
    public void SimdBoundary_UnalignedData_HandledCorrectly()
    {
        // Test that SIMD operations handle unaligned data correctly
        
        var testSizes = new[] { 
            1, 3, 5, 7, 9, 11, 13, 15, 17,      // Odd numbers
            127, 255, 511, 1023, 2047,          // 2^n - 1
            63, 127, 191, 255, 319              // Multiples of odd numbers
        };

        foreach (var size in testSizes)
        {
            // Arrange
            var data = CreateTestData(size);

            // Act
            var count = data.AsQueryable()
                .Where(x => x.Value > 500)
                .Count();

            var expectedCount = Enumerable.Range(0, size)
                .Count(i => i % 1000 > 500);

            // Assert
            Assert.Equal(expectedCount, count);
        }
    }

    [Theory]
    [InlineData(4)]   // AVX2 int32 vector size
    [InlineData(8)]   // AVX2 double vector size / AVX-512 int32 vector size
    [InlineData(16)]  // AVX-512 double vector size
    public void SimdBoundary_ExactVectorSizes_HandledCorrectly(int vectorSize)
    {
        // Test data that is exactly a multiple of vector sizes
        
        for (int multiplier = 1; multiplier <= 10; multiplier++)
        {
            var rowCount = vectorSize * multiplier;
            
            // Arrange
            var data = CreateTestData(rowCount);

            // Act
            var results = data.AsQueryable()
                .Where(x => x.Value > 100)
                .ToList();

            var expectedCount = Enumerable.Range(0, rowCount)
                .Count(i => i % 1000 > 100);

            // Assert
            Assert.Equal(expectedCount, results.Count);
        }
    }

    [Theory]
    [InlineData(100_000)]
    public void SimdBoundary_DoubleComparisons_VectorBoundaries(int rowCount)
    {
        // Test double (8-byte) comparisons at SIMD boundaries
        // AVX2 processes 4 doubles (256 bits) per vector
        
        // Arrange
        var data = CreateTestData(rowCount);

        // Act - Test various predicates
        var count1 = data.AsQueryable().Where(x => x.Score > 500.0).Count();
        var count2 = data.AsQueryable().Where(x => x.Score < 200.0).Count();
        var count3 = data.AsQueryable().Where(x => x.Score > 100.0 && x.Score < 900.0).Count();

        // Verify with LINQ
        var expected1 = Enumerable.Range(0, rowCount).Count(i => i / 100.0 > 500.0);
        var expected2 = Enumerable.Range(0, rowCount).Count(i => i / 100.0 < 200.0);
        var expected3 = Enumerable.Range(0, rowCount).Count(i => i / 100.0 > 100.0 && i / 100.0 < 900.0);

        // Assert
        Assert.Equal(expected1, count1);
        Assert.Equal(expected2, count2);
        Assert.Equal(expected3, count3);
    }

    [Theory]
    [InlineData(100_000)]
    public void SimdBoundary_Int32Comparisons_VectorBoundaries(int rowCount)
    {
        // Test int32 (4-byte) comparisons at SIMD boundaries
        // AVX2 processes 8 int32s (256 bits) per vector
        
        // Arrange
        var data = CreateTestData(rowCount);

        // Act
        var count1 = data.AsQueryable().Where(x => x.Value > 500).Count();
        var count2 = data.AsQueryable().Where(x => x.Value < 200).Count();
        var count3 = data.AsQueryable().Where(x => x.Value > 100 && x.Value < 900).Count();

        // Verify
        var expected1 = Enumerable.Range(0, rowCount).Count(i => i % 1000 > 500);
        var expected2 = Enumerable.Range(0, rowCount).Count(i => i % 1000 < 200);
        var expected3 = Enumerable.Range(0, rowCount).Count(i => i % 1000 > 100 && i % 1000 < 900);

        // Assert
        Assert.Equal(expected1, count1);
        Assert.Equal(expected2, count2);
        Assert.Equal(expected3, count3);
    }

    [Fact]
    public void SimdBoundary_TailProcessing_AllSizesHandled()
    {
        // Test that tail (non-vector-aligned) elements are processed correctly
        // When data size is not a multiple of vector size, remaining elements
        // must be handled by scalar code
        
        var testSizes = new[] {
            // Test sizes that leave different tail sizes for different vector widths
            // AVX2 int32: 8 elements per vector
            1, 2, 3, 4, 5, 6, 7,    // Tails for 8-wide vectors
            9, 10, 11, 12, 13, 14, 15,
            17, 18, 19, 20,
            // AVX-512 int32: 16 elements per vector
            17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31
        };

        foreach (var size in testSizes)
        {
            // Arrange
            var data = CreateTestData(size);

            // Act - Various operations that use SIMD
            var count = data.AsQueryable().Where(x => x.Value > 500).Count();
            var sum = data.AsQueryable().Where(x => x.Value > 500).Sum(x => x.Value);
            var any = data.AsQueryable().Where(x => x.Value > 900).Any();

            // Verify
            var expectedCount = Enumerable.Range(0, size).Count(i => i % 1000 > 500);
            var expectedSum = Enumerable.Range(0, size).Where(i => i % 1000 > 500).Sum(i => i % 1000);
            var expectedAny = Enumerable.Range(0, size).Any(i => i % 1000 > 900);

            // Assert
            Assert.Equal(expectedCount, count);
            Assert.Equal(expectedSum, sum);
            Assert.Equal(expectedAny, any);
        }
    }

    [Theory]
    [InlineData(10_000)]
    public void SimdBoundary_BitmapOperations_VectorAligned(int rowCount)
    {
        // Test that bitmap AND/OR operations work correctly at vector boundaries
        // Bitmaps are stored as ulong[] (64-bit blocks)
        // SIMD operations process 4 or 8 ulongs at a time
        
        // Arrange
        var data = CreateTestData(rowCount);

        // Act - Multiple predicates create bitmap operations
        var count = data.AsQueryable()
            .Where(x => x.Value > 200)
            .Where(x => x.Value < 800)
            .Where(x => x.Score > 50.0)
            .Count();

        // Verify
        var expected = Enumerable.Range(0, rowCount)
            .Count(i => i % 1000 > 200 && i % 1000 < 800 && i / 100.0 > 50.0);

        // Assert
        Assert.Equal(expected, count);
    }

    [Theory]
    [InlineData(63)]   // Just before bitmap block boundary
    [InlineData(64)]   // Exactly one bitmap block (ulong = 64 bits)
    [InlineData(65)]   // Just after bitmap block boundary
    [InlineData(127)]
    [InlineData(128)]  // 2 bitmap blocks
    [InlineData(129)]
    [InlineData(255)]
    [InlineData(256)]  // 4 bitmap blocks (AVX2 vector)
    [InlineData(257)]
    public void SimdBoundary_BitmapBlockBoundaries_HandledCorrectly(int rowCount)
    {
        // Test bitmap operations at ulong block boundaries
        // Each ulong stores 64 bits, SIMD processes multiple ulongs
        
        // Arrange
        var data = CreateTestData(rowCount);

        // Act
        var count1 = data.AsQueryable().Where(x => x.Value > 500).Count();
        var count2 = data.AsQueryable().Where(x => x.Value < 500).Count();

        // Verify
        var expected1 = Enumerable.Range(0, rowCount).Count(i => i % 1000 > 500);
        var expected2 = Enumerable.Range(0, rowCount).Count(i => i % 1000 < 500);

        // Assert
        Assert.Equal(expected1, count1);
        Assert.Equal(expected2, count2);
        Assert.Equal(rowCount, count1 + count2); // Should partition all rows
    }
}
