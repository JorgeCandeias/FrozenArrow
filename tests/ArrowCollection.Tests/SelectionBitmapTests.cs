using ArrowCollection.Query;

namespace ArrowCollection.Tests;

/// <summary>
/// Tests for the SelectionBitmap pooled bitfield.
/// </summary>
public class SelectionBitmapTests
{
    [Fact]
    public void Create_InitializedToTrue_AllBitsSet()
    {
        // Arrange & Act
        using var bitmap = SelectionBitmap.Create(100, initialValue: true);

        // Assert
        Assert.Equal(100, bitmap.Length);
        Assert.Equal(100, bitmap.CountSet());
        for (int i = 0; i < 100; i++)
        {
            Assert.True(bitmap[i]);
        }
    }

    [Fact]
    public void Create_InitializedToFalse_NoBitsSet()
    {
        // Arrange & Act
        using var bitmap = SelectionBitmap.Create(100, initialValue: false);

        // Assert
        Assert.Equal(100, bitmap.Length);
        Assert.Equal(0, bitmap.CountSet());
        for (int i = 0; i < 100; i++)
        {
            Assert.False(bitmap[i]);
        }
    }

    [Fact]
    public void Clear_RemovesBit()
    {
        // Arrange
        using var bitmap = SelectionBitmap.Create(100, initialValue: true);

        // Act
        bitmap.Clear(50);

        // Assert
        Assert.False(bitmap[50]);
        Assert.Equal(99, bitmap.CountSet());
    }

    [Fact]
    public void Set_SetsBit()
    {
        // Arrange
        using var bitmap = SelectionBitmap.Create(100, initialValue: false);

        // Act
        bitmap.Set(50);

        // Assert
        Assert.True(bitmap[50]);
        Assert.Equal(1, bitmap.CountSet());
    }

    [Fact]
    public void Indexer_SetAndGet_WorksCorrectly()
    {
        // Arrange
        var bitmap = SelectionBitmap.Create(128, initialValue: false);
        try
        {
            // Act
            bitmap[0] = true;
            bitmap[63] = true;  // Last bit of first block
            bitmap[64] = true;  // First bit of second block
            bitmap[127] = true; // Last bit

            // Assert
            Assert.True(bitmap[0]);
            Assert.True(bitmap[63]);
            Assert.True(bitmap[64]);
            Assert.True(bitmap[127]);
            Assert.False(bitmap[1]);
            Assert.False(bitmap[62]);
            Assert.False(bitmap[65]);
            Assert.Equal(4, bitmap.CountSet());
        }
        finally
        {
            bitmap.Dispose();
        }
    }

    [Fact]
    public void CountSet_UsesPopcount_ForEfficiency()
    {
        // Arrange - Create a large bitmap
        using var bitmap = SelectionBitmap.Create(1_000_000, initialValue: true);

        // Clear every other bit
        for (int i = 0; i < 1_000_000; i += 2)
        {
            bitmap.Clear(i);
        }

        // Act
        var count = bitmap.CountSet();

        // Assert
        Assert.Equal(500_000, count);
    }

    [Fact]
    public void GetSelectedIndices_EnumeratesSetBits()
    {
        // Arrange
        using var bitmap = SelectionBitmap.Create(100, initialValue: false);
        bitmap.Set(5);
        bitmap.Set(10);
        bitmap.Set(50);
        bitmap.Set(99);

        // Act
        var indices = new List<int>();
        foreach (var idx in bitmap.GetSelectedIndices())
        {
            indices.Add(idx);
        }

        // Assert
        Assert.Equal([5, 10, 50, 99], indices);
    }

    [Fact]
    public void GetSelectedIndices_EmptyBitmap_NoIterations()
    {
        // Arrange
        using var bitmap = SelectionBitmap.Create(100, initialValue: false);

        // Act
        var indices = new List<int>();
        foreach (var idx in bitmap.GetSelectedIndices())
        {
            indices.Add(idx);
        }

        // Assert
        Assert.Empty(indices);
    }

    [Fact]
    public void GetSelectedIndices_AllSet_ReturnsAllIndices()
    {
        // Arrange
        using var bitmap = SelectionBitmap.Create(70, initialValue: true);

        // Act
        var indices = new List<int>();
        foreach (var idx in bitmap.GetSelectedIndices())
        {
            indices.Add(idx);
        }

        // Assert
        Assert.Equal(70, indices.Count);
        Assert.Equal(Enumerable.Range(0, 70), indices);
    }

    [Fact]
    public void And_CombinesBitmaps()
    {
        // Arrange
        using var a = SelectionBitmap.Create(100, initialValue: false);
        using var b = SelectionBitmap.Create(100, initialValue: false);
        
        // Set bits 0-49 in a
        for (int i = 0; i < 50; i++) a.Set(i);
        // Set bits 25-74 in b
        for (int i = 25; i < 75; i++) b.Set(i);

        // Act
        a.And(b);

        // Assert - Should have bits 25-49 (intersection)
        Assert.Equal(25, a.CountSet());
        Assert.False(a[24]);
        Assert.True(a[25]);
        Assert.True(a[49]);
        Assert.False(a[50]);
    }

    [Fact]
    public void Or_CombinesBitmaps()
    {
        // Arrange
        using var a = SelectionBitmap.Create(100, initialValue: false);
        using var b = SelectionBitmap.Create(100, initialValue: false);
        
        // Set bits 0-24 in a
        for (int i = 0; i < 25; i++) a.Set(i);
        // Set bits 75-99 in b
        for (int i = 75; i < 100; i++) b.Set(i);

        // Act
        a.Or(b);

        // Assert - Should have bits 0-24 and 75-99 (union)
        Assert.Equal(50, a.CountSet());
        Assert.True(a[0]);
        Assert.True(a[24]);
        Assert.False(a[25]);
        Assert.False(a[74]);
        Assert.True(a[75]);
        Assert.True(a[99]);
    }

    [Fact]
    public void Not_InvertsBitmap()
    {
        // Arrange
        using var bitmap = SelectionBitmap.Create(100, initialValue: false);
        for (int i = 0; i < 50; i++) bitmap.Set(i);

        // Act
        bitmap.Not();

        // Assert
        Assert.Equal(50, bitmap.CountSet());
        Assert.False(bitmap[0]);
        Assert.False(bitmap[49]);
        Assert.True(bitmap[50]);
        Assert.True(bitmap[99]);
    }

    [Fact]
    public void Not_HandlesPartialLastBlock()
    {
        // Arrange - 70 bits = 1 full block (64) + 6 bits
        using var bitmap = SelectionBitmap.Create(70, initialValue: true);

        // Act
        bitmap.Not();

        // Assert - All 70 bits should be false, not 128 (2 blocks worth)
        Assert.Equal(0, bitmap.CountSet());
        for (int i = 0; i < 70; i++)
        {
            Assert.False(bitmap[i]);
        }
    }

    [Fact]
    public void MemoryEfficiency_8xSmallerThanBoolArray()
    {
        // This is a documentation test - the bitmap uses ~125KB for 1M items vs ~1MB for bool[]
        const int count = 1_000_000;
        
        // bool[] would allocate count bytes = 1MB
        // SelectionBitmap allocates (count + 63) / 64 * 8 bytes = ~125KB
        
        var expectedBlocks = (count + 63) / 64;
        var expectedBytes = expectedBlocks * sizeof(ulong);
        
        Assert.Equal(15625, expectedBlocks);  // 1M / 64 rounded up
        Assert.Equal(125000, expectedBytes);  // ~125KB vs 1MB for bool[]
    }
}
