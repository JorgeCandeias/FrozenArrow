using FrozenArrow;
using FrozenArrow.Query;

// Test data
[ArrowRecord]
public record TestRecord
{
    [ArrowArray] public int Id { get; init; }
    [ArrowArray] public int Value { get; init; }
    [ArrowArray] public bool IsActive { get; init; }
}

var records = new List<TestRecord>();
for (int i = 0; i < 100; i++)
{
    records.Add(new TestRecord 
    { 
        Id = i, 
        Value = i * 10,
        IsActive = i % 2 == 0 
    });
}

var data = records.ToFrozenArrow();

// Test 1: .Take(20).Count(x => x.IsActive) - Should count active in first 20
var test1 = data.AsQueryable()
    .Take(20)
    .Count(x => x.IsActive);
Console.WriteLine($"Test 1 - Take(20).Count(IsActive): {test1} (expected: 10)");

// Test 2: .Where(x => x.IsActive).Take(20).Count() - Should filter all then take 20
var test2 = data.AsQueryable()
    .Where(x => x.IsActive)
    .Take(20)
    .Count();
Console.WriteLine($"Test 2 - Where(IsActive).Take(20).Count(): {test2} (expected: 20)");

// Test 3: .Take(20).Where(x => x.IsActive).ToList() - Should take 20 then filter
var test3 = data.AsQueryable()
    .Take(20)
    .Where(x => x.IsActive)
    .ToList();
Console.WriteLine($"Test 3 - Take(20).Where(IsActive).ToList(): {test3.Count} (expected: 10)");

// Test 4: .Where(x => x.IsActive).Take(20).ToList() - Should filter all then take 20
var test4 = data.AsQueryable()
    .Where(x => x.IsActive)
    .Take(20)
    .ToList();
Console.WriteLine($"Test 4 - Where(IsActive).Take(20).ToList(): {test4.Count} (expected: 20)");

Console.WriteLine("\nAll tests completed!");
Console.WriteLine(test1 == 10 && test2 == 20 && test3.Count == 10 && test4.Count == 20 
    ? "? ALL TESTS PASSED" 
    : "? SOME TESTS FAILED");
