using ArrowCollection;

Console.WriteLine("ArrowCollection - Frozen Collection Demo\n");
Console.WriteLine("================================\n");

// Example 1: Basic usage
Console.WriteLine("Example 1: Basic Usage");
Console.WriteLine("----------------------");

var weatherData = new[]
{
    new WeatherData { Id = 1, Location = "New York", Temperature = 72.5, Humidity = 65.0, Timestamp = DateTime.UtcNow, IsRaining = false },
    new WeatherData { Id = 2, Location = "London", Temperature = 60.2, Humidity = 80.5, Timestamp = DateTime.UtcNow, IsRaining = true },
    new WeatherData { Id = 3, Location = "Tokyo", Temperature = 75.8, Humidity = 70.2, Timestamp = DateTime.UtcNow, IsRaining = false },
    new WeatherData { Id = 4, Location = "Sydney", Temperature = 68.4, Humidity = 55.8, Timestamp = DateTime.UtcNow, IsRaining = false },
};

using (var collection = weatherData.ToArrowCollection())
{
    Console.WriteLine($"Created collection collection with {collection.Count} items");
    Console.WriteLine("\nEnumerating weather data:");
    
    foreach (var data in collection)
    {
        var rain = data.IsRaining ? "Rainy" : "Clear";
        Console.WriteLine($"  {data.Location}: {data.Temperature}°F, {data.Humidity}% humidity - {rain}");
    }
}

Console.WriteLine();

// Example 2: Large dataset
Console.WriteLine("Example 2: Large Dataset (Compression Demo)");
Console.WriteLine("-------------------------------------------");

var largeDataset = Enumerable.Range(1, 100_000).Select(i => new WeatherData
{
    Id = i,
    Location = $"Location_{i % 100}",
    Temperature = 60.0 + (i % 40),
    Humidity = 40.0 + (i % 50),
    Timestamp = DateTime.UtcNow.AddHours(-i),
    IsRaining = i % 3 == 0
}).ToList();

Console.WriteLine($"Created {largeDataset.Count:N0} weather readings");

using (var collection = largeDataset.ToArrowCollection())
{
    Console.WriteLine($"Compressed into collection collection: {collection.Count:N0} items");
    Console.WriteLine("\nData is stored in Apache Arrow columnar format for compression.");
    Console.WriteLine("Items are reconstructed on-the-fly during enumeration.\n");
    
    // Sample the data
    var rainyDays = collection.Where(w => w.IsRaining).Take(5);
    Console.WriteLine("Sample: First 5 rainy locations:");
    foreach (var data in rainyDays)
    {
        Console.WriteLine($"  {data.Location}: {data.Temperature}°F, {data.Humidity}% humidity");
    }
}

Console.WriteLine();

// Example 3: Multiple enumerations
Console.WriteLine("Example 3: Multiple Enumerations");
Console.WriteLine("---------------------------------");

var data3 = new[]
{
    new WeatherData { Id = 1, Location = "Seattle", Temperature = 58.5, Humidity = 75.0, Timestamp = DateTime.UtcNow, IsRaining = true },
    new WeatherData { Id = 2, Location = "Phoenix", Temperature = 95.2, Humidity = 20.5, Timestamp = DateTime.UtcNow, IsRaining = false },
    new WeatherData { Id = 3, Location = "Miami", Temperature = 85.8, Humidity = 88.2, Timestamp = DateTime.UtcNow, IsRaining = false },
};

using (var collection = data3.ToArrowCollection())
{
    // First enumeration
    Console.WriteLine("First enumeration:");
    foreach (var d in collection)
    {
        Console.WriteLine($"  {d.Location}");
    }
    
    // Second enumeration
    Console.WriteLine("\nSecond enumeration (same data):");
    foreach (var d in collection)
    {
        Console.WriteLine($"  {d.Location}");
    }
}

Console.WriteLine("\n================================");
Console.WriteLine("Demo completed!");

// Define a sample data type
[ArrowRecord]
public class WeatherData
{
    [ArrowArray]
    public int Id { get; set; }
    [ArrowArray]
    public string Location { get; set; } = string.Empty;
    [ArrowArray]
    public double Temperature { get; set; }
    [ArrowArray]
    public double Humidity { get; set; }
    [ArrowArray]
    public DateTime Timestamp { get; set; }
    [ArrowArray]
    public bool IsRaining { get; set; }
}


