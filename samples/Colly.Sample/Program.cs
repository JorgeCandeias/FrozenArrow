using Colly;

Console.WriteLine("Colly - Frozen Collection Demo\n");
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

using (var colly = weatherData.ToColly())
{
    Console.WriteLine($"Created Colly collection with {colly.Count} items");
    Console.WriteLine("\nEnumerating weather data:");
    
    foreach (var data in colly)
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

using (var colly = largeDataset.ToColly())
{
    Console.WriteLine($"Compressed into Colly collection: {colly.Count:N0} items");
    Console.WriteLine("\nData is stored in Apache Arrow columnar format for compression.");
    Console.WriteLine("Items are reconstructed on-the-fly during enumeration.\n");
    
    // Sample the data
    var rainyDays = colly.Where(w => w.IsRaining).Take(5);
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

using (var colly = data3.ToColly())
{
    // First enumeration
    Console.WriteLine("First enumeration:");
    foreach (var d in colly)
    {
        Console.WriteLine($"  {d.Location}");
    }
    
    // Second enumeration
    Console.WriteLine("\nSecond enumeration (same data):");
    foreach (var d in colly)
    {
        Console.WriteLine($"  {d.Location}");
    }
}

Console.WriteLine("\n================================");
Console.WriteLine("Demo completed!");

// Define a sample data type
public class WeatherData
{
    public int Id { get; set; }
    public string Location { get; set; } = string.Empty;
    public double Temperature { get; set; }
    public double Humidity { get; set; }
    public DateTime Timestamp { get; set; }
    public bool IsRaining { get; set; }
}
