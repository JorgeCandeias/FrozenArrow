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

Console.WriteLine();

// Example 4: Struct support
Console.WriteLine("Example 4: Struct Support");
Console.WriteLine("--------------------------");

var sensorReadings = new[]
{
    new SensorReading { SensorId = 1, Value = 23.5, Timestamp = DateTime.UtcNow },
    new SensorReading { SensorId = 2, Value = 45.2, Timestamp = DateTime.UtcNow },
    new SensorReading { SensorId = 3, Value = 67.8, Timestamp = DateTime.UtcNow },
};

using (var structCollection = sensorReadings.ToArrowCollection())
{
    Console.WriteLine($"Created struct collection with {structCollection.Count} items");
    foreach (var reading in structCollection)
    {
        Console.WriteLine($"  Sensor {reading.SensorId}: {reading.Value}");
    }
}

Console.WriteLine();

// Example 5: Readonly struct support
Console.WriteLine("Example 5: Readonly Struct Support");
Console.WriteLine("-----------------------------------");

var immutableReadings = new[]
{
    new ImmutableReading { SensorId = 100, Value = 99.9, Timestamp = DateTime.UtcNow },
    new ImmutableReading { SensorId = 101, Value = 88.8, Timestamp = DateTime.UtcNow },
};

using (var readonlyCollection = immutableReadings.ToArrowCollection())
{
    Console.WriteLine($"Created readonly struct collection with {readonlyCollection.Count} items");
    foreach (var reading in readonlyCollection)
    {
        Console.WriteLine($"  Sensor {reading.SensorId}: {reading.Value}");
    }
}

Console.WriteLine();

// Example 6: Half-precision floats and Binary data
Console.WriteLine("Example 6: Half-Precision Floats and Binary Data");
Console.WriteLine("-------------------------------------------------");

var scientificData = new[]
{
    new ScientificMeasurement 
    { 
        Id = 1, 
        LowPrecisionValue = (Half)3.14f, 
        RawData = [0x01, 0x02, 0x03, 0x04],
        OptionalMeasurement = (Half)2.71f,
        OptionalPayload = [0xFF, 0xFE]
    },
    new ScientificMeasurement 
    { 
        Id = 2, 
        LowPrecisionValue = (Half)1.618f, 
        RawData = [0xDE, 0xAD, 0xBE, 0xEF],
        OptionalMeasurement = null,
        OptionalPayload = null
    },
    new ScientificMeasurement 
    { 
        Id = 3, 
        LowPrecisionValue = (Half)0.577f, 
        RawData = [0xCA, 0xFE],
        OptionalMeasurement = (Half)1.414f,
        OptionalPayload = [0xBA, 0xBE]
    }
};

using (var sciCollection = scientificData.ToArrowCollection())
{
    Console.WriteLine($"Created scientific data collection with {sciCollection.Count} items");
    foreach (var item in sciCollection)
    {
        var optionalVal = item.OptionalMeasurement.HasValue ? item.OptionalMeasurement.Value.ToString() : "null";
        var optionalData = item.OptionalPayload != null ? BitConverter.ToString(item.OptionalPayload) : "null";
        Console.WriteLine($"  ID {item.Id}: Half={item.LowPrecisionValue}, Binary={BitConverter.ToString(item.RawData)}, OptHalf={optionalVal}, OptBinary={optionalData}");
    }
}

Console.WriteLine("\n================================");
Console.WriteLine("Demo completed!");

// Define a sample data type (class)
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

// Define a sample data type (struct)
[ArrowRecord]
public struct SensorReading
{
    [ArrowArray]
    public int SensorId { get; set; }
    [ArrowArray]
    public double Value { get; set; }
    [ArrowArray]
    public DateTime Timestamp { get; set; }
}

// Define a readonly struct
[ArrowRecord]
public readonly struct ImmutableReading
{
    [ArrowArray]
    public int SensorId { get; init; }
    [ArrowArray]
    public double Value { get; init; }
    [ArrowArray]
    public DateTime Timestamp { get; init; }
}

// Define a type with Half (half-precision float) and Binary data
[ArrowRecord]
public class ScientificMeasurement
{
    [ArrowArray]
    public int Id { get; set; }
    [ArrowArray]
    public Half LowPrecisionValue { get; set; }
    [ArrowArray]
    public byte[] RawData { get; set; } = [];
    [ArrowArray]
    public Half? OptionalMeasurement { get; set; }
    [ArrowArray]
    public byte[]? OptionalPayload { get; set; }
}


