using Apache.Arrow;
using System.Collections.Concurrent;

namespace ArrowCollection;

/// <summary>
/// Factory registry for ArrowCollection types.
/// Factories are registered at runtime by generated module initializers.
/// </summary>
public static class ArrowCollectionFactoryRegistry
{
    private static readonly ConcurrentDictionary<Type, Delegate> _factories = new();
    private static readonly ConcurrentDictionary<Type, Delegate> _deserializationFactories = new();

    /// <summary>
    /// Registers a factory for the specified type.
    /// This method is called by generated code during module initialization.
    /// </summary>
    /// <typeparam name="T">The ArrowRecord type.</typeparam>
    /// <param name="factory">The factory delegate that creates ArrowCollection instances.</param>
    public static void Register<T>(Func<IEnumerable<T>, ArrowCollection<T>> factory) where T : new()
    {
        _factories[typeof(T)] = factory;
    }

    /// <summary>
    /// Registers a deserialization factory for the specified type.
    /// This method is called by generated code during module initialization.
    /// </summary>
    /// <typeparam name="T">The ArrowRecord type.</typeparam>
    /// <param name="factory">The factory delegate that creates ArrowCollection instances from a RecordBatch.</param>
    public static void RegisterDeserialization<T>(Func<RecordBatch, ArrowReadOptions, ArrowCollection<T>> factory) where T : new()
    {
        _deserializationFactories[typeof(T)] = factory;
    }

    /// <summary>
    /// Tries to get a factory for the specified type.
    /// </summary>
    /// <typeparam name="T">The ArrowRecord type.</typeparam>
    /// <param name="factory">The factory delegate if found; otherwise, null.</param>
    /// <returns>True if a factory was found; otherwise, false.</returns>
    internal static bool TryGetFactory<T>(out Func<IEnumerable<T>, ArrowCollection<T>>? factory) where T : new()
    {
        if (_factories.TryGetValue(typeof(T), out var factoryDelegate))
        {
            factory = (Func<IEnumerable<T>, ArrowCollection<T>>)factoryDelegate;
            return true;
        }
        factory = null;
        return false;
    }

    /// <summary>
    /// Tries to get a deserialization factory for the specified type.
    /// </summary>
    /// <typeparam name="T">The ArrowRecord type.</typeparam>
    /// <param name="factory">The factory delegate if found; otherwise, null.</param>
    /// <returns>True if a factory was found; otherwise, false.</returns>
    internal static bool TryGetDeserializationFactory<T>(out Func<RecordBatch, ArrowReadOptions, ArrowCollection<T>>? factory) where T : new()
    {
        if (_deserializationFactories.TryGetValue(typeof(T), out var factoryDelegate))
        {
            factory = (Func<RecordBatch, ArrowReadOptions, ArrowCollection<T>>)factoryDelegate;
            return true;
        }
        factory = null;
        return false;
    }
}


