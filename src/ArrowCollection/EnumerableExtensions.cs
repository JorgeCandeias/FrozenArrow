namespace ArrowCollection;

/// <summary>
/// Extension methods for converting IEnumerable to ArrowCollection collections.
/// </summary>
public static class EnumerableExtensions
{
    /// <summary>
    /// Converts an IEnumerable to an ArrowCollection frozen collection.
    /// The collection stores the data using Apache Arrow columnar format.
    /// </summary>
    /// <typeparam name="T">The type of items in the collection. Must be marked with [ArrowRecord] and have properties marked with [ArrowArray].</typeparam>
    /// <param name="source">The source enumerable.</param>
    /// <returns>A frozen ArrowCollection.</returns>
    /// <remarks>
    /// The returned collection implements IDisposable and should be disposed when no longer needed to free unmanaged resources.
    /// All DateTime values are stored as UTC timestamps in the Arrow format.
    /// This method requires the type T to be processed by the ArrowCollection source generator at compile time.
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown when source is null.</exception>
    /// <exception cref="InvalidOperationException">Thrown when the type T has not been processed by the source generator (missing [ArrowRecord] attribute or no [ArrowArray] properties).</exception>
    public static ArrowCollection<T> ToArrowCollection<T>(this IEnumerable<T> source) where T : new()
    {
        ArgumentNullException.ThrowIfNull(source);

        // Look up the generated factory for this type
        // The factory is registered by the source generator at compile time
        if (!ArrowCollectionFactoryRegistry.TryGetFactory<T>(out var factory))
        {
            throw new InvalidOperationException(
                $"Type {typeof(T).Name} is not a valid ArrowRecord type. " +
                $"Ensure the type is marked with [{nameof(ArrowRecordAttribute)}] and has at least one property marked with [{nameof(ArrowArrayAttribute)}]. " +
                $"The {nameof(ArrowCollection<>)} source generator must process the type at compile time.");
        }

        return factory!(source);
    }
}
