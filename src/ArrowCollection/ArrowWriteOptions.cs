namespace ArrowCollection;

/// <summary>
/// Options for writing and serializing Arrow collections.
/// </summary>
/// <remarks>
/// This class is provided for future extensibility (e.g., compression settings, custom metadata).
/// Currently, default options are used for all write operations.
/// </remarks>
public sealed class ArrowWriteOptions
{
    /// <summary>
    /// Gets the default write options.
    /// </summary>
    public static ArrowWriteOptions Default { get; } = new();
}
