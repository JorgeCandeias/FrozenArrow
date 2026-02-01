namespace ArrowCollection;

/// <summary>
/// Specifies how to handle columns in the source data that do not map to any member in the target type.
/// </summary>
public enum UnknownColumnBehavior
{
    /// <summary>
    /// Silently ignore columns that do not map to any member in the target type.
    /// This is the default behavior, similar to Protobuf's handling of unknown fields.
    /// </summary>
    Ignore = 0,

    /// <summary>
    /// Throw an exception if the source data contains columns that do not map to any member in the target type.
    /// Use this for strict validation scenarios.
    /// </summary>
    Throw = 1
}
