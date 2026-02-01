namespace ArrowCollection;

/// <summary>
/// Options for reading and deserializing Arrow collections.
/// </summary>
public sealed class ArrowReadOptions
{
    /// <summary>
    /// Gets the default read options.
    /// </summary>
    public static ArrowReadOptions Default { get; } = new();

    /// <summary>
    /// Gets the behavior when the source data contains columns that do not map to any member in the target type.
    /// Defaults to <see cref="UnknownColumnBehavior.Ignore"/>.
    /// </summary>
    public UnknownColumnBehavior UnknownColumns { get; init; } = UnknownColumnBehavior.Ignore;

    /// <summary>
    /// Gets the behavior when the target type has members that do not have corresponding columns in the source data.
    /// Defaults to <see cref="MissingColumnBehavior.UseDefault"/>.
    /// </summary>
    public MissingColumnBehavior MissingColumns { get; init; } = MissingColumnBehavior.UseDefault;
}
