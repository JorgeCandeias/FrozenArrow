namespace ArrowCollection;

/// <summary>
/// Specifies how to handle members in the target type that do not have corresponding columns in the source data.
/// </summary>
public enum MissingColumnBehavior
{
    /// <summary>
    /// Assign the default value for the member type when a column is missing from the source data.
    /// This is the default behavior, allowing forward-compatible schema evolution.
    /// </summary>
    UseDefault = 0,

    /// <summary>
    /// Throw an exception if the target type has members that do not have corresponding columns in the source data.
    /// Use this for strict validation scenarios.
    /// </summary>
    Throw = 1
}
