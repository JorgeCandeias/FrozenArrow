namespace FrozenArrow.Query;

/// <summary>
/// String comparison operators for SQL LIKE and equality operations.
/// Phase 8 Enhancement: String predicate support.
/// </summary>
public enum StringComparisonOperator
{
    /// <summary>
    /// Exact equality (=)
    /// </summary>
    Equal,

    /// <summary>
    /// Not equal (!=, <>)
    /// </summary>
    NotEqual,

    /// <summary>
    /// Pattern starts with (LIKE 'value%')
    /// </summary>
    StartsWith,

    /// <summary>
    /// Pattern ends with (LIKE '%value')
    /// </summary>
    EndsWith,

    /// <summary>
    /// Pattern contains (LIKE '%value%')
    /// </summary>
    Contains,

    /// <summary>
    /// Case-insensitive equality
    /// </summary>
    EqualIgnoreCase,

    /// <summary>
    /// Greater than (lexicographic comparison)
    /// </summary>
    GreaterThan,

    /// <summary>
    /// Less than (lexicographic comparison)
    /// </summary>
    LessThan,

    /// <summary>
    /// Greater than or equal (lexicographic comparison)
    /// </summary>
    GreaterThanOrEqual,

    /// <summary>
    /// Less than or equal (lexicographic comparison)
    /// </summary>
    LessThanOrEqual
}
