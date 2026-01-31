namespace ArrowCollection;

/// <summary>
/// Marks a class as eligible for use with <see cref="ArrowCollection{T}"/>.
/// Only classes decorated with this attribute can be used as the generic type argument
/// when creating Arrow collections.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class ArrowRecordAttribute : Attribute
{
}
