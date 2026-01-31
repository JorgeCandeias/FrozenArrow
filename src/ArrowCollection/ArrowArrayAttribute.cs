namespace ArrowCollection;

/// <summary>
/// Marks a property for inclusion in the Arrow columnar storage.
/// Only properties decorated with this attribute will be compressed into the collection.
/// Non-annotated properties will be assigned their default value when reconstructing items.
/// </summary>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
public sealed class ArrowArrayAttribute : Attribute
{
}
