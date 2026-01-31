namespace ArrowCollection;

/// <summary>
/// Marks a field or auto-property for inclusion in the Arrow columnar storage.
/// Only members decorated with this attribute will be compressed into the collection.
/// Non-annotated members will be assigned their default value when reconstructing items.
/// </summary>
/// <remarks>
/// <para>
/// This attribute can be applied to:
/// <list type="bullet">
///   <item><description>Fields (any visibility) - the field is accessed directly</description></item>
///   <item><description>Auto-properties - the compiler-generated backing field is accessed directly</description></item>
/// </list>
/// </para>
/// <para>
/// Manual properties (properties with custom getter/setter logic) are not supported.
/// For maximum performance, property accessors are always bypassed in favor of direct field access.
/// This enforces the assumption that item models only hold data, not logic.
/// </para>
/// <para>
/// The <c>[field: ArrowArray]</c> syntax on properties is supported but redundant,
/// as the backing field is always targeted regardless.
/// </para>
/// </remarks>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false, Inherited = true)]
public sealed class ArrowArrayAttribute : Attribute
{
}
