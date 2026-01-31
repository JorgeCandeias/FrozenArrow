using Microsoft.CodeAnalysis;

namespace ArrowCollection.Generators;

/// <summary>
/// Diagnostic descriptors for the ArrowCollection source generator.
/// </summary>
internal static class DiagnosticDescriptors
{
    public static readonly DiagnosticDescriptor NoArrowArrayProperties = new(
        id: "ARROWCOL001",
        title: "No ArrowArray properties",
        messageFormat: "Type '{0}' marked with [ArrowRecord] has no properties marked with [ArrowArray]",
        category: "ArrowCollection",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor UnsupportedPropertyType = new(
        id: "ARROWCOL002",
        title: "Unsupported property type",
        messageFormat: "Property '{0}' on type '{1}' has unsupported type '{2}'. Supported types: int, long, short, sbyte, uint, ulong, ushort, byte, float, double, bool, string, DateTime and their nullable variants.",
        category: "ArrowCollection",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor MissingParameterlessConstructor = new(
        id: "ARROWCOL003",
        title: "Missing parameterless constructor",
        messageFormat: "Type '{0}' marked with [ArrowRecord] must have a public parameterless constructor",
        category: "ArrowCollection",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor ManualPropertyNotSupported = new(
        id: "ARROWCOL004",
        title: "Manual property not supported",
        messageFormat: "ArrowArrayAttribute on property '{0}' is not supported because it is not an auto-property. Use the attribute on a field instead.",
        category: "ArrowCollection",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);
}
