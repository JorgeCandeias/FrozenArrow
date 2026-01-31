using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;
using System.Collections.Immutable;
using System.Text;

namespace ArrowCollection.Generators;

/// <summary>
/// Incremental source generator that generates optimized ArrowCollection implementations
/// for types marked with [ArrowRecord] attribute.
/// </summary>
[Generator]
public sealed class ArrowCollectionGenerator : IIncrementalGenerator
{
    private const string ArrowRecordAttributeName = "ArrowCollection.ArrowRecordAttribute";
    private const string ArrowArrayAttributeName = "ArrowCollection.ArrowArrayAttribute";

    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        // Find all class declarations with [ArrowRecord] attribute
        var classDeclarations = context.SyntaxProvider
            .CreateSyntaxProvider(
                predicate: static (node, _) => IsArrowRecordCandidate(node),
                transform: static (ctx, _) => GetArrowRecordInfo(ctx))
            .Where(static info => info is not null)
            .Select(static (info, _) => info!);

        // Combine with compilation for diagnostics
        var compilationAndClasses = context.CompilationProvider.Combine(classDeclarations.Collect());

        // Generate source for each ArrowRecord type
        context.RegisterSourceOutput(compilationAndClasses, static (spc, source) =>
        {
            var (compilation, arrowRecords) = source;
            Execute(spc, compilation, arrowRecords);
        });
    }

    private static bool IsArrowRecordCandidate(SyntaxNode node)
    {
        return node is ClassDeclarationSyntax classDecl &&
               classDecl.AttributeLists.Count > 0;
    }

    private static ArrowRecordInfo? GetArrowRecordInfo(GeneratorSyntaxContext context)
    {
        var classDecl = (ClassDeclarationSyntax)context.Node;
        var symbol = context.SemanticModel.GetDeclaredSymbol(classDecl);

        if (symbol is null)
            return null;

        // Check if it has the ArrowRecord attribute
        var hasArrowRecordAttribute = symbol.GetAttributes()
            .Any(attr => attr.AttributeClass?.ToDisplayString() == ArrowRecordAttributeName);

        if (!hasArrowRecordAttribute)
            return null;

        // Collect properties marked with ArrowArray attribute
        var properties = new List<ArrowPropertyInfo>();
        foreach (var member in symbol.GetMembers())
        {
            if (member is IPropertySymbol property &&
                property.DeclaredAccessibility == Accessibility.Public &&
                property.GetMethod is not null &&
                property.SetMethod is not null)
            {
                var hasArrowArrayAttribute = property.GetAttributes()
                    .Any(attr => attr.AttributeClass?.ToDisplayString() == ArrowArrayAttributeName);

                if (hasArrowArrayAttribute)
                {
                    var propertyType = property.Type;
                    var isNullable = propertyType.NullableAnnotation == NullableAnnotation.Annotated ||
                                     (propertyType is INamedTypeSymbol namedType &&
                                      namedType.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T);

                    var underlyingType = GetUnderlyingType(propertyType);

                    properties.Add(new ArrowPropertyInfo(
                        property.Name,
                        propertyType.ToDisplayString(),
                        underlyingType,
                        isNullable));
                }
            }
        }

        if (properties.Count == 0)
            return null;

        var namespaceName = symbol.ContainingNamespace.IsGlobalNamespace
            ? null
            : symbol.ContainingNamespace.ToDisplayString();

        return new ArrowRecordInfo(
            symbol.Name,
            namespaceName,
            symbol.ToDisplayString(),
            properties);
    }

    private static string GetUnderlyingType(ITypeSymbol type)
    {
        // Handle Nullable<T>
        if (type is INamedTypeSymbol namedType &&
            namedType.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T &&
            namedType.TypeArguments.Length == 1)
        {
            return namedType.TypeArguments[0].ToDisplayString();
        }

        // Handle nullable reference types
        if (type.NullableAnnotation == NullableAnnotation.Annotated && type.OriginalDefinition is INamedTypeSymbol)
        {
            return type.WithNullableAnnotation(NullableAnnotation.NotAnnotated).ToDisplayString();
        }

        return type.ToDisplayString();
    }

    private static void Execute(SourceProductionContext context, Compilation compilation, ImmutableArray<ArrowRecordInfo> arrowRecords)
    {
        if (arrowRecords.IsDefaultOrEmpty)
            return;

        // Generate implementation for each ArrowRecord type
        foreach (var record in arrowRecords.Distinct())
        {
            var source = GenerateArrowCollectionImplementation(record);
            context.AddSource($"ArrowCollection_{record.ClassName}.g.cs", SourceText.From(source, Encoding.UTF8));
        }

        // Generate the factory registry for this assembly
        var registrySource = GenerateFactoryRegistry(arrowRecords);
        context.AddSource("ArrowCollectionFactoryRegistry.g.cs", SourceText.From(registrySource, Encoding.UTF8));

        // Generate the module initializer to register factories
        var moduleInitializerSource = GenerateModuleInitializer(arrowRecords);
        context.AddSource("ArrowCollectionModuleInitializer.g.cs", SourceText.From(moduleInitializerSource, Encoding.UTF8));
    }

    private static string GenerateArrowCollectionImplementation(ArrowRecordInfo record)
    {
        var sb = new StringBuilder();
        sb.AppendLine("// <auto-generated />");
        sb.AppendLine("#nullable enable");
        sb.AppendLine();
        sb.AppendLine("using Apache.Arrow;");
        sb.AppendLine("using Apache.Arrow.Memory;");
        sb.AppendLine("using Apache.Arrow.Types;");
        sb.AppendLine("using System.Collections;");
        sb.AppendLine();

        if (record.Namespace is not null)
        {
            sb.AppendLine($"namespace {record.Namespace}");
            sb.AppendLine("{");
        }

        var indent = record.Namespace is not null ? "    " : "";

        // Generate the concrete ArrowCollection implementation
        sb.AppendLine($"{indent}internal sealed class GeneratedArrowCollection_{record.ClassName} : global::ArrowCollection.ArrowCollection<{record.FullTypeName}>");
        sb.AppendLine($"{indent}{{");
        sb.AppendLine($"{indent}    internal GeneratedArrowCollection_{record.ClassName}(RecordBatch recordBatch, int count)");
        sb.AppendLine($"{indent}        : base(recordBatch, count)");
        sb.AppendLine($"{indent}    {{");
        sb.AppendLine($"{indent}    }}");
        sb.AppendLine();

        // Generate CreateItem override
        sb.AppendLine($"{indent}    protected override {record.FullTypeName} CreateItem(RecordBatch recordBatch, int index)");
        sb.AppendLine($"{indent}    {{");
        sb.AppendLine($"{indent}        var item = new {record.FullTypeName}();");
        sb.AppendLine();

        for (int i = 0; i < record.Properties.Count; i++)
        {
            var prop = record.Properties[i];
            GeneratePropertyRead(sb, prop, i, indent + "        ");
        }

        sb.AppendLine();
        sb.AppendLine($"{indent}        return item;");
        sb.AppendLine($"{indent}    }}");
        sb.AppendLine($"{indent}}}");

        // Generate the static builder class
        sb.AppendLine();
        sb.AppendLine($"{indent}internal static class ArrowCollectionBuilder_{record.ClassName}");
        sb.AppendLine($"{indent}{{");
        sb.AppendLine($"{indent}    internal static global::ArrowCollection.ArrowCollection<{record.FullTypeName}> Create(global::System.Collections.Generic.IEnumerable<{record.FullTypeName}> source)");
        sb.AppendLine($"{indent}    {{");
        sb.AppendLine($"{indent}        var items = source is global::System.Collections.Generic.ICollection<{record.FullTypeName}> col ? col : new global::System.Collections.Generic.List<{record.FullTypeName}>(source);");
        sb.AppendLine($"{indent}        var count = items.Count;");
        sb.AppendLine();
        sb.AppendLine($"{indent}        // Build Arrow schema");
        sb.AppendLine($"{indent}        var fields = new global::System.Collections.Generic.List<Field>");
        sb.AppendLine($"{indent}        {{");

        for (int i = 0; i < record.Properties.Count; i++)
        {
            var prop = record.Properties[i];
            var arrowType = GetArrowTypeExpression(prop.UnderlyingTypeName);
            var nullable = prop.IsNullable ? "true" : "false";
            var comma = i < record.Properties.Count - 1 ? "," : "";
            sb.AppendLine($"{indent}            new Field(\"{prop.PropertyName}\", {arrowType}, {nullable}){comma}");
        }

        sb.AppendLine($"{indent}        }};");
        sb.AppendLine($"{indent}        var schema = new Schema(fields, null);");
        sb.AppendLine();
        sb.AppendLine($"{indent}        // Build Arrow arrays");
        sb.AppendLine($"{indent}        var allocator = new NativeMemoryAllocator();");
        sb.AppendLine($"{indent}        var arrays = new global::System.Collections.Generic.List<IArrowArray>();");
        sb.AppendLine();

        // Generate array builders for each property
        for (int i = 0; i < record.Properties.Count; i++)
        {
            var prop = record.Properties[i];
            GenerateArrayBuilder(sb, prop, i, indent + "        ");
            sb.AppendLine();
        }

        sb.AppendLine($"{indent}        // Create record batch");
        sb.AppendLine($"{indent}        var recordBatch = new RecordBatch(schema, arrays, count);");
        sb.AppendLine();
        sb.AppendLine($"{indent}        return new GeneratedArrowCollection_{record.ClassName}(recordBatch, count);");
        sb.AppendLine($"{indent}    }}");
        sb.AppendLine($"{indent}}}");

        if (record.Namespace is not null)
        {
            sb.AppendLine("}");
        }

        return sb.ToString();
    }

    private static void GeneratePropertyRead(StringBuilder sb, ArrowPropertyInfo prop, int columnIndex, string indent)
    {
        var arrayType = GetArrowArrayType(prop.UnderlyingTypeName);
        var varName = $"array{columnIndex}";

        sb.AppendLine($"{indent}var {varName} = (recordBatch.Column({columnIndex}) as {arrayType})!;");

        if (prop.IsNullable)
        {
            sb.AppendLine($"{indent}if (!{varName}.IsNull(index))");
            sb.AppendLine($"{indent}{{");
            sb.AppendLine($"{indent}    item.{prop.PropertyName} = {GetValueExtraction(varName, prop.UnderlyingTypeName)};");
            sb.AppendLine($"{indent}}}");
        }
        else
        {
            sb.AppendLine($"{indent}item.{prop.PropertyName} = {GetValueExtraction(varName, prop.UnderlyingTypeName)};");
        }
    }

    private static string GetValueExtraction(string varName, string underlyingType)
    {
        return underlyingType switch
        {
            "int" => $"{varName}.GetValue(index)!.Value",
            "long" => $"{varName}.GetValue(index)!.Value",
            "short" => $"{varName}.GetValue(index)!.Value",
            "sbyte" => $"{varName}.GetValue(index)!.Value",
            "uint" => $"{varName}.GetValue(index)!.Value",
            "ulong" => $"{varName}.GetValue(index)!.Value",
            "ushort" => $"{varName}.GetValue(index)!.Value",
            "byte" => $"{varName}.GetValue(index)!.Value",
            "float" => $"{varName}.GetValue(index)!.Value",
            "double" => $"{varName}.GetValue(index)!.Value",
            "bool" => $"{varName}.GetValue(index)!.Value",
            "string" => $"{varName}.GetString(index)!",
            "System.DateTime" => $"global::System.DateTimeOffset.FromUnixTimeMilliseconds({varName}.GetValue(index)!.Value).DateTime",
            _ => throw new NotSupportedException($"Type {underlyingType} is not supported.")
        };
    }

    private static void GenerateArrayBuilder(StringBuilder sb, ArrowPropertyInfo prop, int index, string indent)
    {
        var builderType = GetArrowBuilderType(prop.UnderlyingTypeName);
        var builderVarName = $"builder{index}";

        if (prop.UnderlyingTypeName == "System.DateTime")
        {
            sb.AppendLine($"{indent}var {builderVarName} = new {builderType}(new TimestampType(TimeUnit.Millisecond, global::System.TimeZoneInfo.Utc)).Reserve(count);");
        }
        else
        {
            sb.AppendLine($"{indent}var {builderVarName} = new {builderType}().Reserve(count);");
        }

        sb.AppendLine($"{indent}foreach (var item in items)");
        sb.AppendLine($"{indent}{{");

        if (prop.IsNullable)
        {
            sb.AppendLine($"{indent}    var value{index} = item.{prop.PropertyName};");
            sb.AppendLine($"{indent}    if (value{index} == null)");
            sb.AppendLine($"{indent}        {builderVarName}.AppendNull();");
            sb.AppendLine($"{indent}    else");

            if (prop.UnderlyingTypeName == "System.DateTime")
            {
                sb.AppendLine($"{indent}        {builderVarName}.Append(new global::System.DateTimeOffset(value{index}.Value, global::System.TimeSpan.Zero));");
            }
            else if (prop.UnderlyingTypeName == "string")
            {
                // String is a reference type, doesn't need .Value
                sb.AppendLine($"{indent}        {builderVarName}.Append(value{index});");
            }
            else
            {
                sb.AppendLine($"{indent}        {builderVarName}.Append(value{index}.Value);");
            }
        }
        else
        {
            if (prop.UnderlyingTypeName == "System.DateTime")
            {
                sb.AppendLine($"{indent}    {builderVarName}.Append(new global::System.DateTimeOffset(item.{prop.PropertyName}, global::System.TimeSpan.Zero));");
            }
            else if (prop.UnderlyingTypeName == "string")
            {
                // String is a reference type, handle null as empty or null
                sb.AppendLine($"{indent}    var value{index} = item.{prop.PropertyName};");
                sb.AppendLine($"{indent}    if (value{index} == null)");
                sb.AppendLine($"{indent}        {builderVarName}.AppendNull();");
                sb.AppendLine($"{indent}    else");
                sb.AppendLine($"{indent}        {builderVarName}.Append(value{index});");
            }
            else
            {
                sb.AppendLine($"{indent}    {builderVarName}.Append(item.{prop.PropertyName});");
            }
        }

        sb.AppendLine($"{indent}}}");
        sb.AppendLine($"{indent}arrays.Add({builderVarName}.Build(allocator));");
    }

    private static string GetArrowTypeExpression(string underlyingType)
    {
        return underlyingType switch
        {
            "int" => "Int32Type.Default",
            "long" => "Int64Type.Default",
            "short" => "Int16Type.Default",
            "sbyte" => "Int8Type.Default",
            "uint" => "UInt32Type.Default",
            "ulong" => "UInt64Type.Default",
            "ushort" => "UInt16Type.Default",
            "byte" => "UInt8Type.Default",
            "float" => "FloatType.Default",
            "double" => "DoubleType.Default",
            "bool" => "BooleanType.Default",
            "string" => "StringType.Default",
            "System.DateTime" => "new TimestampType(TimeUnit.Millisecond, global::System.TimeZoneInfo.Utc)",
            _ => throw new NotSupportedException($"Type {underlyingType} is not supported.")
        };
    }

    private static string GetArrowArrayType(string underlyingType)
    {
        return underlyingType switch
        {
            "int" => "Int32Array",
            "long" => "Int64Array",
            "short" => "Int16Array",
            "sbyte" => "Int8Array",
            "uint" => "UInt32Array",
            "ulong" => "UInt64Array",
            "ushort" => "UInt16Array",
            "byte" => "UInt8Array",
            "float" => "FloatArray",
            "double" => "DoubleArray",
            "bool" => "BooleanArray",
            "string" => "StringArray",
            "System.DateTime" => "TimestampArray",
            _ => throw new NotSupportedException($"Type {underlyingType} is not supported.")
        };
    }

    private static string GetArrowBuilderType(string underlyingType)
    {
        return underlyingType switch
        {
            "int" => "Int32Array.Builder",
            "long" => "Int64Array.Builder",
            "short" => "Int16Array.Builder",
            "sbyte" => "Int8Array.Builder",
            "uint" => "UInt32Array.Builder",
            "ulong" => "UInt64Array.Builder",
            "ushort" => "UInt16Array.Builder",
            "byte" => "UInt8Array.Builder",
            "float" => "FloatArray.Builder",
            "double" => "DoubleArray.Builder",
            "bool" => "BooleanArray.Builder",
            "string" => "StringArray.Builder",
            "System.DateTime" => "TimestampArray.Builder",
            _ => throw new NotSupportedException($"Type {underlyingType} is not supported.")
        };
    }

    private static string GenerateFactoryRegistry(ImmutableArray<ArrowRecordInfo> arrowRecords)
    {
        var sb = new StringBuilder();
        sb.AppendLine("// <auto-generated />");
        sb.AppendLine("#nullable enable");
        sb.AppendLine();
        sb.AppendLine("namespace ArrowCollection");
        sb.AppendLine("{");
        sb.AppendLine("    internal static partial class GeneratedArrowCollectionFactories");
        sb.AppendLine("    {");
        sb.AppendLine("        private static readonly global::System.Collections.Generic.Dictionary<global::System.Type, global::System.Delegate> _factories = new()");
        sb.AppendLine("        {");

        foreach (var record in arrowRecords.Distinct())
        {
            var builderNamespace = record.Namespace is not null ? $"global::{record.Namespace}." : "global::";
            sb.AppendLine($"            {{ typeof({record.FullTypeName}), new global::System.Func<global::System.Collections.Generic.IEnumerable<{record.FullTypeName}>, global::ArrowCollection.ArrowCollection<{record.FullTypeName}>>({builderNamespace}ArrowCollectionBuilder_{record.ClassName}.Create) }},");
        }

        sb.AppendLine("        };");
        sb.AppendLine();
        sb.AppendLine("        internal static bool TryGetFactory<T>(out global::System.Func<global::System.Collections.Generic.IEnumerable<T>, global::ArrowCollection.ArrowCollection<T>>? factory) where T : new()");
        sb.AppendLine("        {");
        sb.AppendLine("            if (_factories.TryGetValue(typeof(T), out var factoryDelegate))");
        sb.AppendLine("            {");
        sb.AppendLine("                factory = (global::System.Func<global::System.Collections.Generic.IEnumerable<T>, global::ArrowCollection.ArrowCollection<T>>)factoryDelegate;");
        sb.AppendLine("                return true;");
        sb.AppendLine("            }");
        sb.AppendLine("            factory = null;");
        sb.AppendLine("            return false;");
        sb.AppendLine("        }");
        sb.AppendLine("    }");
        sb.AppendLine("}");

        return sb.ToString();
    }

    private static string GenerateModuleInitializer(ImmutableArray<ArrowRecordInfo> arrowRecords)
    {
        var sb = new StringBuilder();
        sb.AppendLine("// <auto-generated />");
        sb.AppendLine("#nullable enable");
        sb.AppendLine();
        sb.AppendLine("namespace ArrowCollection");
        sb.AppendLine("{");
        sb.AppendLine("    internal static class ArrowCollectionModuleInitializer");
        sb.AppendLine("    {");
        sb.AppendLine("        [global::System.Runtime.CompilerServices.ModuleInitializer]");
        sb.AppendLine("        internal static void Initialize()");
        sb.AppendLine("        {");

        foreach (var record in arrowRecords.Distinct())
        {
            var builderNamespace = record.Namespace is not null ? $"global::{record.Namespace}." : "global::";
            sb.AppendLine($"            global::ArrowCollection.ArrowCollectionFactoryRegistry.Register<{record.FullTypeName}>({builderNamespace}ArrowCollectionBuilder_{record.ClassName}.Create);");
        }

        sb.AppendLine("        }");
        sb.AppendLine("    }");
        sb.AppendLine("}");

        return sb.ToString();
    }
}

/// <summary>
/// Information about a type marked with [ArrowRecord].
/// </summary>
internal sealed class ArrowRecordInfo : IEquatable<ArrowRecordInfo>
{
    public ArrowRecordInfo(string className, string? namespaceName, string fullTypeName, List<ArrowPropertyInfo> properties)
    {
        ClassName = className;
        Namespace = namespaceName;
        FullTypeName = fullTypeName;
        Properties = properties;
    }

    public string ClassName { get; }
    public string? Namespace { get; }
    public string FullTypeName { get; }
    public List<ArrowPropertyInfo> Properties { get; }

    public bool Equals(ArrowRecordInfo? other)
    {
        if (other is null) return false;
        return FullTypeName == other.FullTypeName;
    }

    public override bool Equals(object? obj) => Equals(obj as ArrowRecordInfo);
    public override int GetHashCode() => FullTypeName.GetHashCode();
}

/// <summary>
/// Information about a property marked with [ArrowArray].
/// </summary>
internal sealed class ArrowPropertyInfo
{
    public ArrowPropertyInfo(string propertyName, string propertyTypeName, string underlyingTypeName, bool isNullable)
    {
        PropertyName = propertyName;
        PropertyTypeName = propertyTypeName;
        UnderlyingTypeName = underlyingTypeName;
        IsNullable = isNullable;
    }

    public string PropertyName { get; }
    public string PropertyTypeName { get; }
    public string UnderlyingTypeName { get; }
    public bool IsNullable { get; }
}
