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

        // Collect fields and properties marked with ArrowArray attribute
        var fields = new List<ArrowFieldInfo>();
        var diagnostics = new List<(DiagnosticDescriptor Descriptor, Location Location, object[] Args)>();

        foreach (var member in symbol.GetMembers())
        {
            // Handle fields directly marked with [ArrowArray]
            if (member is IFieldSymbol field && !IsCompilerGeneratedBackingField(field))
            {
                var hasArrowArrayAttribute = field.GetAttributes()
                    .Any(attr => attr.AttributeClass?.ToDisplayString() == ArrowArrayAttributeName);

                if (hasArrowArrayAttribute)
                {
                    var fieldType = field.Type;
                    var isNullable = fieldType.NullableAnnotation == NullableAnnotation.Annotated ||
                                     (fieldType is INamedTypeSymbol namedType &&
                                      namedType.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T);

                    var underlyingType = GetUnderlyingType(fieldType);

                    fields.Add(new ArrowFieldInfo(
                        field.Name,
                        field.Name, // Field name is also the backing field name
                        fieldType.ToDisplayString(),
                        underlyingType,
                        isNullable));
                }
            }
            // Handle properties marked with [ArrowArray] (including [field: ArrowArray] syntax)
            else if (member is IPropertySymbol property)
            {
                // Check for attribute on property or on its backing field
                var hasArrowArrayOnProperty = property.GetAttributes()
                    .Any(attr => attr.AttributeClass?.ToDisplayString() == ArrowArrayAttributeName);

                if (hasArrowArrayOnProperty)
                {
                    // Check if this is an auto-property by looking for synthesized backing field
                    var isAutoProperty = IsAutoProperty(property);

                    if (!isAutoProperty)
                    {
                        // Manual property - emit diagnostic
                        var location = property.Locations.FirstOrDefault() ?? Location.None;
                        diagnostics.Add((DiagnosticDescriptors.ManualPropertyNotSupported, location, new object[] { property.Name }));
                        continue;
                    }

                    // Auto-property - use its backing field
                    var backingFieldName = $"<{property.Name}>k__BackingField";
                    var propertyType = property.Type;
                    var isNullable = propertyType.NullableAnnotation == NullableAnnotation.Annotated ||
                                     (propertyType is INamedTypeSymbol namedType &&
                                      namedType.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T);

                    var underlyingType = GetUnderlyingType(propertyType);

                    fields.Add(new ArrowFieldInfo(
                        property.Name,
                        backingFieldName,
                        propertyType.ToDisplayString(),
                        underlyingType,
                        isNullable));
                }
            }
        }

        // Return null if there are validation errors - they'll be reported during Execute
        if (fields.Count == 0 && diagnostics.Count == 0)
            return null;

        var namespaceName = symbol.ContainingNamespace.IsGlobalNamespace
            ? null
            : symbol.ContainingNamespace.ToDisplayString();

        return new ArrowRecordInfo(
            symbol.Name,
            namespaceName,
            symbol.ToDisplayString(),
            fields,
            diagnostics);
    }

    private static bool IsCompilerGeneratedBackingField(IFieldSymbol field)
    {
        // Compiler-generated backing fields have names like <PropertyName>k__BackingField
        // and have the CompilerGenerated attribute
        return field.Name.StartsWith("<") && field.Name.EndsWith(">k__BackingField") &&
               field.GetAttributes().Any(attr =>
                   attr.AttributeClass?.ToDisplayString() == "System.Runtime.CompilerServices.CompilerGeneratedAttribute");
    }

    private static bool IsAutoProperty(IPropertySymbol property)
    {
        // An auto-property has both getter and setter (or is get-only with init)
        // and neither accessor has user-defined code (they're compiler-generated)
        
        // Must have a getter
        if (property.GetMethod is null)
            return false;

        // Check if the getter is compiler-synthesized
        // Auto-properties have synthesized accessors that are associated with a backing field
        var getter = property.GetMethod;
        
        // If the property has an associated field, it's an auto-property
        // We can check this by looking at the containing type's members for the backing field
        var backingFieldName = $"<{property.Name}>k__BackingField";
        var containingType = property.ContainingType;
        
        foreach (var member in containingType.GetMembers())
        {
            if (member is IFieldSymbol field && field.Name == backingFieldName)
            {
                // Found the backing field - check if it's compiler-generated
                return field.IsImplicitlyDeclared || 
                       field.GetAttributes().Any(attr =>
                           attr.AttributeClass?.ToDisplayString() == "System.Runtime.CompilerServices.CompilerGeneratedAttribute");
            }
        }

        return false;
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

        // Report diagnostics first
        foreach (var record in arrowRecords)
        {
            foreach (var (descriptor, location, args) in record.Diagnostics)
            {
                context.ReportDiagnostic(Diagnostic.Create(descriptor, location, args));
            }
        }

        // Filter to only valid records (those with fields and no errors)
        var validRecords = arrowRecords
            .Where(r => r.Fields.Count > 0 && r.Diagnostics.Count == 0)
            .Distinct()
            .ToList();

        if (validRecords.Count == 0)
            return;

        // Generate implementation for each ArrowRecord type
        foreach (var record in validRecords)
        {
            var source = GenerateArrowCollectionImplementation(record);
            context.AddSource($"ArrowCollection_{record.ClassName}.g.cs", SourceText.From(source, Encoding.UTF8));
        }

        // Generate the factory registry for this assembly
        var registrySource = GenerateFactoryRegistry(validRecords);
        context.AddSource("ArrowCollectionFactoryRegistry.g.cs", SourceText.From(registrySource, Encoding.UTF8));

        // Generate the module initializer to register factories
        var moduleInitializerSource = GenerateModuleInitializer(validRecords);
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
        sb.AppendLine("using System.Reflection;");
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
        
        // Generate static field accessors (cached for performance)
        for (int i = 0; i < record.Fields.Count; i++)
        {
            var field = record.Fields[i];
            var escapedBackingFieldName = EscapeStringLiteral(field.BackingFieldName);
            sb.AppendLine($"{indent}    private static readonly global::System.Action<{record.FullTypeName}, {field.FieldTypeName}> _setter{i} = GetSetter{i}();");
        }
        sb.AppendLine();

        // Generate static methods to get setters
        for (int i = 0; i < record.Fields.Count; i++)
        {
            var field = record.Fields[i];
            var escapedBackingFieldName = EscapeStringLiteral(field.BackingFieldName);
            sb.AppendLine($"{indent}    private static global::System.Action<{record.FullTypeName}, {field.FieldTypeName}> GetSetter{i}()");
            sb.AppendLine($"{indent}    {{");
            sb.AppendLine($"{indent}        var fieldInfo = typeof({record.FullTypeName}).GetField(\"{escapedBackingFieldName}\", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)!;");
            sb.AppendLine($"{indent}        return global::ArrowCollection.FieldAccessor.GetSetter<{record.FullTypeName}, {field.FieldTypeName}>(fieldInfo);");
            sb.AppendLine($"{indent}    }}");
            sb.AppendLine();
        }

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

        for (int i = 0; i < record.Fields.Count; i++)
        {
            var field = record.Fields[i];
            GenerateFieldRead(sb, field, i, indent + "        ");
        }

        sb.AppendLine();
        sb.AppendLine($"{indent}        return item;");
        sb.AppendLine($"{indent}    }}");
        sb.AppendLine($"{indent}}}");

        // Generate the static builder class
        sb.AppendLine();
        sb.AppendLine($"{indent}internal static class ArrowCollectionBuilder_{record.ClassName}");
        sb.AppendLine($"{indent}{{");
        
        // Generate static field getters (cached for performance)
        for (int i = 0; i < record.Fields.Count; i++)
        {
            var field = record.Fields[i];
            sb.AppendLine($"{indent}    private static readonly global::System.Func<{record.FullTypeName}, {field.FieldTypeName}> _getter{i} = GetGetter{i}();");
        }
        sb.AppendLine();

        // Generate static methods to get getters
        for (int i = 0; i < record.Fields.Count; i++)
        {
            var field = record.Fields[i];
            var escapedBackingFieldName = EscapeStringLiteral(field.BackingFieldName);
            sb.AppendLine($"{indent}    private static global::System.Func<{record.FullTypeName}, {field.FieldTypeName}> GetGetter{i}()");
            sb.AppendLine($"{indent}    {{");
            sb.AppendLine($"{indent}        var fieldInfo = typeof({record.FullTypeName}).GetField(\"{escapedBackingFieldName}\", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)!;");
            sb.AppendLine($"{indent}        return global::ArrowCollection.FieldAccessor.GetGetter<{record.FullTypeName}, {field.FieldTypeName}>(fieldInfo);");
            sb.AppendLine($"{indent}    }}");
            sb.AppendLine();
        }

        sb.AppendLine($"{indent}    internal static global::ArrowCollection.ArrowCollection<{record.FullTypeName}> Create(global::System.Collections.Generic.IEnumerable<{record.FullTypeName}> source)");
        sb.AppendLine($"{indent}    {{");
        sb.AppendLine($"{indent}        var items = source is global::System.Collections.Generic.ICollection<{record.FullTypeName}> col ? col : new global::System.Collections.Generic.List<{record.FullTypeName}>(source);");
        sb.AppendLine($"{indent}        var count = items.Count;");
        sb.AppendLine();
        sb.AppendLine($"{indent}        // Build Arrow schema");
        sb.AppendLine($"{indent}        var fields = new global::System.Collections.Generic.List<Field>");
        sb.AppendLine($"{indent}        {{");

        for (int i = 0; i < record.Fields.Count; i++)
        {
            var field = record.Fields[i];
            var arrowType = GetArrowTypeExpression(field.UnderlyingTypeName);
            var nullable = field.IsNullable ? "true" : "false";
            var comma = i < record.Fields.Count - 1 ? "," : "";
            sb.AppendLine($"{indent}            new Field(\"{field.MemberName}\", {arrowType}, {nullable}){comma}");
        }

        sb.AppendLine($"{indent}        }};");
        sb.AppendLine($"{indent}        var schema = new Schema(fields, null);");
        sb.AppendLine();
        sb.AppendLine($"{indent}        // Build Arrow arrays");
        sb.AppendLine($"{indent}        var allocator = new NativeMemoryAllocator();");
        sb.AppendLine($"{indent}        var arrays = new global::System.Collections.Generic.List<IArrowArray>();");
        sb.AppendLine();

        // Generate array builders for each field
        for (int i = 0; i < record.Fields.Count; i++)
        {
            var field = record.Fields[i];
            GenerateArrayBuilder(sb, field, i, indent + "        ");
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

    private static void GenerateFieldRead(StringBuilder sb, ArrowFieldInfo field, int columnIndex, string indent)
    {
        var arrayType = GetArrowArrayType(field.UnderlyingTypeName);
        var varName = $"array{columnIndex}";

        sb.AppendLine($"{indent}var {varName} = (recordBatch.Column({columnIndex}) as {arrayType})!;");

        if (field.IsNullable)
        {
            sb.AppendLine($"{indent}if (!{varName}.IsNull(index))");
            sb.AppendLine($"{indent}{{");
            sb.AppendLine($"{indent}    _setter{columnIndex}(item, {GetValueExtraction(varName, field.UnderlyingTypeName, field.IsNullable)});");
            sb.AppendLine($"{indent}}}");
        }
        else
        {
            sb.AppendLine($"{indent}_setter{columnIndex}(item, {GetValueExtraction(varName, field.UnderlyingTypeName, field.IsNullable)});");
        }
    }

    private static string GetValueExtraction(string varName, string underlyingType, bool isNullable)
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

    private static void GenerateArrayBuilder(StringBuilder sb, ArrowFieldInfo field, int index, string indent)
    {
        var builderType = GetArrowBuilderType(field.UnderlyingTypeName);
        var builderVarName = $"builder{index}";

        if (field.UnderlyingTypeName == "System.DateTime")
        {
            sb.AppendLine($"{indent}var {builderVarName} = new {builderType}(new TimestampType(TimeUnit.Millisecond, global::System.TimeZoneInfo.Utc)).Reserve(count);");
        }
        else
        {
            sb.AppendLine($"{indent}var {builderVarName} = new {builderType}().Reserve(count);");
        }

        sb.AppendLine($"{indent}foreach (var item in items)");
        sb.AppendLine($"{indent}{{");

        if (field.IsNullable)
        {
            sb.AppendLine($"{indent}    var value{index} = _getter{index}(item);");
            sb.AppendLine($"{indent}    if (value{index} == null)");
            sb.AppendLine($"{indent}        {builderVarName}.AppendNull();");
            sb.AppendLine($"{indent}    else");

            if (field.UnderlyingTypeName == "System.DateTime")
            {
                sb.AppendLine($"{indent}        {builderVarName}.Append(new global::System.DateTimeOffset(value{index}.Value, global::System.TimeSpan.Zero));");
            }
            else if (field.UnderlyingTypeName == "string")
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
            if (field.UnderlyingTypeName == "System.DateTime")
            {
                sb.AppendLine($"{indent}    {builderVarName}.Append(new global::System.DateTimeOffset(_getter{index}(item), global::System.TimeSpan.Zero));");
            }
            else if (field.UnderlyingTypeName == "string")
            {
                // String is a reference type, handle null as empty or null
                sb.AppendLine($"{indent}    var value{index} = _getter{index}(item);");
                sb.AppendLine($"{indent}    if (value{index} == null)");
                sb.AppendLine($"{indent}        {builderVarName}.AppendNull();");
                sb.AppendLine($"{indent}    else");
                sb.AppendLine($"{indent}        {builderVarName}.Append(value{index});");
            }
            else
            {
                sb.AppendLine($"{indent}    {builderVarName}.Append(_getter{index}(item));");
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

    private static string GenerateFactoryRegistry(List<ArrowRecordInfo> arrowRecords)
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

        foreach (var record in arrowRecords)
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

    private static string GenerateModuleInitializer(List<ArrowRecordInfo> arrowRecords)
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

        foreach (var record in arrowRecords)
        {
            var builderNamespace = record.Namespace is not null ? $"global::{record.Namespace}." : "global::";
            sb.AppendLine($"            global::ArrowCollection.ArrowCollectionFactoryRegistry.Register<{record.FullTypeName}>({builderNamespace}ArrowCollectionBuilder_{record.ClassName}.Create);");
        }

        sb.AppendLine("        }");
        sb.AppendLine("    }");
        sb.AppendLine("}");

        return sb.ToString();
    }

    private static string EscapeStringLiteral(string value)
    {
        return value.Replace("\\", "\\\\").Replace("\"", "\\\"");
    }
}

/// <summary>
/// Information about a type marked with [ArrowRecord].
/// </summary>
internal sealed class ArrowRecordInfo : IEquatable<ArrowRecordInfo>
{
    public ArrowRecordInfo(
        string className,
        string? namespaceName,
        string fullTypeName,
        List<ArrowFieldInfo> fields,
        List<(DiagnosticDescriptor Descriptor, Location Location, object[] Args)> diagnostics)
    {
        ClassName = className;
        Namespace = namespaceName;
        FullTypeName = fullTypeName;
        Fields = fields;
        Diagnostics = diagnostics;
    }

    public string ClassName { get; }
    public string? Namespace { get; }
    public string FullTypeName { get; }
    public List<ArrowFieldInfo> Fields { get; }
    public List<(DiagnosticDescriptor Descriptor, Location Location, object[] Args)> Diagnostics { get; }

    public bool Equals(ArrowRecordInfo? other)
    {
        if (other is null) return false;
        return FullTypeName == other.FullTypeName;
    }

    public override bool Equals(object? obj) => Equals(obj as ArrowRecordInfo);
    public override int GetHashCode() => FullTypeName.GetHashCode();
}

/// <summary>
/// Information about a field or auto-property backing field marked with [ArrowArray].
/// </summary>
internal sealed class ArrowFieldInfo
{
    public ArrowFieldInfo(string memberName, string backingFieldName, string fieldTypeName, string underlyingTypeName, bool isNullable)
    {
        MemberName = memberName;
        BackingFieldName = backingFieldName;
        FieldTypeName = fieldTypeName;
        UnderlyingTypeName = underlyingTypeName;
        IsNullable = isNullable;
    }

    /// <summary>
    /// The name of the field or property as declared in source code.
    /// Used for Arrow schema field naming.
    /// </summary>
    public string MemberName { get; }

    /// <summary>
    /// The name of the backing field to access via reflection.
    /// For fields, this is the same as MemberName.
    /// For auto-properties, this is the compiler-generated backing field name (e.g., "&lt;PropertyName&gt;k__BackingField").
    /// </summary>
    public string BackingFieldName { get; }

    /// <summary>
    /// The full type name of the field.
    /// </summary>
    public string FieldTypeName { get; }

    /// <summary>
    /// The underlying type name (unwrapped from Nullable if applicable).
    /// </summary>
    public string UnderlyingTypeName { get; }

    /// <summary>
    /// Whether the field is nullable.
    /// </summary>
    public bool IsNullable { get; }
}
