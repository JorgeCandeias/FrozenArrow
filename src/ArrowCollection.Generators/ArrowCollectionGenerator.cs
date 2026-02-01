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
            Execute(spc, arrowRecords);
        });
    }

    private static bool IsArrowRecordCandidate(SyntaxNode node)
    {
        return node is TypeDeclarationSyntax typeDecl &&
               (typeDecl is ClassDeclarationSyntax || 
                typeDecl is StructDeclarationSyntax || 
                typeDecl is RecordDeclarationSyntax) &&
               typeDecl.AttributeLists.Count > 0;
    }

    private static ArrowRecordInfo? GetArrowRecordInfo(GeneratorSyntaxContext context)
    {
        var typeDecl = (TypeDeclarationSyntax)context.Node;
        var symbol = context.SemanticModel.GetDeclaredSymbol(typeDecl);

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
                var arrowArrayAttribute = field.GetAttributes()
                    .FirstOrDefault(attr => attr.AttributeClass?.ToDisplayString() == ArrowArrayAttributeName);

                if (arrowArrayAttribute is not null)
                {
                    var fieldType = field.Type;
                    var isNullable = fieldType.NullableAnnotation == NullableAnnotation.Annotated ||
                                     (fieldType is INamedTypeSymbol namedType &&
                                      namedType.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T);

                    var underlyingType = GetUnderlyingType(fieldType);

                    // Extract the Name property from the attribute, if present
                    var explicitName = GetAttributeNameProperty(arrowArrayAttribute);
                    var columnName = explicitName ?? field.Name;

                    // Warn if field has no explicit Name (field naming convention issue)
                    if (explicitName is null)
                    {
                        var location = field.Locations.FirstOrDefault() ?? Location.None;
                        diagnostics.Add((DiagnosticDescriptors.FieldWithoutSerializationName, location, new object[] { field.Name }));
                    }

                    fields.Add(new ArrowFieldInfo(
                        field.Name,
                        field.Name, // Field name is also the backing field name
                        columnName,
                        fieldType.ToDisplayString(),
                        underlyingType,
                        isNullable));
                }
            }
            // Handle properties marked with [ArrowArray] (including [field: ArrowArray] syntax)
            else if (member is IPropertySymbol property)
            {
                // Check for attribute on property or on its backing field
                var arrowArrayAttribute = property.GetAttributes()
                    .FirstOrDefault(attr => attr.AttributeClass?.ToDisplayString() == ArrowArrayAttributeName);

                if (arrowArrayAttribute is not null)
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

                    // Extract the Name property from the attribute, if present
                    var explicitName = GetAttributeNameProperty(arrowArrayAttribute);
                    var columnName = explicitName ?? property.Name;

                    // Note: We don't warn for properties since they typically have good names already

                    fields.Add(new ArrowFieldInfo(
                        property.Name,
                        backingFieldName,
                        columnName,
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
            diagnostics,
            symbol.IsValueType);
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

        private static string? GetAttributeNameProperty(AttributeData attribute)
        {
            // Check named arguments first (Name = "...")
            foreach (var namedArg in attribute.NamedArguments)
            {
                if (namedArg.Key == "Name" && namedArg.Value.Value is string nameValue)
                {
                    return string.IsNullOrEmpty(nameValue) ? null : nameValue;
                }
            }
            return null;
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

        // Handle nullable reference types (including arrays)
        if (type.NullableAnnotation == NullableAnnotation.Annotated)
        {
            return type.WithNullableAnnotation(NullableAnnotation.NotAnnotated).ToDisplayString();
        }

        return type.ToDisplayString();
    }

    private static void Execute(SourceProductionContext context, ImmutableArray<ArrowRecordInfo> arrowRecords)
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
        // Note: We allow records with warnings (like ARROWCOL005), only filter out errors
        var validRecords = arrowRecords
            .Where(r => r.Fields.Count > 0 && !r.Diagnostics.Any(d => d.Descriptor.DefaultSeverity == DiagnosticSeverity.Error))
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

        // Column index mappings for deserialization (when columns may be in different order)
        sb.AppendLine($"{indent}    private readonly int[] _columnIndices;");
        sb.AppendLine();

        // Generate static field accessors (cached for performance)
        // For structs, we use RefFieldSetter to avoid copying
        for (int i = 0; i < record.Fields.Count; i++)
        {
            var field = record.Fields[i];
            if (record.IsValueType)
            {
                sb.AppendLine($"{indent}    private static readonly global::ArrowCollection.RefFieldSetter<{record.FullTypeName}, {field.FieldTypeName}> _setter{i} = GetSetter{i}();");
            }
            else
            {
                sb.AppendLine($"{indent}    private static readonly global::System.Action<{record.FullTypeName}, {field.FieldTypeName}> _setter{i} = GetSetter{i}();");
            }
        }
        sb.AppendLine();

        // Generate static methods to get setters
        for (int i = 0; i < record.Fields.Count; i++)
        {
            var field = record.Fields[i];
            var escapedBackingFieldName = EscapeStringLiteral(field.BackingFieldName);
            if (record.IsValueType)
            {
                sb.AppendLine($"{indent}    private static global::ArrowCollection.RefFieldSetter<{record.FullTypeName}, {field.FieldTypeName}> GetSetter{i}()");
                sb.AppendLine($"{indent}    {{");
                sb.AppendLine($"{indent}        var fieldInfo = typeof({record.FullTypeName}).GetField(\"{escapedBackingFieldName}\", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)!;");
                sb.AppendLine($"{indent}        return global::ArrowCollection.FieldAccessor.GetRefSetter<{record.FullTypeName}, {field.FieldTypeName}>(fieldInfo);");
                sb.AppendLine($"{indent}    }}");
            }
            else
            {
                sb.AppendLine($"{indent}    private static global::System.Action<{record.FullTypeName}, {field.FieldTypeName}> GetSetter{i}()");
                sb.AppendLine($"{indent}    {{");
                sb.AppendLine($"{indent}        var fieldInfo = typeof({record.FullTypeName}).GetField(\"{escapedBackingFieldName}\", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)!;");
                sb.AppendLine($"{indent}        return global::ArrowCollection.FieldAccessor.GetSetter<{record.FullTypeName}, {field.FieldTypeName}>(fieldInfo);");
                sb.AppendLine($"{indent}    }}");
            }
            sb.AppendLine();
        }

        // Primary constructor (used when building from IEnumerable)
        sb.AppendLine($"{indent}    internal GeneratedArrowCollection_{record.ClassName}(RecordBatch recordBatch, int count, global::ArrowCollection.ArrowCollectionBuildStatistics? buildStatistics = null)");
        sb.AppendLine($"{indent}        : base(recordBatch, count, buildStatistics)");
        sb.AppendLine($"{indent}    {{");
        sb.AppendLine($"{indent}        // Default column indices (sequential order)");
        sb.AppendLine($"{indent}        _columnIndices = new int[] {{ {string.Join(", ", Enumerable.Range(0, record.Fields.Count))} }};");
        sb.AppendLine($"{indent}    }}");
        sb.AppendLine();

        // Secondary constructor (used when deserializing with name-based column mapping)
        sb.AppendLine($"{indent}    internal GeneratedArrowCollection_{record.ClassName}(RecordBatch recordBatch, int count, int[] colIndex)");
        sb.AppendLine($"{indent}        : base(recordBatch, count, null)");
        sb.AppendLine($"{indent}    {{");
        sb.AppendLine($"{indent}        _columnIndices = colIndex;");
        sb.AppendLine($"{indent}    }}");
        sb.AppendLine();

        // Generate CreateItem override with name-based column support
        sb.AppendLine($"{indent}    protected override {record.FullTypeName} CreateItem(RecordBatch recordBatch, int index)");
        sb.AppendLine($"{indent}    {{");
        
        // For structs: use default(T) to avoid boxing
        // For classes: use RuntimeHelpers.GetUninitializedObject to support positional records
        if (record.IsValueType)
        {
            sb.AppendLine($"{indent}        var item = default({record.FullTypeName});");
        }
        else
        {
            sb.AppendLine($"{indent}        var item = ({record.FullTypeName})global::System.Runtime.CompilerServices.RuntimeHelpers.GetUninitializedObject(typeof({record.FullTypeName}));");
        }
        sb.AppendLine();

        for (int i = 0; i < record.Fields.Count; i++)
        {
            var field = record.Fields[i];
            GenerateFieldReadWithColumnIndex(sb, field, i, indent + "        ", record.IsValueType);
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
        sb.AppendLine($"{indent}        var statsStopwatch = global::System.Diagnostics.Stopwatch.StartNew();");
        sb.AppendLine($"{indent}        var items = source is global::System.Collections.Generic.ICollection<{record.FullTypeName}> col ? col : new global::System.Collections.Generic.List<{record.FullTypeName}>(source);");
        sb.AppendLine($"{indent}        var count = items.Count;");
        sb.AppendLine();
        
        // Generate value buffers and statistics collectors for each field
        sb.AppendLine($"{indent}        // Phase 1: Collect all values and statistics");
        for (int i = 0; i < record.Fields.Count; i++)
        {
            var field = record.Fields[i];
            sb.AppendLine($"{indent}        var values{i} = new global::System.Collections.Generic.List<{field.FieldTypeName}>(count);");
            sb.AppendLine($"{indent}        var statsCollector{i} = new global::ArrowCollection.ColumnStatisticsCollector<{field.FieldTypeName}>(\"{field.MemberName}\");");
        }
        sb.AppendLine();
        
        // Single pass to collect all values
        sb.AppendLine($"{indent}        foreach (var item in items)");
        sb.AppendLine($"{indent}        {{");
        for (int i = 0; i < record.Fields.Count; i++)
        {
            sb.AppendLine($"{indent}            var v{i} = _getter{i}(item);");
            sb.AppendLine($"{indent}            values{i}.Add(v{i});");
            sb.AppendLine($"{indent}            statsCollector{i}.Record(v{i});");
        }
        sb.AppendLine($"{indent}        }}");
        sb.AppendLine();
        
        // Collect statistics
        sb.AppendLine($"{indent}        // Collect build statistics");
        sb.AppendLine($"{indent}        var columnStats = new global::System.Collections.Generic.Dictionary<string, global::ArrowCollection.ColumnStatistics>");
        sb.AppendLine($"{indent}        {{");
        for (int i = 0; i < record.Fields.Count; i++)
        {
            var field = record.Fields[i];
            var comma = i < record.Fields.Count - 1 ? "," : "";
            sb.AppendLine($"{indent}            {{ \"{field.MemberName}\", statsCollector{i}.GetStatistics() }}{comma}");
        }
        sb.AppendLine($"{indent}        }};");
        sb.AppendLine();
        sb.AppendLine($"{indent}        statsStopwatch.Stop();");
        sb.AppendLine();
        
        // Phase 2: Build arrays with optimal encoding
        sb.AppendLine($"{indent}        // Phase 2: Build arrays with optimal encoding based on statistics");
        sb.AppendLine($"{indent}        var allocator = new NativeMemoryAllocator();");
        sb.AppendLine($"{indent}        var arrays = new global::System.Collections.Generic.List<IArrowArray>();");
        sb.AppendLine($"{indent}        var schemaFields = new global::System.Collections.Generic.List<Field>();");
        sb.AppendLine();
        
        // Generate array building with dictionary encoding support
        for (int i = 0; i < record.Fields.Count; i++)
        {
            var field = record.Fields[i];
            GenerateOptimalArrayBuilder(sb, field, i, indent + "        ");
            sb.AppendLine();
        }
        
        sb.AppendLine($"{indent}        // Build schema from actual array types");
        sb.AppendLine($"{indent}        var schema = new Schema(schemaFields, null);");
        sb.AppendLine();
            sb.AppendLine($"{indent}        var buildStatistics = new global::ArrowCollection.ArrowCollectionBuildStatistics");
            sb.AppendLine($"{indent}        {{");
            sb.AppendLine($"{indent}            ColumnStatistics = columnStats,");
            sb.AppendLine($"{indent}            RowCount = count,");
            sb.AppendLine($"{indent}            StatisticsCollectionTime = statsStopwatch.Elapsed");
            sb.AppendLine($"{indent}        }};");
            sb.AppendLine();
            sb.AppendLine($"{indent}        // Create record batch");
            sb.AppendLine($"{indent}        var recordBatch = new RecordBatch(schema, arrays, count);");
            sb.AppendLine();
            sb.AppendLine($"{indent}        return new GeneratedArrowCollection_{record.ClassName}(recordBatch, count, buildStatistics);");
            sb.AppendLine($"{indent}    }}");
            sb.AppendLine();

            // Generate the deserialization factory method
            GenerateCreateFromRecordBatchMethod(sb, record, indent);

            sb.AppendLine($"{indent}}}");

            if (record.Namespace is not null)
            {
                sb.AppendLine("}");
            }

            return sb.ToString();
        }

        private static void GenerateCreateFromRecordBatchMethod(StringBuilder sb, ArrowRecordInfo record, string indent)
        {
            sb.AppendLine($"{indent}    /// <summary>");
            sb.AppendLine($"{indent}    /// Creates an ArrowCollection from a deserialized RecordBatch.");
            sb.AppendLine($"{indent}    /// </summary>");
            sb.AppendLine($"{indent}    internal static global::ArrowCollection.ArrowCollection<{record.FullTypeName}> CreateFromRecordBatch(global::Apache.Arrow.RecordBatch recordBatch, global::ArrowCollection.ArrowReadOptions options)");
            sb.AppendLine($"{indent}    {{");
            sb.AppendLine($"{indent}        var schema = recordBatch.Schema;");
            sb.AppendLine($"{indent}        var expectedColumns = new global::System.Collections.Generic.HashSet<string>");
            sb.AppendLine($"{indent}        {{");
        
            // List expected column names
            for (int i = 0; i < record.Fields.Count; i++)
            {
                var field = record.Fields[i];
                var comma = i < record.Fields.Count - 1 ? "," : "";
                sb.AppendLine($"{indent}            \"{field.ColumnName}\"{comma}");
            }
            sb.AppendLine($"{indent}        }};");
            sb.AppendLine();

            // Check for unknown columns if strict mode
            sb.AppendLine($"{indent}        // Validate unknown columns if strict mode");
            sb.AppendLine($"{indent}        if (options.UnknownColumns == global::ArrowCollection.UnknownColumnBehavior.Throw)");
            sb.AppendLine($"{indent}        {{");
            sb.AppendLine($"{indent}            foreach (var field in schema.FieldsList)");
            sb.AppendLine($"{indent}            {{");
            sb.AppendLine($"{indent}                if (!expectedColumns.Contains(field.Name))");
            sb.AppendLine($"{indent}                {{");
            sb.AppendLine($"{indent}                    throw new global::System.InvalidOperationException($\"Unknown column '{{field.Name}}' found in source data. Expected columns: {{string.Join(\", \", expectedColumns)}}\");");
            sb.AppendLine($"{indent}                }}");
            sb.AppendLine($"{indent}            }}");
            sb.AppendLine($"{indent}        }}");
            sb.AppendLine();

            // Check for missing columns if strict mode
            sb.AppendLine($"{indent}        // Validate missing columns if strict mode");
            sb.AppendLine($"{indent}        if (options.MissingColumns == global::ArrowCollection.MissingColumnBehavior.Throw)");
            sb.AppendLine($"{indent}        {{");
            sb.AppendLine($"{indent}            var sourceColumns = new global::System.Collections.Generic.HashSet<string>();");
            sb.AppendLine($"{indent}            foreach (var field in schema.FieldsList)");
            sb.AppendLine($"{indent}            {{");
            sb.AppendLine($"{indent}                sourceColumns.Add(field.Name);");
            sb.AppendLine($"{indent}            }}");
            sb.AppendLine($"{indent}            foreach (var expectedColumn in expectedColumns)");
            sb.AppendLine($"{indent}            {{");
            sb.AppendLine($"{indent}                if (!sourceColumns.Contains(expectedColumn))");
            sb.AppendLine($"{indent}                {{");
            sb.AppendLine($"{indent}                    throw new global::System.InvalidOperationException($\"Missing column '{{expectedColumn}}' in source data.\");");
            sb.AppendLine($"{indent}                }}");
            sb.AppendLine($"{indent}            }}");
            sb.AppendLine($"{indent}        }}");
            sb.AppendLine();

            // Build column index map for name-based lookup
            sb.AppendLine($"{indent}        // Build column index map for name-based lookup");
            sb.AppendLine($"{indent}        var columnIndexMap = new global::System.Collections.Generic.Dictionary<string, int>();");
            sb.AppendLine($"{indent}        for (int i = 0; i < schema.FieldsList.Count; i++)");
            sb.AppendLine($"{indent}        {{");
            sb.AppendLine($"{indent}            columnIndexMap[schema.FieldsList[i].Name] = i;");
            sb.AppendLine($"{indent}        }}");
            sb.AppendLine();

            // Store column indices for each expected field
            for (int i = 0; i < record.Fields.Count; i++)
            {
                var field = record.Fields[i];
                sb.AppendLine($"{indent}        var colIndex{i} = columnIndexMap.TryGetValue(\"{field.ColumnName}\", out var idx{i}) ? idx{i} : -1;");
            }
            sb.AppendLine();

            // Create a new collection that uses name-based column lookup
            sb.AppendLine($"{indent}        return new GeneratedArrowCollection_{record.ClassName}(recordBatch, recordBatch.Length, colIndex: new int[] {{ {string.Join(", ", Enumerable.Range(0, record.Fields.Count).Select(i => $"colIndex{i}"))} }});");
            sb.AppendLine($"{indent}    }}");
        }

    private static void GenerateFieldRead(StringBuilder sb, ArrowFieldInfo field, int columnIndex, string indent, bool isValueType)
    {
        var varName = $"col{columnIndex}";
        
        // For dictionary-supported types, use the helper that handles both dictionary and primitive arrays
        if (SupportsDictionaryEncoding(field.UnderlyingTypeName))
        {
            GenerateDictionaryAwareFieldRead(sb, field, columnIndex, indent, isValueType, varName);
        }
        else
        {
            // For other types, use the original approach
            var arrayType = GetArrowArrayType(field.UnderlyingTypeName);
            sb.AppendLine($"{indent}var {varName} = (recordBatch.Column({columnIndex}) as {arrayType})!;");

            if (field.IsNullable)
            {
                sb.AppendLine($"{indent}if (!{varName}.IsNull(index))");
                sb.AppendLine($"{indent}{{");
                if (isValueType)
                {
                    sb.AppendLine($"{indent}    _setter{columnIndex}(ref item, {GetValueExtraction(varName, field.UnderlyingTypeName)});");
                }
                else
                {
                    sb.AppendLine($"{indent}    _setter{columnIndex}(item, {GetValueExtraction(varName, field.UnderlyingTypeName)});");
                }
                sb.AppendLine($"{indent}}}");
            }
            else
            {
                if (isValueType)
                {
                    sb.AppendLine($"{indent}_setter{columnIndex}(ref item, {GetValueExtraction(varName, field.UnderlyingTypeName)});");
                }
                else
                {
                    sb.AppendLine($"{indent}_setter{columnIndex}(item, {GetValueExtraction(varName, field.UnderlyingTypeName)});");
                }
            }
        }
    }

    /// <summary>
    /// Generates field read code that uses the _columnIndices array for name-based column lookup.
    /// This allows deserialized data with different column orders to be read correctly.
    /// </summary>
    private static void GenerateFieldReadWithColumnIndex(StringBuilder sb, ArrowFieldInfo field, int fieldIndex, string indent, bool isValueType)
    {
        var varName = $"col{fieldIndex}";
        var colIdxVar = $"_columnIndices[{fieldIndex}]";
        
        // Check if column exists (index != -1)
        sb.AppendLine($"{indent}if ({colIdxVar} >= 0)");
        sb.AppendLine($"{indent}{{");
        
        var innerIndent = indent + "    ";
        
        // For dictionary-supported types, use the helper that handles both dictionary and primitive arrays
        if (SupportsDictionaryEncoding(field.UnderlyingTypeName))
        {
            GenerateDictionaryAwareFieldReadWithColumnIndex(sb, field, fieldIndex, innerIndent, isValueType, varName, colIdxVar);
        }
        else
        {
            // For other types, use the original approach
            var arrayType = GetArrowArrayType(field.UnderlyingTypeName);
            sb.AppendLine($"{innerIndent}var {varName} = (recordBatch.Column({colIdxVar}) as {arrayType})!;");

            if (field.IsNullable)
            {
                sb.AppendLine($"{innerIndent}if (!{varName}.IsNull(index))");
                sb.AppendLine($"{innerIndent}{{");
                if (isValueType)
                {
                    sb.AppendLine($"{innerIndent}    _setter{fieldIndex}(ref item, {GetValueExtraction(varName, field.UnderlyingTypeName)});");
                }
                else
                {
                    sb.AppendLine($"{innerIndent}    _setter{fieldIndex}(item, {GetValueExtraction(varName, field.UnderlyingTypeName)});");
                }
                sb.AppendLine($"{innerIndent}}}");
            }
            else
            {
                if (isValueType)
                {
                    sb.AppendLine($"{innerIndent}_setter{fieldIndex}(ref item, {GetValueExtraction(varName, field.UnderlyingTypeName)});");
                }
                else
                {
                    sb.AppendLine($"{innerIndent}_setter{fieldIndex}(item, {GetValueExtraction(varName, field.UnderlyingTypeName)});");
                }
            }
        }
        
        sb.AppendLine($"{indent}}}");
    }

    private static void GenerateDictionaryAwareFieldReadWithColumnIndex(StringBuilder sb, ArrowFieldInfo field, int fieldIndex, string indent, bool isValueType, string varName, string colIdxVar)
    {
        sb.AppendLine($"{indent}var {varName} = recordBatch.Column({colIdxVar});");
        
        // Use RunLengthEncodedArrayBuilder which handles RLE, dictionary, and primitive arrays
        var getValue = field.UnderlyingTypeName switch
        {
            "string" => $"global::ArrowCollection.RunLengthEncodedArrayBuilder.GetStringValue({varName}, index)",
            "int" => $"global::ArrowCollection.RunLengthEncodedArrayBuilder.GetInt32Value({varName}, index)",
            "double" => $"global::ArrowCollection.RunLengthEncodedArrayBuilder.GetDoubleValue({varName}, index)",
            "decimal" => $"global::ArrowCollection.RunLengthEncodedArrayBuilder.GetDecimalValue({varName}, index)",
            _ => throw new NotSupportedException($"Type {field.UnderlyingTypeName} does not support optimized encoding.")
        };

        if (field.IsNullable || field.UnderlyingTypeName == "string")
        {
            sb.AppendLine($"{indent}if (!{varName}.IsNull(index))");
            sb.AppendLine($"{indent}{{");
            if (isValueType)
            {
                sb.AppendLine($"{indent}    _setter{fieldIndex}(ref item, {getValue});");
            }
            else
            {
                sb.AppendLine($"{indent}    _setter{fieldIndex}(item, {getValue});");
            }
            sb.AppendLine($"{indent}}}");
        }
        else
        {
            if (isValueType)
            {
                sb.AppendLine($"{indent}_setter{fieldIndex}(ref item, {getValue});");
            }
            else
            {
                sb.AppendLine($"{indent}_setter{fieldIndex}(item, {getValue});");
            }
        }
    }

    private static bool SupportsDictionaryEncoding(string underlyingTypeName)
    {
        return underlyingTypeName is "string" or "int" or "double" or "decimal";
    }

    private static void GenerateDictionaryAwareFieldRead(StringBuilder sb, ArrowFieldInfo field, int columnIndex, string indent, bool isValueType, string varName)
    {
        sb.AppendLine($"{indent}var {varName} = recordBatch.Column({columnIndex});");
        
        // Use RunLengthEncodedArrayBuilder which handles RLE, dictionary, and primitive arrays
        var getValue = field.UnderlyingTypeName switch
        {
            "string" => $"global::ArrowCollection.RunLengthEncodedArrayBuilder.GetStringValue({varName}, index)",
            "int" => $"global::ArrowCollection.RunLengthEncodedArrayBuilder.GetInt32Value({varName}, index)",
            "double" => $"global::ArrowCollection.RunLengthEncodedArrayBuilder.GetDoubleValue({varName}, index)",
            "decimal" => $"global::ArrowCollection.RunLengthEncodedArrayBuilder.GetDecimalValue({varName}, index)",
            _ => throw new NotSupportedException($"Type {field.UnderlyingTypeName} does not support optimized encoding.")
        };

        if (field.IsNullable || field.UnderlyingTypeName == "string")
        {
            sb.AppendLine($"{indent}if (!{varName}.IsNull(index))");
            sb.AppendLine($"{indent}{{");
            if (isValueType)
            {
                sb.AppendLine($"{indent}    _setter{columnIndex}(ref item, {getValue});");
            }
            else
            {
                sb.AppendLine($"{indent}    _setter{columnIndex}(item, {getValue});");
            }
            sb.AppendLine($"{indent}}}");
        }
        else
        {
            if (isValueType)
            {
                sb.AppendLine($"{indent}_setter{columnIndex}(ref item, {getValue});");
            }
            else
            {
                sb.AppendLine($"{indent}_setter{columnIndex}(item, {getValue});");
            }
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
            "System.Half" => $"{varName}.GetValue(index)!.Value",
            "bool" => $"{varName}.GetValue(index)!.Value",
            "decimal" => $"(decimal){varName}.GetValue(index)!.Value",
            "string" => $"{varName}.GetString(index)!",
            "byte[]" => $"{varName}.GetBytes(index).ToArray()",
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
        else if (field.UnderlyingTypeName == "decimal")
        {
            sb.AppendLine($"{indent}var {builderVarName} = new {builderType}(new Decimal128Type(29, 6)).Reserve(count);");
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
            sb.AppendLine($"{indent}    statsCollector{index}.Record(value{index});");
            sb.AppendLine($"{indent}    if (value{index} == null)");
            sb.AppendLine($"{indent}        {builderVarName}.AppendNull();");
            sb.AppendLine($"{indent}    else");

            if (field.UnderlyingTypeName == "System.DateTime")
            {
                sb.AppendLine($"{indent}        {builderVarName}.Append(new global::System.DateTimeOffset(value{index}.Value, global::System.TimeSpan.Zero));");
            }
            else if (field.UnderlyingTypeName == "System.Half")
            {
                // Half value - use .Value to unwrap from nullable
                sb.AppendLine($"{indent}        {builderVarName}.Append(value{index}.Value);");
            }
            else if (field.UnderlyingTypeName == "decimal")
            {
                // Decimal value - Decimal128Array.Builder accepts decimal directly
                sb.AppendLine($"{indent}        {builderVarName}.Append(value{index}.Value);");
            }
            else if (field.UnderlyingTypeName == "string" || field.UnderlyingTypeName == "byte[]")
            {
                // Reference types don't need .Value
                sb.AppendLine($"{indent}        {builderVarName}.Append(value{index});");
            }
            else
            {
                sb.AppendLine($"{indent}        {builderVarName}.Append(value{index}.Value);");
            }
        }
        else
        {
            sb.AppendLine($"{indent}    var value{index} = _getter{index}(item);");
            sb.AppendLine($"{indent}    statsCollector{index}.Record(value{index});");
            
            if (field.UnderlyingTypeName == "System.DateTime")
            {
                sb.AppendLine($"{indent}    {builderVarName}.Append(new global::System.DateTimeOffset(value{index}, global::System.TimeSpan.Zero));");
            }
            else if (field.UnderlyingTypeName == "decimal")
            {
                // Decimal value - Decimal128Array.Builder accepts decimal directly
                sb.AppendLine($"{indent}    {builderVarName}.Append(value{index});");
            }
            else if (field.UnderlyingTypeName == "string" || field.UnderlyingTypeName == "byte[]")
            {
                // Reference types, handle null
                sb.AppendLine($"{indent}    if (value{index} == null)");
                sb.AppendLine($"{indent}        {builderVarName}.AppendNull();");
                sb.AppendLine($"{indent}    else");
                sb.AppendLine($"{indent}        {builderVarName}.Append(value{index});");
            }
            else
            {
                sb.AppendLine($"{indent}    {builderVarName}.Append(value{index});");
            }
        }

        sb.AppendLine($"{indent}}}");
        sb.AppendLine($"{indent}arrays.Add({builderVarName}.Build(allocator));");
    }

    /// <summary>
    /// Generates code that builds arrays with optimal encoding (RLE, dictionary, or primitive) based on statistics.
    /// Encoding priority: RLE > Dictionary > Primitive
    /// </summary>
    private static void GenerateOptimalArrayBuilder(StringBuilder sb, ArrowFieldInfo field, int index, string indent)
    {
        var stats = $"columnStats[\"{field.MemberName}\"]";
        var nullable = field.IsNullable ? "true" : "false";

        // For nullable types, we can't use advanced encoding directly - use primitive
        if (field.IsNullable)
        {
            GeneratePrimitiveArrayBuilder(sb, field, index, indent, nullable);
            return;
        }

        // For types that support RLE/dictionary encoding, generate conditional code
        // The RLE builder internally falls back to dictionary, which falls back to primitive
        switch (field.UnderlyingTypeName)
        {
            case "string":
                GenerateOptimalStringArrayBuilder(sb, field, index, indent, stats, nullable);
                break;
            case "int":
                GenerateOptimalInt32ArrayBuilder(sb, field, index, indent, stats, nullable);
                break;
            case "double":
                GenerateOptimalDoubleArrayBuilder(sb, field, index, indent, stats, nullable);
                break;
            case "decimal":
                GenerateOptimalDecimalArrayBuilder(sb, field, index, indent, stats, nullable);
                break;
            default:
                // For other types, use primitive encoding
                GeneratePrimitiveArrayBuilder(sb, field, index, indent, nullable);
                break;
        }
    }

    private static void GenerateOptimalStringArrayBuilder(StringBuilder sb, ArrowFieldInfo field, int index, string indent, string stats, string nullable)
    {
        sb.AppendLine($"{indent}// Build string array with optimal encoding (RLE > Dictionary > Primitive)");
        sb.AppendLine($"{indent}{{");
        sb.AppendLine($"{indent}    var array{index} = global::ArrowCollection.RunLengthEncodedArrayBuilder.BuildStringArray(values{index}, {stats}, allocator);");
        sb.AppendLine($"{indent}    arrays.Add(array{index});");
        sb.AppendLine($"{indent}    schemaFields.Add(new Field(\"{field.ColumnName}\", array{index}.Data.DataType, {nullable}));");
        sb.AppendLine($"{indent}}}");
    }

    private static void GenerateOptimalInt32ArrayBuilder(StringBuilder sb, ArrowFieldInfo field, int index, string indent, string stats, string nullable)
    {
        sb.AppendLine($"{indent}// Build int32 array with optimal encoding (RLE > Dictionary > Primitive)");
        sb.AppendLine($"{indent}{{");
        sb.AppendLine($"{indent}    var array{index} = global::ArrowCollection.RunLengthEncodedArrayBuilder.BuildInt32Array(values{index}, {stats}, allocator);");
        sb.AppendLine($"{indent}    arrays.Add(array{index});");
        sb.AppendLine($"{indent}    schemaFields.Add(new Field(\"{field.ColumnName}\", array{index}.Data.DataType, {nullable}));");
        sb.AppendLine($"{indent}}}");
    }

    private static void GenerateOptimalDoubleArrayBuilder(StringBuilder sb, ArrowFieldInfo field, int index, string indent, string stats, string nullable)
    {
        sb.AppendLine($"{indent}// Build double array with optimal encoding (RLE > Dictionary > Primitive)");
        sb.AppendLine($"{indent}{{");
        sb.AppendLine($"{indent}    var array{index} = global::ArrowCollection.RunLengthEncodedArrayBuilder.BuildDoubleArray(values{index}, {stats}, allocator);");
        sb.AppendLine($"{indent}    arrays.Add(array{index});");
        sb.AppendLine($"{indent}    schemaFields.Add(new Field(\"{field.ColumnName}\", array{index}.Data.DataType, {nullable}));");
        sb.AppendLine($"{indent}}}");
    }

    private static void GenerateOptimalDecimalArrayBuilder(StringBuilder sb, ArrowFieldInfo field, int index, string indent, string stats, string nullable)
    {
        sb.AppendLine($"{indent}// Build decimal array with optimal encoding (RLE > Dictionary > Primitive)");
        sb.AppendLine($"{indent}{{");
        sb.AppendLine($"{indent}    var array{index} = global::ArrowCollection.RunLengthEncodedArrayBuilder.BuildDecimalArray(values{index}, {stats}, allocator);");
        sb.AppendLine($"{indent}    arrays.Add(array{index});");
        sb.AppendLine($"{indent}    schemaFields.Add(new Field(\"{field.ColumnName}\", array{index}.Data.DataType, {nullable}));");
        sb.AppendLine($"{indent}}}");
    }

    private static void GeneratePrimitiveArrayBuilder(StringBuilder sb, ArrowFieldInfo field, int index, string indent, string nullable)
    {
        var builderType = GetArrowBuilderType(field.UnderlyingTypeName);
        var arrowType = GetArrowTypeExpression(field.UnderlyingTypeName);
        
        sb.AppendLine($"{indent}// Build primitive array for {field.UnderlyingTypeName}");
        sb.AppendLine($"{indent}{{");
        
        if (field.UnderlyingTypeName == "System.DateTime")
        {
            sb.AppendLine($"{indent}    var builder{index} = new {builderType}(new TimestampType(TimeUnit.Millisecond, global::System.TimeZoneInfo.Utc)).Reserve(count);");
            sb.AppendLine($"{indent}    foreach (var value in values{index})");
            if (field.IsNullable)
            {
                sb.AppendLine($"{indent}    {{");
                sb.AppendLine($"{indent}        if (value == null)");
                sb.AppendLine($"{indent}            builder{index}.AppendNull();");
                sb.AppendLine($"{indent}        else");
                sb.AppendLine($"{indent}            builder{index}.Append(new global::System.DateTimeOffset(value.Value, global::System.TimeSpan.Zero));");
                sb.AppendLine($"{indent}    }}");
            }
            else
            {
                sb.AppendLine($"{indent}        builder{index}.Append(new global::System.DateTimeOffset(value, global::System.TimeSpan.Zero));");
            }
        }
        else if (field.UnderlyingTypeName == "string")
        {
            // Strings are reference types - handle null without .Value
            sb.AppendLine($"{indent}    var builder{index} = new {builderType}();");
            sb.AppendLine($"{indent}    foreach (var value in values{index})");
            sb.AppendLine($"{indent}    {{");
            sb.AppendLine($"{indent}        if (value == null)");
            sb.AppendLine($"{indent}            builder{index}.AppendNull();");
            sb.AppendLine($"{indent}        else");
            sb.AppendLine($"{indent}            builder{index}.Append(value);");
            sb.AppendLine($"{indent}    }}");
        }
        else if (field.UnderlyingTypeName == "byte[]")
        {
            sb.AppendLine($"{indent}    var builder{index} = new {builderType}();");
            sb.AppendLine($"{indent}    foreach (var value in values{index})");
            sb.AppendLine($"{indent}    {{");
            sb.AppendLine($"{indent}        if (value == null)");
            sb.AppendLine($"{indent}            builder{index}.AppendNull();");
            sb.AppendLine($"{indent}        else");
            sb.AppendLine($"{indent}            builder{index}.Append(value);");
            sb.AppendLine($"{indent}    }}");
        }
        else if (field.IsNullable)
        {
            // Nullable value types use .Value
            if (field.UnderlyingTypeName == "decimal")
            {
                sb.AppendLine($"{indent}    var builder{index} = new {builderType}(new Decimal128Type(29, 6)).Reserve(count);");
            }
            else
            {
                sb.AppendLine($"{indent}    var builder{index} = new {builderType}().Reserve(count);");
            }
            sb.AppendLine($"{indent}    foreach (var value in values{index})");
            sb.AppendLine($"{indent}    {{");
            sb.AppendLine($"{indent}        if (value == null)");
            sb.AppendLine($"{indent}            builder{index}.AppendNull();");
            sb.AppendLine($"{indent}        else");
            sb.AppendLine($"{indent}            builder{index}.Append(value.Value);");
            sb.AppendLine($"{indent}    }}");
        }
        else
        {
            // Non-nullable value types
            if (field.UnderlyingTypeName == "decimal")
            {
                sb.AppendLine($"{indent}    var builder{index} = new {builderType}(new Decimal128Type(29, 6)).Reserve(count);");
            }
            else
            {
                sb.AppendLine($"{indent}    var builder{index} = new {builderType}().Reserve(count);");
            }
            sb.AppendLine($"{indent}    foreach (var value in values{index})");
            sb.AppendLine($"{indent}        builder{index}.Append(value);");
        }
        
        sb.AppendLine($"{indent}    arrays.Add(builder{index}.Build(allocator));");
            sb.AppendLine($"{indent}    schemaFields.Add(new Field(\"{field.ColumnName}\", {arrowType}, {nullable}));");
            sb.AppendLine($"{indent}}}");
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
            "System.Half" => "HalfFloatType.Default",
            "bool" => "BooleanType.Default",
            "decimal" => "new Decimal128Type(29, 6)",
            "string" => "StringType.Default",
            "byte[]" => "BinaryType.Default",
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
            "System.Half" => "HalfFloatArray",
            "bool" => "BooleanArray",
            "decimal" => "Decimal128Array",
            "string" => "StringArray",
            "byte[]" => "BinaryArray",
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
            "System.Half" => "HalfFloatArray.Builder",
            "bool" => "BooleanArray.Builder",
            "decimal" => "Decimal128Array.Builder",
            "string" => "StringArray.Builder",
            "byte[]" => "BinaryArray.Builder",
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
            sb.AppendLine($"            global::ArrowCollection.ArrowCollectionFactoryRegistry.RegisterDeserialization<{record.FullTypeName}>({builderNamespace}ArrowCollectionBuilder_{record.ClassName}.CreateFromRecordBatch);");
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
internal sealed class ArrowRecordInfo(
    string className,
    string? namespaceName,
    string fullTypeName,
    List<ArrowFieldInfo> fields,
    List<(DiagnosticDescriptor Descriptor, Location Location, object[] Args)> diagnostics,
    bool isValueType)
    : IEquatable<ArrowRecordInfo>
{
    public string ClassName { get; } = className;
    public string? Namespace { get; } = namespaceName;
    public string FullTypeName { get; } = fullTypeName;
    public List<ArrowFieldInfo> Fields { get; } = fields;
    public List<(DiagnosticDescriptor Descriptor, Location Location, object[] Args)> Diagnostics { get; } = diagnostics;

    /// <summary>
    /// Whether the type is a value type (struct).
    /// </summary>
    public bool IsValueType { get; } = isValueType;

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
internal sealed class ArrowFieldInfo(string memberName, string backingFieldName, string columnName, string fieldTypeName, string underlyingTypeName, bool isNullable)
{
    /// <summary>
    /// The name of the field or property as declared in source code.
    /// </summary>
    public string MemberName { get; } = memberName;

    /// <summary>
    /// The name of the backing field to access via reflection.
    /// For fields, this is the same as MemberName.
    /// For auto-properties, this is the compiler-generated backing field name (e.g., "&lt;PropertyName&gt;k__BackingField").
    /// </summary>
    public string BackingFieldName { get; } = backingFieldName;

    /// <summary>
    /// The column name used for Arrow schema and serialization.
    /// This is either the explicit Name from ArrowArrayAttribute or falls back to MemberName.
    /// </summary>
    public string ColumnName { get; } = columnName;

    /// <summary>
    /// The full type name of the field.
    /// </summary>
    public string FieldTypeName { get; } = fieldTypeName;

    /// <summary>
    /// The underlying type name (unwrapped from Nullable if applicable).
    /// </summary>
    public string UnderlyingTypeName { get; } = underlyingTypeName;

    /// <summary>
    /// Whether the field is nullable.
    /// </summary>
    public bool IsNullable { get; } = isNullable;
}
