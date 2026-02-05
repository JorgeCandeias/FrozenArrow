using System.Linq.Expressions;
using System.Reflection;

namespace FrozenArrow.Query;

/// <summary>
/// Analyzes LINQ expressions to extract column predicates that can be pushed down
/// to Arrow column-level filtering.
/// </summary>
public sealed class PredicateAnalyzer : ExpressionVisitor
{
    private readonly List<ColumnPredicate> _predicates = [];
    private readonly List<string> _unsupportedReasons = [];
    private readonly Dictionary<string, int> _columnIndexMap;
    private ParameterExpression? _parameter;

    public IReadOnlyList<ColumnPredicate> Predicates => _predicates;
    public IReadOnlyList<string> UnsupportedReasons => _unsupportedReasons;
    public bool HasUnsupportedPatterns => _unsupportedReasons.Count > 0;

    private PredicateAnalyzer(Dictionary<string, int> columnIndexMap)
    {
        _columnIndexMap = columnIndexMap;
    }

    /// <summary>
    /// Analyzes a predicate expression and extracts column-level predicates.
    /// Predicates are created with their column indices immediately for thread-safety.
    /// </summary>
    public static PredicateAnalysisResult Analyze<T>(
        Expression<Func<T, bool>> predicate,
        Dictionary<string, int> columnIndexMap)
    {
        var analyzer = new PredicateAnalyzer(columnIndexMap)
        {
            _parameter = predicate.Parameters[0]
        };
        analyzer.Visit(predicate.Body);

        // All predicates are now created with their column indices
        // No post-processing mutation needed!

        return new PredicateAnalysisResult
        {
            Predicates = analyzer._predicates,
            UnsupportedReasons = analyzer._unsupportedReasons,
            IsFullySupported = !analyzer.HasUnsupportedPatterns
        };
    }

    protected override Expression VisitBinary(BinaryExpression node)
    {
        // Handle AND expressions (&&)
        if (node.NodeType == ExpressionType.AndAlso)
        {
            Visit(node.Left);
            Visit(node.Right);
            return node;
        }

        // Handle OR expressions (||) - these complicate things, mark as unsupported for now
        if (node.NodeType == ExpressionType.OrElse)
        {
            _unsupportedReasons.Add("OR expressions are not yet supported for column pushdown.");
            return node;
        }

        // Handle comparison expressions
        if (TryExtractComparison(node, out var predicate))
        {
            _predicates.Add(predicate!);
            return node;
        }

        _unsupportedReasons.Add($"Binary expression '{node.NodeType}' is not supported.");
        return node;
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        // Handle string methods
        if (node.Method.DeclaringType == typeof(string))
        {
            if (TryExtractStringOperation(node, out var predicate))
            {
                _predicates.Add(predicate!);
                return node;
            }
        }

        // Handle Equals methods
        if (node.Method.Name == "Equals")
        {
            if (TryExtractEqualsCall(node, out var predicate))
            {
                _predicates.Add(predicate!);
                return node;
            }
        }

        _unsupportedReasons.Add($"Method call '{node.Method.DeclaringType?.Name}.{node.Method.Name}' is not supported.");
        return node;
    }

    protected override Expression VisitUnary(UnaryExpression node)
    {
        // Handle negation (!)
        if (node.NodeType == ExpressionType.Not)
        {
            // Check if it's negating a member access (bool property)
            if (node.Operand is MemberExpression memberExpr && 
                memberExpr.Type == typeof(bool) &&
                TryGetColumnName(memberExpr, out var columnName) &&
                TryGetColumnIndex(columnName, out var columnIndex))
            {
                _predicates.Add(new BooleanPredicate(columnName, columnIndex, expectedValue: false));
                return node;
            }
        }

        return base.VisitUnary(node);
    }

    protected override Expression VisitMember(MemberExpression node)
    {
        // Handle direct boolean property access (e.g., x => x.IsActive)
        if (node.Type == typeof(bool) && 
            node.Expression == _parameter &&
            TryGetColumnName(node, out var columnName) &&
            TryGetColumnIndex(columnName, out var columnIndex))
        {
            _predicates.Add(new BooleanPredicate(columnName, columnIndex, expectedValue: true));
            return node;
        }

        return base.VisitMember(node);
    }

    private bool TryGetColumnIndex(string columnName, out int columnIndex)
    {
        if (_columnIndexMap.TryGetValue(columnName, out columnIndex))
        {
            return true;
        }

        _unsupportedReasons.Add($"Column '{columnName}' not found in schema.");
        columnIndex = -1;
        return false;
    }

    private bool TryExtractComparison(BinaryExpression node, out ColumnPredicate? predicate)
    {
        predicate = null;

        // Determine which side is the column and which is the constant
        MemberExpression? memberExpr = null;
        object? constantValue = null;
        bool isReversed = false;

        if (node.Left is MemberExpression leftMember && TryGetConstantValue(node.Right, out var rightValue))
        {
            memberExpr = leftMember;
            constantValue = rightValue;
        }
        else if (node.Right is MemberExpression rightMember && TryGetConstantValue(node.Left, out var leftValue))
        {
            memberExpr = rightMember;
            constantValue = leftValue;
            isReversed = true;
        }

        if (memberExpr is null || 
            !TryGetColumnName(memberExpr, out var columnName) ||
            !TryGetColumnIndex(columnName, out var columnIndex))
        {
            return false;
        }

        var op = GetComparisonOperator(node.NodeType, isReversed);
        if (op is null)
        {
            return false;
        }

        // Create appropriate predicate based on type - WITH columnIndex for immutability!
        predicate = constantValue switch
        {
            int intValue => new Int32ComparisonPredicate(columnName, columnIndex, op.Value, intValue),
            double doubleValue => new DoubleComparisonPredicate(columnName, columnIndex, op.Value, doubleValue),
            decimal decimalValue => new DecimalComparisonPredicate(columnName, columnIndex, op.Value, decimalValue),
            string stringValue when op == ComparisonOperator.Equal => new StringEqualityPredicate(columnName, columnIndex, stringValue),
            string stringValue when op == ComparisonOperator.NotEqual => new StringEqualityPredicate(columnName, columnIndex, stringValue, negate: true),
            null when op == ComparisonOperator.Equal => new IsNullPredicate(columnName, columnIndex, checkForNull: true),
            null when op == ComparisonOperator.NotEqual => new IsNullPredicate(columnName, columnIndex, checkForNull: false),
            _ => null
        };

        return predicate is not null;
    }

    private bool TryExtractStringOperation(MethodCallExpression node, out ColumnPredicate? predicate)
    {
        predicate = null;

        // Handle: x.Property.Contains("value"), x.Property.StartsWith("value"), x.Property.EndsWith("value")
        // Also handles char overloads: x.Property.Contains('a'), etc.
        if (node.Object is MemberExpression memberExpr && 
            TryGetColumnName(memberExpr, out var columnName) &&
            TryGetColumnIndex(columnName, out var columnIndex) &&
            node.Arguments.Count >= 1 &&
            TryGetConstantValue(node.Arguments[0], out var patternObj))
        {
            // Handle both string and char arguments
            string? pattern = patternObj switch
            {
                string s => s,
                char c => c.ToString(),
                _ => null
            };

            if (pattern is not null)
            {
                StringOperation? operation = node.Method.Name switch
                {
                    "Contains" => StringOperation.Contains,
                    "StartsWith" => StringOperation.StartsWith,
                    "EndsWith" => StringOperation.EndsWith,
                    _ => null
                };

                if (operation is not null)
                {
                    var comparison = StringComparison.Ordinal;
                    
                    // Check for StringComparison argument
                    if (node.Arguments.Count >= 2 && 
                        TryGetConstantValue(node.Arguments[1], out var compObj) &&
                        compObj is StringComparison comp)
                    {
                        comparison = comp;
                    }

                    predicate = new StringOperationPredicate(columnName, columnIndex, pattern, operation.Value, comparison);
                    return true;
                }
            }
        }

        // Handle: string.IsNullOrEmpty(x.Property), string.IsNullOrWhiteSpace(x.Property)
        if (node.Method.DeclaringType == typeof(string) && 
            node.Arguments.Count == 1 &&
            node.Arguments[0] is MemberExpression argMember &&
            TryGetColumnName(argMember, out _))
        {
            if (node.Method.Name == "IsNullOrEmpty" || node.Method.Name == "IsNullOrWhiteSpace")
            {
                // This is a complex check - for now, mark as unsupported
                _unsupportedReasons.Add($"string.{node.Method.Name} requires special handling.");
                return false;
            }
        }

        return false;
    }

    private bool TryExtractEqualsCall(MethodCallExpression node, out ColumnPredicate? predicate)
    {
        predicate = null;

        // Handle: x.Property.Equals("value") or x.Property.Equals(variable)
        if (node.Object is MemberExpression memberExpr &&
            TryGetColumnName(memberExpr, out var columnName) &&
            TryGetColumnIndex(columnName, out var columnIndex) &&
            node.Arguments.Count >= 1 &&
            TryGetConstantValue(node.Arguments[0], out var value))
        {
            if (value is string stringValue)
            {
                var comparison = StringComparison.Ordinal;
                if (node.Arguments.Count >= 2 && 
                    TryGetConstantValue(node.Arguments[1], out var compObj) &&
                    compObj is StringComparison comp)
                {
                    comparison = comp;
                }
                predicate = new StringEqualityPredicate(columnName, columnIndex, stringValue, comparison: comparison);
                return true;
            }
        }

        return false;
    }

    private bool TryGetColumnName(MemberExpression memberExpr, out string columnName)
    {
        columnName = string.Empty;

        // Must be a direct member access on the parameter (e.g., x.Property, not x.Property.Something)
        if (memberExpr.Expression != _parameter)
        {
            return false;
        }

        columnName = memberExpr.Member.Name;
        return true;
    }

    private static bool TryGetConstantValue(Expression expr, out object? value)
    {
        value = null;

        // Direct constant
        if (expr is ConstantExpression constant)
        {
            value = constant.Value;
            return true;
        }

        // Captured variable (member access on a constant/closure)
        if (expr is MemberExpression memberExpr && memberExpr.Expression is ConstantExpression closureConstant)
        {
            try
            {
                if (memberExpr.Member is FieldInfo field)
                {
                    value = field.GetValue(closureConstant.Value);
                    return true;
                }
                if (memberExpr.Member is PropertyInfo property)
                {
                    value = property.GetValue(closureConstant.Value);
                    return true;
                }
            }
            catch
            {
                return false;
            }
        }

        // Convert expression (e.g., (object)"string")
        if (expr is UnaryExpression unary && unary.NodeType == ExpressionType.Convert)
        {
            return TryGetConstantValue(unary.Operand, out value);
        }
        
        // Complex constant expressions (arithmetic, method calls, etc.)
        // Try to evaluate the expression at compile time
        try
        {
            // Check if this is a constant expression tree (no parameters)
            if (IsConstantExpression(expr))
            {
                // Compile and evaluate the expression
                var lambda = Expression.Lambda(expr);
                var compiled = lambda.Compile();
                value = compiled.DynamicInvoke();
                return true;
            }
        }
        catch
        {
            // If evaluation fails, return false
            return false;
        }

        return false;
    }
    
    /// <summary>
    /// Checks if an expression contains only constants and no parameters.
    /// </summary>
    private static bool IsConstantExpression(Expression expr)
    {
        return expr switch
        {
            ConstantExpression => true,
            MemberExpression member => member.Expression == null || IsConstantExpression(member.Expression),
            UnaryExpression unary => IsConstantExpression(unary.Operand),
            BinaryExpression binary => IsConstantExpression(binary.Left) && IsConstantExpression(binary.Right),
            MethodCallExpression method => (method.Object == null || IsConstantExpression(method.Object)) && method.Arguments.All(IsConstantExpression),
            ParameterExpression => false,  // Has parameters - NOT constant
            _ => false
        };
    }

    private static ComparisonOperator? GetComparisonOperator(ExpressionType nodeType, bool isReversed)
    {
        return nodeType switch
        {
            ExpressionType.Equal => ComparisonOperator.Equal,
            ExpressionType.NotEqual => ComparisonOperator.NotEqual,
            ExpressionType.LessThan => isReversed ? ComparisonOperator.GreaterThan : ComparisonOperator.LessThan,
            ExpressionType.LessThanOrEqual => isReversed ? ComparisonOperator.GreaterThanOrEqual : ComparisonOperator.LessThanOrEqual,
            ExpressionType.GreaterThan => isReversed ? ComparisonOperator.LessThan : ComparisonOperator.GreaterThan,
            ExpressionType.GreaterThanOrEqual => isReversed ? ComparisonOperator.LessThanOrEqual : ComparisonOperator.GreaterThanOrEqual,
            _ => null
        };
    }
}

/// <summary>
/// Result of predicate analysis.
/// </summary>
public sealed class PredicateAnalysisResult
{
    public IReadOnlyList<ColumnPredicate> Predicates { get; init; } = [];
    public IReadOnlyList<string> UnsupportedReasons { get; init; } = [];
    public bool IsFullySupported { get; init; }
}
