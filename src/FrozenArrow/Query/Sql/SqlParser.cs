using System.Text.RegularExpressions;
using FrozenArrow.Query.LogicalPlan;

namespace FrozenArrow.Query.Sql;

/// <summary>
/// Simple SQL parser that translates SQL queries to logical plans.
/// Phase 8: SQL support with full optimization pipeline.
/// Supports: SELECT, WHERE, GROUP BY, LIMIT, OFFSET
/// </summary>
public sealed partial class SqlParser(Dictionary<string, Type> schema, Dictionary<string, int> columnIndexMap, long rowCount)
{
    private readonly Dictionary<string, Type> _schema = schema ?? throw new ArgumentNullException(nameof(schema));
    private readonly Dictionary<string, int> _columnIndexMap = columnIndexMap ?? throw new ArgumentNullException(nameof(columnIndexMap));

    /// <summary>
    /// Parses a SQL query and returns a logical plan.
    /// </summary>
    public LogicalPlanNode Parse(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            throw new ArgumentException("SQL query cannot be empty", nameof(sql));

        sql = sql.Trim();

        // Parse query components
        var selectMatch = SelectRegex().Match(sql);
        var whereMatch = WhereRegex().Match(sql);
        var groupByMatch = GroupByRegex().Match(sql);
        var limitMatch = LimitRegex().Match(sql);
        var offsetMatch = OffsetRegex().Match(sql);

        if (!selectMatch.Success)
            throw new ArgumentException("SQL query must contain SELECT clause", nameof(sql));

        // Build logical plan from bottom up: Scan ? Filter ? GroupBy/Aggregate ? Limit/Offset ? Project

        // 1. Start with scan
        LogicalPlanNode plan = new ScanPlan("sql_source", new object(), _schema, rowCount);

        // 2. Add WHERE predicates if present
        if (whereMatch.Success)
        {
            var whereClause = whereMatch.Groups[1].Value;
            var predicates = ParseWhereClause(whereClause);
            if (predicates.Count > 0)
            {
                plan = new FilterPlan(plan, predicates, EstimateSelectivity(predicates));
            }
        }

        // 3. Handle GROUP BY or simple aggregation
        var selectClause = selectMatch.Groups[1].Value.Trim();
        
        if (groupByMatch.Success)
        {
            // GROUP BY with aggregations
            var groupByColumn = groupByMatch.Groups[1].Value;
            var aggregations = ParseAggregations(selectClause);
            
            var keyType = _schema.TryGetValue(groupByColumn, out var type) ? type : typeof(object);
            plan = new GroupByPlan(plan, groupByColumn, keyType, aggregations, groupByColumn);
        }
        else if (IsAggregateQuery(selectClause))
        {
            // Simple aggregation without GROUP BY
            var (operation, columnName, outputType) = ParseSimpleAggregation(selectClause);
            plan = new AggregatePlan(plan, operation, columnName, outputType);
        }
        else
        {
            // Regular SELECT (projection) - for now just return scan
            // Full projection support would map columns to ProjectionColumn list
            // For Phase 8, we'll keep it simple and return all columns
            // plan = new ProjectPlan(plan, columns);
        }

        // 4. Add OFFSET first (it comes before LIMIT in the plan tree)
        if (offsetMatch.Success)
        {
            var offset = int.Parse(offsetMatch.Groups[1].Value);
            plan = new OffsetPlan(plan, offset);
        }

        // 5. Then add LIMIT
        if (limitMatch.Success)
        {
            var limit = int.Parse(limitMatch.Groups[1].Value);
            plan = new LimitPlan(plan, limit);
        }

        return plan;
    }

    private List<ColumnPredicate> ParseWhereClause(string whereClause)
    {
        var predicates = new List<ColumnPredicate>();

        // Phase 8 Enhancement (Part 2): Handle OR and NOT operators
        // Parse the expression tree with proper precedence: NOT > AND > OR

        // For now, handle simple cases:
        // 1. Simple AND: "a AND b" -> list of predicates
        // 2. Simple OR: "a OR b" -> single OrPredicate
        // 3. NOT: "NOT a" -> single NotPredicate

        // Check if we have OR at the top level (outside parentheses)
        if (ContainsOperatorAtTopLevel(whereClause, "OR"))
        {
            // Split by OR and create OrPredicate
            var orParts = SplitByTopLevelOperator(whereClause, "OR");
            
            if (orParts.Count == 2)
            {
                // Binary OR: left OR right
                var leftPredicates = ParseWhereClause(orParts[0]);
                var rightPredicates = ParseWhereClause(orParts[1]);

                if (leftPredicates.Count == 1 && rightPredicates.Count == 1)
                {
                    predicates.Add(new OrPredicate(leftPredicates[0], rightPredicates[0]));
                    return predicates;
                }
                else
                {
                    throw new NotSupportedException(
                        "Complex OR expressions with multiple AND conditions not yet fully supported. " +
                        "Use parentheses for now: (a AND b) OR (c AND d)");
                }
            }
            else if (orParts.Count > 2)
            {
                // Multiple ORs: a OR b OR c
                // Build as nested: (a OR (b OR c))
                var result = ParseWhereClause(orParts[^1])[0]; // Last item
                for (int i = orParts.Count - 2; i >= 0; i--)
                {
                    var left = ParseWhereClause(orParts[i])[0];
                    result = new OrPredicate(left, result);
                }
                predicates.Add(result);
                return predicates;
            }
        }

        // Check for NOT at the beginning
        var trimmed = whereClause.Trim();
        if (trimmed.StartsWith("NOT ", StringComparison.OrdinalIgnoreCase))
        {
            // Parse the inner expression
            var inner = trimmed[4..].Trim();

            // Handle parentheses
            if (inner.StartsWith('(') && inner.EndsWith(')'))
            {
                inner = inner[1..^1];
            }

            var innerPredicates = ParseWhereClause(inner);
            if (innerPredicates.Count == 1)
            {
                predicates.Add(new NotPredicate(innerPredicates[0]));
                return predicates;
            }
            else
            {
                throw new NotSupportedException("NOT with multiple AND conditions not yet supported");
            }
        }

        // Simple AND case - split and parse each condition
        var conditions = SplitByTopLevelOperator(whereClause, "AND");

        foreach (var condition in conditions)
        {
            var conditionTrimmed = condition.Trim();
            
            // Remove outer parentheses if present
            if (conditionTrimmed.StartsWith('(') && conditionTrimmed.EndsWith(')'))
            {
                conditionTrimmed = conditionTrimmed[1..^1].Trim();
                
                // Recursively parse the inner expression
                var innerPredicates = ParseWhereClause(conditionTrimmed);
                predicates.AddRange(innerPredicates);
            }
            else
            {
                var predicate = ParseCondition(conditionTrimmed);
                if (predicate != null)
                {
                    predicates.Add(predicate);
                }
            }
        }

        return predicates;
    }

    /// <summary>
    /// Checks if an operator exists at the top level (outside parentheses).
    /// </summary>
    private static bool ContainsOperatorAtTopLevel(string expression, string op)
    {
        int parenDepth = 0;
        var upper = expression.ToUpper();
        var opUpper = " " + op.ToUpper() + " ";

        for (int i = 0; i <= expression.Length - opUpper.Length; i++)
        {
            if (expression[i] == '(')
            {
                parenDepth++;
            }
            else if (expression[i] == ')')
            {
                parenDepth--;
            }
            else if (parenDepth == 0)
            {
                var window = upper.Substring(i, Math.Min(opUpper.Length, upper.Length - i));
                if (window == opUpper)
                {
                    return true;
                }
            }
        }

        return false;
    }

    /// <summary>
    /// Splits an expression by a top-level operator (respecting parentheses).
    /// </summary>
    private static List<string> SplitByTopLevelOperator(string expression, string op)
    {
        var result = new List<string>();
        var current = new System.Text.StringBuilder();
        int parenDepth = 0;
        var upper = expression.ToUpper();
        var opUpper = " " + op.ToUpper() + " ";

        for (int i = 0; i < expression.Length; i++)
        {
            if (expression[i] == '(')
            {
                parenDepth++;
                current.Append(expression[i]);
            }
            else if (expression[i] == ')')
            {
                parenDepth--;
                current.Append(expression[i]);
            }
            else if (parenDepth == 0 && i <= expression.Length - opUpper.Length)
            {
                var window = upper.Substring(i, Math.Min(opUpper.Length, upper.Length - i));
                if (window == opUpper)
                {
                    // Found operator at top level
                    result.Add(current.ToString().Trim());
                    current.Clear();
                    i += opUpper.Length - 1; // Skip the operator
                    continue;
                }
                else
                {
                    current.Append(expression[i]);
                }
            }
            else
            {
                current.Append(expression[i]);
            }
        }

        // Add the last part
        if (current.Length > 0)
        {
            result.Add(current.ToString().Trim());
        }

        // If no operator found, return the whole expression
        if (result.Count == 0)
        {
            result.Add(expression.Trim());
        }

        return result;
    }

    private ColumnPredicate? ParseCondition(string condition)
    {
        // Parse: column operator value
        // Examples: "Age > 30", "Name = 'Alice'", "Name LIKE 'A%'"

        var match = OperatorRegex().Match(condition);
        if (!match.Success)
            return null;

        var columnName = match.Groups[1].Value;
        var operatorStr = match.Groups[2].Value;
        var valueStr = match.Groups[3].Value.Trim('\'', '"', ' ');

        if (!_columnIndexMap.TryGetValue(columnName, out var columnIndex))
            throw new ArgumentException($"Column '{columnName}' not found in schema");

        if (!_schema.TryGetValue(columnName, out var columnType))
            throw new ArgumentException($"Column '{columnName}' not found in schema");

        // Handle LIKE operator first (before ComparisonOperator switch)
        if (operatorStr.Equals("LIKE", StringComparison.OrdinalIgnoreCase))
        {
            if (columnType == typeof(string))
            {
                return ParseLikeOperator(columnName, columnIndex, valueStr);
            }
            throw new NotSupportedException($"LIKE operator only supported for string columns");
        }

        var op = operatorStr switch
        {
            "=" => ComparisonOperator.Equal,
            ">" => ComparisonOperator.GreaterThan,
            "<" => ComparisonOperator.LessThan,
            ">=" => ComparisonOperator.GreaterThanOrEqual,
            "<=" => ComparisonOperator.LessThanOrEqual,
            "!=" => ComparisonOperator.NotEqual,
            "<>" => ComparisonOperator.NotEqual,
            _ => throw new ArgumentException($"Unsupported operator: {operatorStr}")
        };

        // Create typed predicate based on column type
        if (columnType == typeof(int))
        {
            var value = int.Parse(valueStr);
            return new Int32ComparisonPredicate(columnName, columnIndex, op, value);
        }
        else if (columnType == typeof(double))
        {
            var value = double.Parse(valueStr);
            return new DoubleComparisonPredicate(columnName, columnIndex, op, value);
        }
        else if (columnType == typeof(string))
        {
            // Phase 8 Enhancement: String predicate support
            
            // Handle standard comparison operators
            var stringOp = op switch
            {
                ComparisonOperator.Equal => StringComparisonOperator.Equal,
                ComparisonOperator.NotEqual => StringComparisonOperator.NotEqual,
                ComparisonOperator.GreaterThan => StringComparisonOperator.GreaterThan,
                ComparisonOperator.LessThan => StringComparisonOperator.LessThan,
                ComparisonOperator.GreaterThanOrEqual => StringComparisonOperator.GreaterThanOrEqual,
                ComparisonOperator.LessThanOrEqual => StringComparisonOperator.LessThanOrEqual,
                _ => throw new NotSupportedException($"Operator {op} not supported for strings")
            };
            
            return new StringComparisonPredicate(columnName, columnIndex, stringOp, valueStr);
        }

        throw new NotSupportedException($"Column type {columnType} not yet supported in SQL queries");
    }

    private static bool IsAggregateQuery(string selectClause)
    {
        var upper = selectClause.ToUpper();
        return upper.Contains("COUNT(") || upper.Contains("SUM(") || 
               upper.Contains("AVG(") || upper.Contains("MIN(") || upper.Contains("MAX(");
    }

    private (AggregationOperation, string?, Type) ParseSimpleAggregation(string selectClause)
    {
        var upper = selectClause.ToUpper();

        if (upper.Contains("COUNT(*)"))
        {
            return (AggregationOperation.Count, null, typeof(int));
        }

        var match = SimpleAggregationRegex().Match(selectClause);
        if (!match.Success)
            throw new ArgumentException($"Invalid aggregation: {selectClause}");

        var funcName = match.Groups[1].Value.ToUpper();
        var columnName = match.Groups[2].Value;

        if (!_schema.TryGetValue(columnName, out var columnType))
            throw new ArgumentException($"Column '{columnName}' not found");

        var operation = funcName switch
        {
            "COUNT" => AggregationOperation.Count,
            "SUM" => AggregationOperation.Sum,
            "AVG" => AggregationOperation.Average,
            "MIN" => AggregationOperation.Min,
            "MAX" => AggregationOperation.Max,
            _ => throw new ArgumentException($"Unknown aggregation: {funcName}")
        };

        var outputType = operation == AggregationOperation.Count ? typeof(int) : columnType;

        return (operation, columnName, outputType);
    }

    private static List<AggregationDescriptor> ParseAggregations(string selectClause)
    {
        var aggregations = new List<AggregationDescriptor>();

        // Find all aggregation functions in SELECT clause
        var matches = AggregationRegex().Matches(selectClause);

        foreach (Match match in matches)
        {
            var funcName = match.Groups[1].Value.ToUpper();
            var columnName = match.Groups[2].Value;

            var operation = funcName switch
            {
                "COUNT" => AggregationOperation.Count,
                "SUM" => AggregationOperation.Sum,
                "AVG" => AggregationOperation.Average,
                "MIN" => AggregationOperation.Min,
                "MAX" => AggregationOperation.Max,
                _ => throw new ArgumentException($"Unknown aggregation: {funcName}")
            };

            aggregations.Add(new AggregationDescriptor
            {
                Operation = operation,
                ColumnName = columnName == "*" ? null : columnName,
                ResultPropertyName = $"{funcName}_{columnName}"
            });
        }

        return aggregations;
    }

    /// <summary>
    /// Parses LIKE operator into appropriate string comparison.
    /// Supports SQL wildcards: % (any characters), _ (single character).
    /// Phase 8 Enhancement: LIKE operator support.
    /// </summary>
    private static StringComparisonPredicate ParseLikeOperator(string columnName, int columnIndex, string pattern)
    {
        // Remove quotes if present
        pattern = pattern.Trim('\'', '"');

        // Analyze pattern to determine operation
        bool startsWithPercent = pattern.StartsWith('%');
        bool endsWithPercent = pattern.EndsWith('%');

        if (startsWithPercent && endsWithPercent)
        {
            // %value% -> Contains
            var value = pattern.Trim('%');
            return new StringComparisonPredicate(columnName, columnIndex, 
                StringComparisonOperator.Contains, value);
        }
        else if (startsWithPercent)
        {
            // %value -> EndsWith
            var value = pattern.TrimStart('%');
            return new StringComparisonPredicate(columnName, columnIndex,
                StringComparisonOperator.EndsWith, value);
        }
        else if (endsWithPercent)
        {
            // value% -> StartsWith
            var value = pattern.TrimEnd('%');
            return new StringComparisonPredicate(columnName, columnIndex,
                StringComparisonOperator.StartsWith, value);
        }
        else
        {
            // No wildcards -> Exact match
            return new StringComparisonPredicate(columnName, columnIndex,
                StringComparisonOperator.Equal, pattern);
        }
    }

    private static double EstimateSelectivity(List<ColumnPredicate> predicates)
    {
        // Simple heuristic: assume each predicate filters 50%
        return Math.Pow(0.5, predicates.Count);
    }

    [GeneratedRegex(@"SELECT\s+(.+?)\s+FROM", RegexOptions.IgnoreCase, "en-GB")]
    private static partial Regex SelectRegex();

    [GeneratedRegex(@"WHERE\s+(.+?)(?:\s+GROUP BY|\s+ORDER BY|\s+LIMIT|\s+OFFSET|$)", RegexOptions.IgnoreCase, "en-GB")]
    private static partial Regex WhereRegex();

    [GeneratedRegex(@"GROUP BY\s+(\w+)", RegexOptions.IgnoreCase, "en-GB")]
    private static partial Regex GroupByRegex();

    [GeneratedRegex(@"LIMIT\s+(\d+)", RegexOptions.IgnoreCase, "en-GB")]
    private static partial Regex LimitRegex();

    [GeneratedRegex(@"OFFSET\s+(\d+)", RegexOptions.IgnoreCase, "en-GB")]
    private static partial Regex OffsetRegex();

    [GeneratedRegex(@"(\w+)\s*(=|>|<|>=|<=|!=|<>|LIKE)\s*(.+)", RegexOptions.IgnoreCase, "en-GB")]
    private static partial Regex OperatorRegex();

    [GeneratedRegex(@"(COUNT|SUM|AVG|MIN|MAX)\((\w+)\)", RegexOptions.IgnoreCase, "en-GB")]
    private static partial Regex SimpleAggregationRegex();

    [GeneratedRegex(@"(COUNT|SUM|AVG|MIN|MAX)\((\w+|\*)\)", RegexOptions.IgnoreCase, "en-GB")]
    private static partial Regex AggregationRegex();
}
