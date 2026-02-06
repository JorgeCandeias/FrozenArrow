using System.Linq.Expressions;

namespace FrozenArrow.Query.LogicalPlan;

/// <summary>
/// Helper class to extract column information from LINQ expression trees.
/// </summary>
public static class ExpressionHelper
{
    /// <summary>
    /// Extracts a column name from a member access expression like `x => x.ColumnName`.
    /// </summary>
    public static bool TryExtractColumnName(LambdaExpression lambda, out string? columnName)
    {
        columnName = null;

        // Handle simple member access: x => x.PropertyName
        if (lambda.Body is MemberExpression memberExpr)
        {
            columnName = memberExpr.Member.Name;
            return true;
        }

        // Handle convert operations: x => (object)x.PropertyName
        if (lambda.Body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary
            && unary.Operand is MemberExpression convertedMember)
        {
            columnName = convertedMember.Member.Name;
            return true;
        }

        return false;
    }

    /// <summary>
    /// Extracts projections from a NewExpression like `x => new { x.Name, x.Age }`.
    /// </summary>
    public static bool TryExtractProjections(
        LambdaExpression lambda,
        IReadOnlyDictionary<string, Type> inputSchema,
        out List<ProjectionColumn>? projections)
    {
        projections = null;

        // Handle anonymous type creation: x => new { x.Name, x.Age }
        if (lambda.Body is NewExpression newExpr)
        {
            projections = [];

            // Anonymous types have constructor arguments that match properties
            for (int i = 0; i < newExpr.Arguments.Count; i++)
            {
                var arg = newExpr.Arguments[i];
                var member = newExpr.Members?[i];

                if (member is null)
                    continue;

                // Extract source column name
                string? sourceColumn = null;
                if (arg is MemberExpression memberExpr)
                {
                    sourceColumn = memberExpr.Member.Name;
                }
                else if (arg is UnaryExpression { Operand: MemberExpression convertedMember })
                {
                    sourceColumn = convertedMember.Member.Name;
                }

                if (sourceColumn is null)
                    continue;

                // Get output name and type
                var outputName = member.Name;
                var outputType = inputSchema.TryGetValue(sourceColumn, out var type) ? type : typeof(object);

                projections.Add(new ProjectionColumn(sourceColumn, outputName, outputType));
            }

            return projections.Count > 0;
        }

        // Handle member init: x => new MyClass { Name = x.Name, Age = x.Age }
        if (lambda.Body is MemberInitExpression memberInit)
        {
            projections = [];

            foreach (var binding in memberInit.Bindings)
            {
                if (binding is MemberAssignment assignment)
                {
                    string? sourceColumn = null;
                    if (assignment.Expression is MemberExpression memberExpr)
                    {
                        sourceColumn = memberExpr.Member.Name;
                    }
                    else if (assignment.Expression is UnaryExpression { Operand: MemberExpression convertedMember })
                    {
                        sourceColumn = convertedMember.Member.Name;
                    }

                    if (sourceColumn is null)
                        continue;

                    var outputName = assignment.Member.Name;
                    var outputType = inputSchema.TryGetValue(sourceColumn, out var type) ? type : typeof(object);

                    projections.Add(new ProjectionColumn(sourceColumn, outputName, outputType));
                }
            }

            return projections.Count > 0;
        }

        return false;
    }

    /// <summary>
    /// Extracts aggregation operations from a selector like `g => new { Sum = g.Sum(x => x.Sales) }`.
    /// </summary>
    public static bool TryExtractAggregations(
        LambdaExpression lambda,
        out List<AggregationDescriptor>? aggregations,
        out string? groupKeyProperty)
    {
        aggregations = null;
        groupKeyProperty = null;

        // Handle anonymous type with aggregations: g => new { g.Key, Total = g.Sum(x => x.Sales) }
        if (lambda.Body is NewExpression newExpr)
        {
            aggregations = [];

            for (int i = 0; i < newExpr.Arguments.Count; i++)
            {
                var arg = newExpr.Arguments[i];
                var member = newExpr.Members?[i];

                if (member is null)
                    continue;

                var resultPropertyName = member.Name;

                // Check if this is the Key property
                if (arg is MemberExpression { Member.Name: "Key" })
                {
                    groupKeyProperty = resultPropertyName;
                    continue;
                }

                // Check if this is an aggregation method call
                if (arg is MethodCallExpression methodCall)
                {
                    var operation = methodCall.Method.Name switch
                    {
                        "Sum" => AggregationOperation.Sum,
                        "Average" => AggregationOperation.Average,
                        "Min" => AggregationOperation.Min,
                        "Max" => AggregationOperation.Max,
                        "Count" => AggregationOperation.Count,
                        "LongCount" => AggregationOperation.Count,
                        _ => (AggregationOperation?)null
                    };

                    if (operation.HasValue)
                    {
                        // Try to extract column name from selector
                        string? columnName = null;
                        
                        // For Count/LongCount without selector, there's no column
                        if (operation.Value == AggregationOperation.Count && methodCall.Arguments.Count == 1)
                        {
                            // Count() with no selector
                            columnName = null;
                        }
                        // For aggregates with a selector: Sum(x => x.Column)
                        else if (methodCall.Arguments.Count > 1)
                        {
                            var selectorArg = methodCall.Arguments[1];
                            
                            // Handle Quote wrapper: Quote(Lambda)
                            if (selectorArg is UnaryExpression { NodeType: ExpressionType.Quote, Operand: LambdaExpression quotedLambda })
                            {
                                TryExtractColumnName(quotedLambda, out columnName);
                            }
                            // Handle direct Lambda
                            else if (selectorArg is LambdaExpression directLambda)
                            {
                                TryExtractColumnName(directLambda, out columnName);
                            }
                        }

                        aggregations.Add(new AggregationDescriptor
                        {
                            Operation = operation.Value,
                            ColumnName = columnName,
                            ResultPropertyName = resultPropertyName
                        });
                    }
                }
            }

            return aggregations.Count > 0;
        }

        // Handle member init
        if (lambda.Body is MemberInitExpression memberInit)
        {
            aggregations = [];

            foreach (var binding in memberInit.Bindings)
            {
                if (binding is MemberAssignment assignment)
                {
                    var resultPropertyName = assignment.Member.Name;

                    // Check if this is the Key property
                    if (assignment.Expression is MemberExpression { Member.Name: "Key" })
                    {
                        groupKeyProperty = resultPropertyName;
                        continue;
                    }

                    // Check for aggregation method call
                    if (assignment.Expression is MethodCallExpression methodCall)
                    {
                        var operation = methodCall.Method.Name switch
                        {
                            "Sum" => AggregationOperation.Sum,
                            "Average" => AggregationOperation.Average,
                            "Min" => AggregationOperation.Min,
                            "Max" => AggregationOperation.Max,
                            "Count" => AggregationOperation.Count,
                            "LongCount" => AggregationOperation.Count,
                            _ => (AggregationOperation?)null
                        };

                        if (operation.HasValue)
                        {
                            string? columnName = null;
                            
                            // For Count/LongCount without selector, there's no column
                            if (operation.Value == AggregationOperation.Count && methodCall.Arguments.Count == 1)
                            {
                                columnName = null;
                            }
                            // For aggregates with a selector
                            else if (methodCall.Arguments.Count > 1)
                            {
                                var selectorArg = methodCall.Arguments[1];
                                
                                // Handle Quote wrapper
                                if (selectorArg is UnaryExpression { NodeType: ExpressionType.Quote, Operand: LambdaExpression quotedLambda })
                                {
                                    TryExtractColumnName(quotedLambda, out columnName);
                                }
                                // Handle direct Lambda
                                else if (selectorArg is LambdaExpression directLambda)
                                {
                                    TryExtractColumnName(directLambda, out columnName);
                                }
                            }

                            aggregations.Add(new AggregationDescriptor
                            {
                                Operation = operation.Value,
                                ColumnName = columnName,
                                ResultPropertyName = resultPropertyName
                            });
                        }
                    }
                }
            }

            return aggregations.Count > 0;
        }

        return false;
    }
}
