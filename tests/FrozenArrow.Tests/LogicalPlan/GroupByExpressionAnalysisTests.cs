using FrozenArrow.Query;
using System.Linq.Expressions;

namespace FrozenArrow.Tests.LogicalPlan;

/// <summary>
/// Tests to understand Expression tree structure for GroupBy operations.
/// </summary>
public class GroupByExpressionAnalysisTests
{
    [ArrowRecord]
    public record AnalysisTestRecord
    {
        [ArrowArray(Name = "Category")]
        public string Category { get; init; } = string.Empty;

        [ArrowArray(Name = "Sales")]
        public double Sales { get; init; }
    }

    [Fact]
    public void AnalyzeGroupBySelectExpression()
    {
        // Arrange
        var data = new[] { new AnalysisTestRecord { Category = "A", Sales = 100 } }.ToFrozenArrow();
        var queryable = data.AsQueryable();

        // Act - Get the expression tree
        var query = queryable
            .GroupBy(x => x.Category)
            .Select(g => new { Category = g.Key, Total = g.Sum(x => x.Sales) });

        var expression = ((IQueryable)query).Expression;

        // Debug output
        var output = ExpressionTreePrinter.Print(expression);

        // Assert - Just output for analysis
        Assert.NotNull(output);

        // Write to test output
        Console.WriteLine("Expression Tree:");
        Console.WriteLine(output);
    }

    private static class ExpressionTreePrinter
    {
        public static string Print(Expression expression, int depth = 0)
        {
            var indent = new string(' ', depth * 2);
            var result = $"{indent}{expression.NodeType}: {expression.GetType().Name}";

            if (expression is MethodCallExpression methodCall)
            {
                result += $" - Method: {methodCall.Method.Name}";
                result += $"\n{indent}  Arguments:";
                foreach (var arg in methodCall.Arguments)
                {
                    result += $"\n{Print(arg, depth + 2)}";
                }
            }
            else if (expression is LambdaExpression lambda)
            {
                result += $" - Parameters: {string.Join(", ", lambda.Parameters.Select(p => $"{p.Name}:{p.Type.Name}"))}";
                result += $"\n{indent}  Body:\n{Print(lambda.Body, depth + 2)}";
            }
            else if (expression is UnaryExpression unary)
            {
                result += $"\n{indent}  Operand:\n{Print(unary.Operand, depth + 2)}";
            }
            else if (expression is NewExpression newExpr)
            {
                result += $" - Type: {newExpr.Type.Name}";
                if (newExpr.Members != null)
                {
                    result += $"\n{indent}  Members: {string.Join(", ", newExpr.Members.Select(m => m.Name))}";
                }
                result += $"\n{indent}  Arguments:";
                foreach (var arg in newExpr.Arguments)
                {
                    result += $"\n{Print(arg, depth + 2)}";
                }
            }
            else if (expression is MemberExpression member)
            {
                result += $" - Member: {member.Member.Name}";
                if (member.Expression != null)
                {
                    result += $"\n{indent}  Expression:\n{Print(member.Expression, depth + 2)}";
                }
            }
            else if (expression is ConstantExpression constant)
            {
                result += $" - Value: {constant.Value?.GetType().Name ?? "null"}";
            }
            else if (expression is ParameterExpression param)
            {
                result += $" - Name: {param.Name}, Type: {param.Type.Name}";
            }

            return result;
        }
    }
}
