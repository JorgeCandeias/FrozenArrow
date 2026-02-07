using FrozenArrow.Query;
using FrozenArrow.Query.Sql;

namespace FrozenArrow.Tests.Sql;

/// <summary>
/// Debug tests for SQL parser OR operator.
/// </summary>
public class SqlParserOrDebugTests
{
    [Fact]
    public void SqlParser_OrExpression_CreatesOrPredicate()
    {
        // Arrange
        var schema = new Dictionary<string, Type>
        {
            ["Value"] = typeof(int)
        };
        
        var columnIndexMap = new Dictionary<string, int>
        {
            ["Value"] = 0
        };

        var parser = new SqlParser(schema, columnIndexMap, 100);

        // Act
        var sql = "SELECT * FROM data WHERE Value = 100 OR Value = 300";
        var plan = parser.Parse(sql);

        // Assert - Check the logical plan structure
        Console.WriteLine($"Plan type: {plan.GetType().Name}");
        Console.WriteLine($"Plan: {plan}");
        
        if (plan is FrozenArrow.Query.LogicalPlan.FilterPlan filterPlan)
        {
            Console.WriteLine($"Number of predicates: {filterPlan.Predicates.Count}");
            foreach (var pred in filterPlan.Predicates)
            {
                Console.WriteLine($"  Predicate type: {pred.GetType().Name}");
                Console.WriteLine($"  Predicate: {pred}");
                
                if (pred is OrPredicate)
                {
                    Console.WriteLine($"    This is an OR predicate!  ✓");
                }
            }
            
            // Should have exactly 1 predicate (the OrPredicate)
            Assert.Single(filterPlan.Predicates);
            Assert.IsType<OrPredicate>(filterPlan.Predicates[0]);
        }
        else
        {
            Assert.Fail($"Expected FilterPlan but got {plan.GetType().Name}");
        }
    }
}
