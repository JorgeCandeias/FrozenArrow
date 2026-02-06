using FrozenArrow.Query.LogicalPlan;

namespace FrozenArrow.Query;

/// <summary>
/// Example demonstrating the new logical plan architecture.
/// This shows how the query engine is decoupled from LINQ.
/// </summary>
public static class LogicalPlanExample
{
    /// <summary>
    /// Example: Creating a logical plan directly (without LINQ).
    /// This is what future query frontends (SQL, JSON) will do.
    /// </summary>
    public static LogicalPlanNode CreatePlanDirectly()
    {
        // Define schema
        var schema = new Dictionary<string, Type>
        {
            ["Id"] = typeof(int),
            ["Name"] = typeof(string),
            ["Age"] = typeof(int),
            ["Country"] = typeof(string),
            ["Sales"] = typeof(double)
        };

        // Build logical plan programmatically
        // Equivalent to: SELECT * FROM Orders WHERE Age > 25 AND Country = 'USA' LIMIT 100

        var scan = new ScanPlan(
            tableName: "Orders",
            sourceReference: new object(), // Would be FrozenArrow<Order>
            schema: schema,
            rowCount: 1_000_000);

        var columnIndexMap = new Dictionary<string, int>
        {
            ["Id"] = 0,
            ["Name"] = 1,
            ["Age"] = 2,
            ["Country"] = 3,
            ["Sales"] = 4
        };

        var predicates = new List<ColumnPredicate>
        {
            new Int32ComparisonPredicate(
                columnName: "Age",
                columnIndex: columnIndexMap["Age"],
                op: ComparisonOperator.GreaterThan,
                value: 25),
            new StringEqualityPredicate(
                columnName: "Country",
                columnIndex: columnIndexMap["Country"],
                value: "USA")
        };

        var filter = new FilterPlan(
            input: scan,
            predicates: predicates,
            estimatedSelectivity: 0.25);

        var limit = new LimitPlan(
            input: filter,
            count: 100);

        return limit;
    }

    /// <summary>
    /// Example: Optimizing a logical plan.
    /// </summary>
    public static LogicalPlanNode OptimizePlan(LogicalPlanNode plan, ZoneMap? zoneMap = null)
    {
        var optimizer = new LogicalPlanOptimizer(zoneMap);
        return optimizer.Optimize(plan);
    }

    /// <summary>
    /// Example: Visualizing a logical plan.
    /// </summary>
    public static string ExplainPlan(LogicalPlanNode plan)
    {
        return new LogicalPlanExplainer().Explain(plan);
    }

    /// <summary>
    /// Example: How LINQ will use this internally (future implementation).
    /// </summary>
    public static void LinqUsageExample<T>(FrozenArrow<T> data) where T : class
    {
        // User writes LINQ
        var query = data
            .AsQueryable()
            .Where(x => true) // Simplified
            .Take(100);

        // Behind the scenes (in ArrowQueryProvider):
        // 1. Translate LINQ expression to logical plan
        // 2. Optimize logical plan
        // 3. Create physical plan
        // 4. Execute physical plan

        // This is pseudo-code showing the flow:
        /*
        var translator = new LinqToLogicalPlanTranslator(...);
        var logicalPlan = translator.Translate(query.Expression);
        
        var optimizer = new LogicalPlanOptimizer(zoneMap);
        var optimizedPlan = optimizer.Optimize(logicalPlan);
        
        var physicalPlanner = new PhysicalPlanner();
        var physicalPlan = physicalPlanner.CreatePlan(optimizedPlan);
        
        var executor = new PhysicalExecutor();
        return executor.Execute<T>(physicalPlan);
        */
    }

    /// <summary>
    /// Example: Future SQL support (once implemented).
    /// </summary>
    public static void SqlUsageExample<T>(FrozenArrow<T> data) where T : class
    {
        // Future API - SQL queries map to logical plans
        /*
        var results = data.Query(@"
            SELECT Category, SUM(Sales) as TotalSales
            FROM data
            WHERE Country = 'USA' AND Age > 25
            GROUP BY Category
            LIMIT 10
        ");
        
        // Behind the scenes:
        // 1. Parse SQL ? Logical plan (same as LINQ produces)
        // 2. Optimize logical plan (same optimizer!)
        // 3. Execute physical plan (same executor!)
        */
    }

    /// <summary>
    /// Example: Comparing plans (for testing optimizer).
    /// </summary>
    public static void ComparePlansExample()
    {
        var plan = CreatePlanDirectly();
        
        Console.WriteLine("=== Original Plan ===");
        Console.WriteLine(ExplainPlan(plan));
        Console.WriteLine();

        var optimized = OptimizePlan(plan);
        
        Console.WriteLine("=== Optimized Plan ===");
        Console.WriteLine(ExplainPlan(optimized));
        Console.WriteLine();

        // Output might show:
        // - Predicates reordered by selectivity
        // - Fused operations identified
        // - Better cost estimates
    }
}

/// <summary>
/// Simple plan explainer for visualization.
/// </summary>
internal class LogicalPlanExplainer : ILogicalPlanVisitor<string>
{
    private int _indent = 0;

    public string Explain(LogicalPlanNode plan)
    {
        _indent = 0;
        return plan.Accept(this);
    }

    private string Indent() => new string(' ', _indent * 2);

    public string Visit(ScanPlan plan)
    {
        return $"{Indent()}{plan.Description} ? {plan.EstimatedRowCount:N0} rows";
    }

    public string Visit(FilterPlan plan)
    {
        var result = $"{Indent()}{plan.Description} ? {plan.EstimatedRowCount:N0} rows\n";
        _indent++;
        result += plan.Input.Accept(this);
        _indent--;
        return result;
    }

    public string Visit(ProjectPlan plan)
    {
        var result = $"{Indent()}{plan.Description} ? {plan.EstimatedRowCount:N0} rows\n";
        _indent++;
        result += plan.Input.Accept(this);
        _indent--;
        return result;
    }

    public string Visit(AggregatePlan plan)
    {
        var result = $"{Indent()}{plan.Description} ? {plan.EstimatedRowCount:N0} rows\n";
        _indent++;
        result += plan.Input.Accept(this);
        _indent--;
        return result;
    }

    public string Visit(GroupByPlan plan)
    {
        var result = $"{Indent()}{plan.Description} ? {plan.EstimatedRowCount:N0} groups\n";
        _indent++;
        result += plan.Input.Accept(this);
        _indent--;
        return result;
    }

    public string Visit(LimitPlan plan)
    {
        var result = $"{Indent()}{plan.Description} ? {plan.EstimatedRowCount:N0} rows\n";
        _indent++;
        result += plan.Input.Accept(this);
        _indent--;
        return result;
    }

    public string Visit(OffsetPlan plan)
    {
        var result = $"{Indent()}{plan.Description} ? {plan.EstimatedRowCount:N0} rows\n";
        _indent++;
        result += plan.Input.Accept(this);
        _indent--;
        return result;
    }
}
