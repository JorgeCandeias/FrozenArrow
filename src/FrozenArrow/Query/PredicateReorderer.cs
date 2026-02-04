using System.Runtime.CompilerServices;

namespace FrozenArrow.Query;

/// <summary>
/// Reorders predicates by estimated selectivity to minimize rows evaluated.
/// More selective predicates (lower selectivity = fewer matching rows) are evaluated first,
/// reducing the number of rows that subsequent predicates need to check.
/// </summary>
/// <remarks>
/// The optimization is based on the principle that if Predicate A matches 10% of rows
/// and Predicate B matches 90%, evaluating A first means B only needs to check 10% of rows,
/// vs evaluating B first where A would check 90% of rows.
/// 
/// Selectivity estimation uses zone map statistics:
/// - For range predicates (&gt;, &lt;, &gt;=, &lt;=): Estimate based on min/max overlap
/// - For equality predicates (==): Conservative estimate based on value range
/// - For predicates without zone maps: Assume 50% selectivity (neutral)
/// 
/// This optimization is most effective when:
/// - Predicates have varying selectivity
/// - The most selective predicate is not already first
/// - Zone maps are available for accurate estimation
/// </remarks>
internal static class PredicateReorderer
{
    /// <summary>
    /// Minimum number of predicates to consider reordering.
    /// Reordering a single predicate is pointless.
    /// </summary>
    private const int MinPredicatesForReorder = 2;

    /// <summary>
    /// Default selectivity when no zone map data is available.
    /// 0.5 = assume 50% of rows match (neutral assumption).
    /// </summary>
    private const double DefaultSelectivity = 0.5;

    /// <summary>
    /// Selectivity for predicates that cannot be skipped by zone maps (e.g., NotEqual).
    /// These are assumed to match most rows.
    /// </summary>
    private const double HighSelectivity = 0.9;
    
    /// <summary>
    /// Minimum selectivity difference to justify reordering.
    /// If all predicates have similar selectivity, don't bother reordering.
    /// Set to 0.20 to avoid overhead for queries with similar predicate selectivity.
    /// </summary>
    private const double MinSelectivityDifference = 0.20;

    /// <summary>
    /// Reorders predicates by estimated selectivity (most selective first).
    /// Returns the same list if reordering would not help.
    /// </summary>
    /// <param name="predicates">The predicates to potentially reorder.</param>
    /// <param name="zoneMap">Zone map for selectivity estimation.</param>
    /// <param name="totalRows">Total number of rows in the dataset.</param>
    /// <returns>
    /// A reordered list of predicates if beneficial, or the original list if not.
    /// The returned list may be a new array or the original reference.
    /// </returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IReadOnlyList<ColumnPredicate> ReorderBySelectivity(
        IReadOnlyList<ColumnPredicate> predicates,
        ZoneMap? zoneMap,
        int totalRows)
    {
        // Quick exit: nothing to reorder
        if (predicates.Count < MinPredicatesForReorder)
            return predicates;

        // Use stack allocation for small predicate counts (most common case)
        // This avoids heap allocation for the typical 2-4 predicate queries
        if (predicates.Count <= 8)
        {
            return ReorderSmall(predicates, zoneMap);
        }
        
        return ReorderLarge(predicates, zoneMap);
    }

    /// <summary>
    /// Optimized path for small predicate counts (2-8 predicates).
    /// Uses stack allocation to avoid heap pressure.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IReadOnlyList<ColumnPredicate> ReorderSmall(
        IReadOnlyList<ColumnPredicate> predicates,
        ZoneMap? zoneMap)
    {
        // Fast path: Check if predicates are all the same type (common case).
        // If they are, selectivity differences are likely small, skip reordering.
        if (AreAllSamePredicateType(predicates))
            return predicates;
        
        Span<double> selectivities = stackalloc double[predicates.Count];
        double minSelectivity = double.MaxValue;
        double maxSelectivity = double.MinValue;

        // Estimate selectivity for each predicate using lightweight estimation
        for (int i = 0; i < predicates.Count; i++)
        {
            var predicate = predicates[i];
            ColumnZoneMapData? zoneMapData = null;
            zoneMap?.TryGetColumnZoneMap(predicate.ColumnName, out zoneMapData);
            
            var selectivity = EstimateSelectivityFast(predicate, zoneMapData);
            selectivities[i] = selectivity;
            
            if (selectivity < minSelectivity) minSelectivity = selectivity;
            if (selectivity > maxSelectivity) maxSelectivity = selectivity;
        }

        // Early exit: if all predicates have similar selectivity, don't reorder
        // The overhead of reordering wouldn't be worth the minimal benefit
        if (maxSelectivity - minSelectivity < MinSelectivityDifference)
            return predicates;

        // Check if already optimally ordered (most selective first)
        bool alreadyOrdered = true;
        for (int i = 1; i < selectivities.Length; i++)
        {
            if (selectivities[i] < selectivities[i - 1] - 0.01) // Small tolerance for floating point
            {
                alreadyOrdered = false;
                break;
            }
        }

        if (alreadyOrdered)
            return predicates;

        // Create reordered array using simple insertion sort (good for small N)
        var reordered = new ColumnPredicate[predicates.Count];
        Span<int> indices = stackalloc int[predicates.Count];
        for (int i = 0; i < predicates.Count; i++) indices[i] = i;
        
        // Simple insertion sort by selectivity (ascending)
        for (int i = 1; i < indices.Length; i++)
        {
            int key = indices[i];
            double keySelectivity = selectivities[key];
            int j = i - 1;
            
            while (j >= 0 && selectivities[indices[j]] > keySelectivity)
            {
                indices[j + 1] = indices[j];
                j--;
            }
            indices[j + 1] = key;
        }
        
        for (int i = 0; i < indices.Length; i++)
        {
            reordered[i] = predicates[indices[i]];
        }

        return reordered;
    }
    
    /// <summary>
    /// Fast check if all predicates are the same type.
    /// When predicates are the same type, their selectivity estimates are
    /// likely similar (all based on same estimation logic).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool AreAllSamePredicateType(IReadOnlyList<ColumnPredicate> predicates)
    {
        if (predicates.Count < 2)
            return true;
            
        var firstType = predicates[0].GetType();
        for (int i = 1; i < predicates.Count; i++)
        {
            if (predicates[i].GetType() != firstType)
                return false;
        }
        return true;
    }

    /// <summary>
    /// Path for larger predicate counts (>8 predicates).
    /// Uses heap allocation but this is rare in practice.
    /// </summary>
    private static IReadOnlyList<ColumnPredicate> ReorderLarge(
        IReadOnlyList<ColumnPredicate> predicates,
        ZoneMap? zoneMap)
    {
        var selectivities = new (ColumnPredicate Predicate, double Selectivity)[predicates.Count];
        double minSelectivity = double.MaxValue;
        double maxSelectivity = double.MinValue;
        
        for (int i = 0; i < predicates.Count; i++)
        {
            var predicate = predicates[i];
            ColumnZoneMapData? zoneMapData = null;
            zoneMap?.TryGetColumnZoneMap(predicate.ColumnName, out zoneMapData);
            
            var selectivity = EstimateSelectivityFast(predicate, zoneMapData);
            selectivities[i] = (predicate, selectivity);
            
            if (selectivity < minSelectivity) minSelectivity = selectivity;
            if (selectivity > maxSelectivity) maxSelectivity = selectivity;
        }

        // Early exit check
        if (maxSelectivity - minSelectivity < MinSelectivityDifference)
            return predicates;

        // Check if already ordered
        bool alreadyOrdered = true;
        for (int i = 1; i < selectivities.Length; i++)
        {
            if (selectivities[i].Selectivity < selectivities[i - 1].Selectivity - 0.01)
            {
                alreadyOrdered = false;
                break;
            }
        }

        if (alreadyOrdered)
            return predicates;

        // Sort and extract
        Array.Sort(selectivities, (a, b) => a.Selectivity.CompareTo(b.Selectivity));

        var reordered = new ColumnPredicate[predicates.Count];
        for (int i = 0; i < selectivities.Length; i++)
        {
            reordered[i] = selectivities[i].Predicate;
        }

        return reordered;
    }

    /// <summary>
    /// Fast selectivity estimation that uses pre-computed global min/max from zone maps.
    /// Avoids iterating all chunks by using cached values.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static double EstimateSelectivityFast(
        ColumnPredicate predicate,
        ColumnZoneMapData? zoneMapData)
    {
        // Dispatch to type-specific estimation
        return predicate switch
        {
            Int32ComparisonPredicate int32Pred => EstimateInt32SelectivityFast(int32Pred, zoneMapData),
            DoubleComparisonPredicate doublePred => EstimateDoubleSelectivityFast(doublePred, zoneMapData),
            DecimalComparisonPredicate decimalPred => EstimateDecimalSelectivityFast(decimalPred, zoneMapData),
            BooleanPredicate boolPred => EstimateBooleanSelectivity(boolPred),
            StringEqualityPredicate => 0.1, // String equality is typically selective
            StringOperationPredicate => 0.3, // String contains/startswith/endswith moderately selective
            IsNullPredicate => 0.05, // Null checks are usually very selective
            _ => DefaultSelectivity // Unknown predicate type
        };
    }

    /// <summary>
    /// Fast Int32 selectivity estimation using pre-computed global min/max.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static double EstimateInt32SelectivityFast(
        Int32ComparisonPredicate predicate,
        ColumnZoneMapData? zoneMapData)
    {
        if (zoneMapData == null || zoneMapData.Type != ZoneMapType.Int32)
            return DefaultSelectivity;

        // Use pre-computed global min/max (O(1) instead of O(chunks))
        var (globalMin, globalMax) = zoneMapData.GetGlobalMinMaxInt32();
        
        if (globalMax <= globalMin)
            return DefaultSelectivity;

        double range = globalMax - globalMin;
        double value = predicate.Value;

        return predicate.Operator switch
        {
            // Equality is very selective in numeric columns
            ComparisonOperator.Equal => 0.01,
            
            // NotEqual matches almost everything
            ComparisonOperator.NotEqual => HighSelectivity,
            
            // Range predicates: estimate fraction based on position in range
            ComparisonOperator.LessThan => Math.Clamp((value - globalMin) / range, 0.01, 0.99),
            ComparisonOperator.LessThanOrEqual => Math.Clamp((value - globalMin + 1) / range, 0.01, 0.99),
            ComparisonOperator.GreaterThan => Math.Clamp((globalMax - value) / range, 0.01, 0.99),
            ComparisonOperator.GreaterThanOrEqual => Math.Clamp((globalMax - value + 1) / range, 0.01, 0.99),
            
            _ => DefaultSelectivity
        };
    }

    /// <summary>
    /// Fast Double selectivity estimation.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static double EstimateDoubleSelectivityFast(
        DoubleComparisonPredicate predicate,
        ColumnZoneMapData? zoneMapData)
    {
        if (zoneMapData == null || zoneMapData.Type != ZoneMapType.Double)
            return DefaultSelectivity;

        var (globalMin, globalMax) = zoneMapData.GetGlobalMinMaxDouble();
        
        if (globalMax <= globalMin)
            return DefaultSelectivity;

        double range = globalMax - globalMin;
        double value = predicate.Value;

        return predicate.Operator switch
        {
            ComparisonOperator.Equal => 0.01, // Equality on doubles is rare
            ComparisonOperator.NotEqual => HighSelectivity,
            ComparisonOperator.LessThan => Math.Clamp((value - globalMin) / range, 0.01, 0.99),
            ComparisonOperator.LessThanOrEqual => Math.Clamp((value - globalMin) / range, 0.01, 0.99),
            ComparisonOperator.GreaterThan => Math.Clamp((globalMax - value) / range, 0.01, 0.99),
            ComparisonOperator.GreaterThanOrEqual => Math.Clamp((globalMax - value) / range, 0.01, 0.99),
            _ => DefaultSelectivity
        };
    }

    /// <summary>
    /// Fast Decimal selectivity estimation.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static double EstimateDecimalSelectivityFast(
        DecimalComparisonPredicate predicate,
        ColumnZoneMapData? zoneMapData)
    {
        if (zoneMapData == null || zoneMapData.Type != ZoneMapType.Decimal)
            return DefaultSelectivity;

        var (globalMin, globalMax) = zoneMapData.GetGlobalMinMaxDecimal();
        
        if (globalMax <= globalMin)
            return DefaultSelectivity;

        double range = (double)(globalMax - globalMin);
        double value = (double)predicate.Value;
        double min = (double)globalMin;
        double max = (double)globalMax;

        return predicate.Operator switch
        {
            ComparisonOperator.Equal => 0.01,
            ComparisonOperator.NotEqual => HighSelectivity,
            ComparisonOperator.LessThan => Math.Clamp((value - min) / range, 0.01, 0.99),
            ComparisonOperator.LessThanOrEqual => Math.Clamp((value - min) / range, 0.01, 0.99),
            ComparisonOperator.GreaterThan => Math.Clamp((max - value) / range, 0.01, 0.99),
            ComparisonOperator.GreaterThanOrEqual => Math.Clamp((max - value) / range, 0.01, 0.99),
            _ => DefaultSelectivity
        };
    }

    /// <summary>
    /// Estimates selectivity for Boolean predicates.
    /// Without statistics, assume roughly 50% match rate.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static double EstimateBooleanSelectivity(BooleanPredicate predicate)
    {
        // Boolean columns often have skewed distributions (e.g., IsActive is often true)
        // Without histograms, use a slightly biased estimate
        return predicate.ExpectedValue ? 0.6 : 0.4;
    }
}
