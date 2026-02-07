using Apache.Arrow;

namespace FrozenArrow.Query;

/// <summary>
/// Predicate that combines multiple predicates with OR logic.
/// Phase 8 Enhancement (Part 2): Enables OR operator in SQL WHERE clauses.
/// </summary>
public sealed class OrPredicate(ColumnPredicate left, ColumnPredicate right) : ColumnPredicate
{
    private readonly ColumnPredicate _left = left ?? throw new ArgumentNullException(nameof(left));
    private readonly ColumnPredicate _right = right ?? throw new ArgumentNullException(nameof(right));

    public override string ColumnName => $"({_left.ColumnName} OR {_right.ColumnName})";
    public override int ColumnIndex => _left.ColumnIndex; // Use first predicate's column

    public override void Evaluate(RecordBatch batch, Span<bool> selection)
    {
        // Create temporary selection arrays for each predicate
        // Start with all rows selected for each predicate to evaluate independently
        var leftSelection = new bool[batch.Length];
        var rightSelection = new bool[batch.Length];
        
        // Initialize to the current selection state
        for (int i = 0; i < batch.Length; i++)
        {
            leftSelection[i] = selection[i];
            rightSelection[i] = selection[i];
        }

        // Evaluate both predicates independently
        _left.Evaluate(batch, leftSelection.AsSpan());
        _right.Evaluate(batch, rightSelection.AsSpan());

        // Combine with OR logic: keep row if it was selected by EITHER predicate
        for (int i = 0; i < batch.Length; i++)
        {
            // Only update if originally selected
            if (selection[i])
            {
                selection[i] = leftSelection[i] || rightSelection[i];
            }
        }
    }

    public override void Evaluate(RecordBatch batch, ref SelectionBitmap selection, int? endIndex = null)
    {
        // For OR, we need to evaluate both predicates and combine results
        var actualEndIndex = endIndex ?? batch.Length;
        
        // Create temporary bitmaps
        using var leftSelection = SelectionBitmap.Create(actualEndIndex, initialValue: true);
        using var rightSelection = SelectionBitmap.Create(actualEndIndex, initialValue: true);

        // Copy current selection to both
        for (int i = 0; i < actualEndIndex; i++)
        {
            if (!selection[i])
            {
                leftSelection.Clear(i);
                rightSelection.Clear(i);
            }
        }

        // Evaluate both predicates
        _left.Evaluate(batch, ref System.Runtime.CompilerServices.Unsafe.AsRef(in leftSelection), actualEndIndex);
        _right.Evaluate(batch, ref System.Runtime.CompilerServices.Unsafe.AsRef(in rightSelection), actualEndIndex);

        // Combine with OR: selection[i] = leftSelection[i] OR rightSelection[i]
        for (int i = 0; i < actualEndIndex; i++)
        {
            if (selection[i])
            {
                // Keep if either predicate selected it
                if (!leftSelection[i] && !rightSelection[i])
                {
                    selection.Clear(i);
                }
            }
        }
    }

    protected override bool EvaluateSingle(IArrowArray column, int rowIndex)
    {
        // This should not be called for compound predicates
        throw new NotSupportedException("OrPredicate uses Evaluate(RecordBatch, ref SelectionBitmap) for evaluation");
    }

    public override string ToString()
    {
        return $"({_left} OR {_right})";
    }
}

/// <summary>
/// Predicate that negates another predicate with NOT logic.
/// Phase 8 Enhancement (Part 2): Enables NOT operator in SQL WHERE clauses.
/// </summary>
public sealed class NotPredicate(ColumnPredicate inner) : ColumnPredicate
{
    private readonly ColumnPredicate _inner = inner ?? throw new ArgumentNullException(nameof(inner));

    public override string ColumnName => $"NOT {_inner.ColumnName}";
    public override int ColumnIndex => _inner.ColumnIndex;

    public override void Evaluate(RecordBatch batch, Span<bool> selection)
    {
        // Create a copy of the selection to evaluate the inner predicate
        var innerSelection = new bool[selection.Length];
        selection.CopyTo(innerSelection);

        // Evaluate the inner predicate
        _inner.Evaluate(batch, innerSelection.AsSpan());

        // Negate the results: if inner selected it, we don't; if inner didn't, we do
        for (int i = 0; i < selection.Length; i++)
        {
            selection[i] = selection[i] && !innerSelection[i];
        }
    }

    protected override bool EvaluateSingle(IArrowArray column, int rowIndex)
    {
        // Compound predicates use Evaluate(RecordBatch, Span<bool>) instead
        // This method is not used for compound predicates
        throw new NotSupportedException("NotPredicate uses Evaluate(RecordBatch, Span<bool>) for evaluation");
    }

    public override string ToString()
    {
        return $"NOT ({_inner})";
    }
}
