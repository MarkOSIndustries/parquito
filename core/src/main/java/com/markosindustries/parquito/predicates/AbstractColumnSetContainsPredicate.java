package com.markosindustries.parquito.predicates;

import com.markosindustries.parquito.ColumnValuesSet;
import com.markosindustries.parquito.ParquetSchemaPath;
import com.markosindustries.parquito.rows.PredicateRowMatcher;

public abstract class AbstractColumnSetContainsPredicate<
        Converted, RowMatcher extends PredicateRowMatcher>
    extends ColumnPredicate<Converted, RowMatcher> {
  protected final ColumnValuesSet<Converted> referenceValues;

  public AbstractColumnSetContainsPredicate(
      final ColumnValuesSet<Converted> referenceValues,
      final ParquetSchemaPath schemaPath,
      final RowMatcherConstructor<RowMatcher> rowMatcherConstructor) {
    super(schemaPath, rowMatcherConstructor);
    this.referenceValues = referenceValues;
  }
}
