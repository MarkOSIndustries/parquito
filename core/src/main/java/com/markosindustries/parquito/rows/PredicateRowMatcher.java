package com.markosindustries.parquito.rows;

public sealed interface PredicateRowMatcher
    permits PredicateRowMatcher.AllMatch, PredicateRowMatcher.AnyMatch {
  boolean rowMatches();

  final class AnyMatch<T> implements PredicateRowMatcher {
    private final DataPageCursor<T> dataPageCursor;
    private final PredicateMaterialisedMatches materialisedMatches;
    private final boolean matchesNull;

    public AnyMatch(
        final DataPageCursor<T> dataPageCursor,
        final PredicateMaterialisedMatches materialisedMatches,
        final boolean matchesNull) {
      this.dataPageCursor = dataPageCursor;
      this.materialisedMatches = materialisedMatches;
      this.matchesNull = matchesNull;
    }

    @Override
    public boolean rowMatches() {
      int dIndex = dataPageCursor.getDefinitionIndex(), vIndex = dataPageCursor.getValueIndex();
      do {
        if (dataPageCursor.getDataPage().getDefinitionLevels()[dIndex]
            == dataPageCursor.getSchemaNode().getDefinitionLevelMax()) {
          if (materialisedMatches.matches(vIndex++)) {
            return true;
          }
        } else if (matchesNull) {
          return true;
        }
        dIndex++;
      } while (dIndex < dataPageCursor.getDataPage().getDefinitionLevels().length
          && dataPageCursor.getDataPage().getRepetitionLevels()[dIndex] != 0);

      return false;
    }
  }

  final class AllMatch<T> implements PredicateRowMatcher {
    private final DataPageCursor<T> dataPageCursor;
    private final PredicateMaterialisedMatches materialisedMatches;
    private final boolean matchesNull;

    public AllMatch(
        final DataPageCursor<T> dataPageCursor,
        final PredicateMaterialisedMatches materialisedMatches,
        final boolean matchesNull) {
      this.dataPageCursor = dataPageCursor;
      this.materialisedMatches = materialisedMatches;
      this.matchesNull = matchesNull;
    }

    @Override
    public boolean rowMatches() {
      int dIndex = dataPageCursor.getDefinitionIndex(), vIndex = dataPageCursor.getValueIndex();
      do {
        if (dataPageCursor.getDataPage().getDefinitionLevels()[dIndex]
            == dataPageCursor.getSchemaNode().getDefinitionLevelMax()) {
          if (!materialisedMatches.matches(vIndex++)) {
            return false;
          }
        } else if (matchesNull) {
          return false;
        }
        dIndex++;
      } while (dIndex < dataPageCursor.getDataPage().getDefinitionLevels().length
          && dataPageCursor.getDataPage().getRepetitionLevels()[dIndex] != 0);

      return true;
    }
  }
}
