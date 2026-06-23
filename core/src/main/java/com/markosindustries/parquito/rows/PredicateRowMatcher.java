package com.markosindustries.parquito.rows;

public interface PredicateRowMatcher {
  boolean rowMatches();

  final class AnyMatch implements PredicateRowMatcher {
    private final DataPageCursor dataPageCursor;
    private final PredicateMaterialisedMatches materialisedMatches;
    private final boolean matchesNull;

    public AnyMatch(
        final DataPageCursor dataPageCursor,
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

  final class AllMatch implements PredicateRowMatcher {
    private final DataPageCursor dataPageCursor;
    private final PredicateMaterialisedMatches materialisedMatches;
    private final boolean matchesNull;

    public AllMatch(
        final DataPageCursor dataPageCursor,
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

  final class NoneMatch implements PredicateRowMatcher {
    private final DataPageCursor dataPageCursor;
    private final PredicateMaterialisedMatches materialisedMatches;
    private final boolean matchesNull;

    public NoneMatch(
        final DataPageCursor dataPageCursor,
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
