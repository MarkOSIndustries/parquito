package com.markosindustries.parquito.arrays;

public sealed interface FastArray64 extends FastArray
    permits LongArray, LongArraySlice, LongListBoxless, LongListBoxed {}
