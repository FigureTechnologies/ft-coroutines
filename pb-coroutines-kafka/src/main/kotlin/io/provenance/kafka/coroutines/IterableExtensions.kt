package io.provenance.kafka.coroutines

internal fun <T, L : Iterable<T>> L.ifEmpty(block: () -> L): L = if (count() == 0) block() else this