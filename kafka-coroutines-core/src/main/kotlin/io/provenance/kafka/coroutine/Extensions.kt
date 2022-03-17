package io.provenance.kafka.coroutine

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map

internal fun <T, L : Iterable<T>> L.ifEmpty(block: () -> L): L = if (count() == 0) block() else this

fun <K, V> Flow<UnAckedConsumerRecord<K, V>>.acking(
    block: (UnAckedConsumerRecord<K, V>) -> Unit = {}
): Flow<AckedConsumerRecord<K, V>> = map {
    block(it)
    it.ack()
}
