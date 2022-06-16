package io.provenance.kafka.records

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.transform

suspend fun <K, V> UnAckedConsumerRecord<K, V>.acking(
    block: suspend (UnAckedConsumerRecord<K, V>) -> Unit = {}
): AckedConsumerRecord<K, V> {
    block(this)
    return ack()
}

fun <K, V> Flow<UnAckedConsumerRecord<K, V>>.acking(
    every: Int = 1,
    block: suspend (UnAckedConsumerRecord<K, V>) -> Unit = {}
): Flow<AckedConsumerRecord<K, V>> {
    var current = 0
    return transform {
        if (current++ % every == 0) {
            emit(it.acking(block))
        }
    }
}
