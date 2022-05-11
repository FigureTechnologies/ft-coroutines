package io.provenance.kafka

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map

suspend fun <K, V> UnAckedConsumerRecord<K, V>.acking(
    block: suspend (UnAckedConsumerRecord<K, V>) -> Unit = {}
): AckedConsumerRecord<K, V> {
    block(this)
    return ack()
}

fun <K, V> Flow<UnAckedConsumerRecord<K, V>>.acking(
    block: suspend (UnAckedConsumerRecord<K, V>) -> Unit = {}
): Flow<AckedConsumerRecord<K, V>> = map { it.acking(block) }
