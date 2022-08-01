package io.provenance.kafka.records

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.transform

suspend fun <K, V> UnAckedConsumerRecord<K, V>.acking(
    block: suspend (UnAckedConsumerRecord<K, V>) -> Unit = {}
): AckedConsumerRecord<K, V> {
    block(this)
    return ack()
}

fun <K, V> Flow<UnAckedConsumerRecords<K, V>>.acking(
    block: suspend (UnAckedConsumerRecord<K, V>) -> Unit = {}
): Flow<AckedConsumerRecord<K, V>> {
    return transform { it.forEach { record -> emit(record.acking(block)) } }
}
