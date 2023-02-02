package tech.figure.coroutines.retry.flow

import java.time.OffsetDateTime
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import tech.figure.coroutines.retry.store.RetryRecord
import tech.figure.coroutines.retry.store.RetryRecordStore

const val DEFAULT_RETRY_GROUP_SIZE = 40
const val DEFAULT_RETRY_COLLECTION_NAME = ""

/**
 * Retry a flow of objects using the backing [store].
 *
 * @param handler The handler to reprocess with.
 * @param store [RetryRecordStore] to save and retrieve data object of type [T] from.
 * @param groupSize Process a max of this many elements each poll loop (aka: limit).
 */
fun <T> recordStoreFlowRetry(
    handler: suspend (T) -> Unit,
    store: RetryRecordStore<T>,
    groupSize: Int = DEFAULT_RETRY_GROUP_SIZE,
): FlowRetry<T> = RecordStoreFlowRetry(handler, store, groupSize)

/**
 * Retry a flow of objects using the backing [store].
 *
 * @param handler The handler to reprocess with.
 * @param store [RetryRecordStore] to save and retrieve data object of type [T] from.
 * @param groupSize Process a max of this many elements each poll loop (aka: limit).
 */
internal open class RecordStoreFlowRetry<T>(
    private val handler: suspend (T) -> Unit,
    private val store: RetryRecordStore<T>,
    private val groupSize: Int = DEFAULT_RETRY_GROUP_SIZE,
) : FlowRetry<T> {
    override suspend fun produceNext(
        attemptRange: IntRange,
        olderThan: OffsetDateTime,
        limit: Int
    ): Flow<RetryRecord<T>> =
        store
            .select(attemptRange, olderThan, limit)
            .sortedByDescending { it.lastAttempted }
            .take(groupSize)
            .asFlow()

    override suspend fun send(item: T, e: Throwable) =
        store.insert(item, e)

    override suspend fun onSuccess(item: RetryRecord<T>) =
        store.remove(item.data)

    override suspend fun onFailure(item: RetryRecord<T>, e: Throwable) =
        store.update(item.data, e)

    override suspend fun process(item: T, attempt: Int) =
        handler(item)

    override suspend fun hasNext() =
        !store.isEmpty()
}
