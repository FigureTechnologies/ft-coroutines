package tech.figure.coroutines.retry.flow

import io.kotest.core.spec.style.AnnotationSpec
import java.time.OffsetDateTime
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.fail
import tech.figure.coroutines.retry.store.RetryRecord

@OptIn(ExperimentalCoroutinesApi::class)
class TestRetryFlow : AnnotationSpec() {
    suspend fun <E> List<E>.asChannel(): Channel<E> {
        val ch = Channel<E>(capacity = size)
        forEach {
            ch.send(it)
        }
        return ch
    }

    suspend fun <T> testFlowRetry(
        list: List<RetryRecord<T>>,
        onSuccess: (item: T) -> Unit,
        onFailure: (item: T) -> Unit,
        block: (item: T, attempt: Int) -> Unit,
    ): SimpleChannelFlowRetry<T> {
        return SimpleChannelFlowRetry(
            queue = list.asChannel(),
            onSuccess = onSuccess,
            onFailure = onFailure,
            block = block,
        )
    }

    @Test
    fun testRetryFlowEmpty() = runTest {
        val flow = testFlowRetry(
            emptyList<RetryRecord<Int>>(),
            { fail("should not be called") },
            { fail("should not be called") },
            { _, _ -> fail("should not be called") },
        )

        assert(!flow.hasNext()) { "hasNext should be false" }
        assert(flow.produceNext(0..Int.MAX_VALUE, OffsetDateTime.MIN).toList().isEmpty()) { "should produce nothing" }
    }

    @Test
    fun testRetryFlowNotEmpty() = runTest {
        val flow = testFlowRetry(
            listOf(RetryRecord(1), RetryRecord(2), RetryRecord(3)),
            { },
            { fail("should not be called") },
            { _, _ -> },
        )

        assert(flow.drain().size == 3) { "not all elements produced" }
    }
}

private suspend fun <T> FlowRetry<T>.drain(): List<RetryRecord<T>> {
    val list = mutableListOf<RetryRecord<T>>()
    while (hasNext()) {
        list.addAll(produceNext(0..Int.MAX_VALUE, OffsetDateTime.MIN, Int.MAX_VALUE).toList())
    }
    return list
}
