package tech.figure.coroutines.flows

import io.kotest.core.spec.style.AnnotationSpec
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.cancellable
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging

class FlowChunkTest : AnnotationSpec() {
    private val log = KotlinLogging.logger("tester")

    @Test(CancellationException::class)
    fun testEmptyFlowChunked(): Unit = runBlocking {
        val f = flowOf<Int>().cancellable()

        f.chunked(10, 5.seconds).collect {
            log.info("got:${it.toList()}")
            assert(it.isEmpty())
            cancel(CancellationException())
        }
        error("shouldn't be reached")
    }

    @Test(CancellationException::class)
    fun testIncompleteChunked(): Unit = runBlocking {
        val f = flowOf(1, 2, 3, 4, 5).cancellable()

        var i = 0
        f.chunked(2, 2.seconds).collect {
            log.info("got:${it.toList()}")
            assert(it.isNotEmpty())
            if (++i == 3) {
                cancel(CancellationException())
            }
        }
        error("shouldn't be reached")
    }
}
