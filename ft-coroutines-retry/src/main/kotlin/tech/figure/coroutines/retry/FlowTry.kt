package tech.figure.coroutines.retry

import tech.figure.coroutines.retry.flow.FlowProcessor
import tech.figure.coroutines.tryOnEach
import kotlinx.coroutines.flow.Flow
import mu.KotlinLogging

/**
 * Wrap [onEach] into a try {} catch {} to allow dropping the failed flow element into a [FlowProcessor] for later reprocessing.
 *
 * @param flowProcessor The [FlowProcessor] containing callbacks for processing and error handling.
 * @return The original flow.
 */
fun <T> Flow<T>.tryOnEachProcess(flowProcessor: FlowProcessor<T>): Flow<T> = tryOnEach(
    onFailure = { it, e ->
        KotlinLogging.logger {}.warn("failed to process record", e)
        flowProcessor.send(it, e)
    },
    tryBlock = {
        flowProcessor.process(it)
    }
)
