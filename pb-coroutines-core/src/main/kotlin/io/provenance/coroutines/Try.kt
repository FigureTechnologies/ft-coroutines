package io.provenance.coroutines

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.transform

/**
 * Wrap [onEach] into a try {} catch {} to allow dropping the failed flow element into the [onFailure] handler for later reprocessing.
 *
 * @param onFailure The method used to submit the failed element into retry flow.
 * @param tryBlock The method to use to initially process the flow element.
 * @return The original flow.
 */
fun <T> Flow<T>.tryOnEach(
    onFailure: suspend (value: T, throwable: Throwable) -> Unit = logErrorFlowHandler(),
    tryBlock: suspend (value: T) -> Unit,
): Flow<T> = onEach { value: T ->
    runCatching { tryBlock(value) }.fold(
        onSuccess = { },
        onFailure = { onFailure(value, it) }
    )
}

/**
 * Wrap [transform] into a try {} catch {} to allow dropping the failed flow element into the [onFailure] handler for later reprocessing.
 *
 * @param onFailure The method used to submit the failed element into retry flow.
 * @param tryBlock The method to use to initially process the flow element.
 * @return A [Flow] of successful output from [tryBlock].
 */
fun <T, R> Flow<T>.tryMap(
    onFailure: suspend (value: T, throwable: Throwable) -> Unit = logErrorFlowHandler(),
    tryBlock: suspend (value: T) -> R,
): Flow<R> = transform { value: T ->
    runCatching { tryBlock(value) }.fold(
        onSuccess = { emit(it) },
        onFailure = {
            runCatching { onFailure(value, it) }.fold(
                onSuccess = {},
                onFailure = {},
            )
        },
    )
}
