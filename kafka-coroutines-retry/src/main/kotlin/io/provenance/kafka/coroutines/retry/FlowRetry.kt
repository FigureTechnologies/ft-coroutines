package io.provenance.kafka.coroutines.retry

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.transform
import mu.KotlinLogging

fun <T> logErrorFlowHandler(): suspend (value: T, throwable: Throwable) -> Unit {
    val log = KotlinLogging.logger {}
    return { value, throwable ->
        log.error("Failed to handle $value", throwable)
    }
}

fun <T> Flow<T>.tryOnEach(
    onFailure: suspend (value: T, throwable: Throwable) -> Unit = logErrorFlowHandler(),
    tryBlock: suspend (value: T) -> Unit,
): Flow<T> = onEach { value: T ->
    runCatching { tryBlock(value) }.fold(
        onSuccess = { },
        onFailure = { onFailure(value, it) }
    )
}

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
