package tech.figure.coroutines

import mu.KotlinLogging

/**
 * Generic logging flow error handler.
 */
fun <T> logErrorFlowHandler(): suspend (value: T, throwable: Throwable) -> Unit {
    val log = KotlinLogging.logger {}
    return { value, throwable ->
        log.error("Failed to handle $value", throwable)
    }
}
