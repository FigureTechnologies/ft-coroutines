package tech.figure.kafka.cli

import ch.qos.logback.classic.Level
import org.slf4j.Logger
import org.slf4j.LoggerFactory

internal var Logger.level: Level
    get() = (this as ch.qos.logback.classic.Logger).level
    set(value) { (this as ch.qos.logback.classic.Logger).level = value }

internal object LoggerDsl {
    var String.level: Level
        get() = LoggerFactory.getLogger(this).level
        set(value) { LoggerFactory.getLogger(this).level = value }
}

internal fun log(block: LoggerDsl.() -> Unit) = LoggerDsl.block()
