package ru.finam.tradeapi

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.google.protobuf.Message
import com.google.protobuf.util.JsonFormat
import com.google.protobuf.util.Timestamps
import grpc.tradeapi.v1.marketdata.TimeFrame
import io.grpc.ManagedChannelBuilder
import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.websocket.*
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.websocket.*
import kotlinx.coroutines.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.*

val logger: Logger = LoggerFactory.getLogger("WebSocket.kt")
val mapper: ObjectMapper = ObjectMapper().apply {
    disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    registerModule(KotlinModule.Builder().build())
    registerModule(JavaTimeModule())
}

class WebsocketClient(
    private val host: String = "api.finam.ru",
    private val port: Int = 443,
    private val endpoint: String = "/ws/trading-info",
    private val secret: String,
    parentScope: CoroutineScope? = null
) {
    private val client = HttpClient(CIO) {
        install(WebSockets)
    }
    private val scope = parentScope ?: CoroutineScope(Dispatchers.IO)
    private val authStub = ManagedChannelBuilder
        .forAddress(host, port)
        .build().let { channel -> authServiceStub(channel) }
    private var token: String? = null
    private var tokenRenewalJob: Job? = null
    private var session: DefaultClientWebSocketSession? = null

    suspend fun connect(awaitHandshake: CompletableDeferred<Unit>, onMessage: (String) -> Unit = {}) {
        token = authStub.auth(secret).token
        tokenRenewalJob = startTokenRenewal()

        session = client.webSocketSession(
            urlString = "wss://$host:$port$endpoint",
        ) {
            header(HttpHeaders.Authorization, token)
        }

        for (frame in session!!.incoming) {
            when (frame) {
                is Frame.Text -> {
                    val text = frame.readText()
                    if (isHandshake(text)) {
                        awaitHandshake.complete(Unit)
                    } else {
                        onMessage(text)
                    }
                }

                is Frame.Close -> {
                    logger.info("Connection was closed")
                }

                else -> logger.error("Unsupported frame type ${frame.frameType}")
            }
        }
    }

    suspend fun writeText(payload: WsRequest) {
        if (session == null) {
            throw IllegalStateException("No websocket session has been established")
        }
        if (token == null) {
            throw IllegalStateException("Authentication token not found")
        }
        session!!.outgoing.send(Frame.Text(mapper.writeValueAsString(payload.copy(token = token!!))))
    }

    suspend fun close(code: Short = CloseReason.Codes.NORMAL.code, reason: String = "Client close") {
        tokenRenewalJob?.cancelAndJoin()
        session?.close(CloseReason(code, reason))
        session = null
    }

    private fun startTokenRenewal() = scope.launch {
        authStub.subscribeJwtRenewal(secret).collect {
            token = it.token
            val tokenDetails = authStub.tokenDetails(it.token)
            logger.debug(
                "New auth token received. Expiration: {}",
                Date(Timestamps.toMillis(tokenDetails.expiresAt))
            )
        }
    }

}

data class WsRequest(
    val action: Action,
    val type: SubscriptionType? = null,
    val data: Map<String, Any?>? = null,
    val token: String = ""
) {
    companion object {
        fun subscribeBarsRequest(symbol: String, timeframe: TimeFrame) = WsRequest(
            Action.SUBSCRIBE, SubscriptionType.BARS,
            mapOf(
                "symbol" to symbol,
                "timeframe" to timeframe.name
            )
        )
    }
}

data class WsEvent(val event: String, val code: Int, val reason: String)
data class WsError(val code: Int, val type: String, val message: String? = null)

data class WsEnvelope(
    val type: MessageType,
    @param:JsonProperty("subscription_type")
    val subscriptionType: SubscriptionType? = null,
    @param:JsonProperty("subscription_key")
    val subscriptionKey: String? = null,
    val payload: Any? = null,
    @param:JsonProperty("error_info")
    val errorInfo: WsError? = null,
    @param:JsonProperty("event_info")
    val eventInfo: WsEvent? = null,
    val timestamp: Instant
)

data class WsEnvelopeInfo(
    val type: MessageType,
    @param:JsonProperty("error_info") val errorInfo: WsError? = null,
    @param:JsonProperty("event_info") val eventInfo: WsEvent? = null,
)

enum class MessageType {
    DATA, ERROR, EVENT
}

enum class Action {
    SUBSCRIBE, UNSUBSCRIBE, UNSUBSCRIBE_ALL
}

enum class SubscriptionType {
    ORDERS, TRADES, QUOTES, ORDER_BOOK, BARS, INSTRUMENT_TRADES
}

data class BarsRequest(val symbol: String, val timeframe: String)
data class LatestTradesRequest(val symbol: String)
data class QuotesRequest(val symbols: List<String>)
data class OrderBookRequest(val symbol: String)
data class OrdersRequest(@param:JsonProperty("account_id") val accountId: String)
data class TradesRequest(@param:JsonProperty("account_id") val accountId: String)


inline fun <reified M : Message, B : Message.Builder> parseProto(
    rawJson: String,
    builderSupplier: () -> B,
    field: String = "payload"
): M? {
    val builder = builderSupplier()
    return try {
        mapper.readTree(rawJson).get(field)?.let { node ->
            JsonFormat.parser().ignoringUnknownFields().merge(node.asText(), builder)
            builder.build() as? M
        }
    } catch (_: Exception) {
        logger.error("Failed to parse incoming message: $rawJson")
        null
    }
}

fun parseEnvInfo(message: String): WsEnvelopeInfo? = try {
    mapper.readValue(message, WsEnvelopeInfo::class.java)
} catch (_: Exception) {
    logger.error("Failed to parse envelope info: $message")
    null
}

fun parseEnv(message: String): WsEnvelope? = try {
    mapper.readValue(message, WsEnvelope::class.java)
} catch (_: Exception) {
    logger.error("Failed to parse envelope: $message")
    null
}

fun isHandshake(message: String) = parseEnvInfo(message)
    ?.takeIf { it.type == MessageType.EVENT }?.eventInfo?.let { it.event == "HANDSHAKE_SUCCESS" } ?: false
