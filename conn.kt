import org.springframework.core.ParameterizedTypeReference
import org.springframework.core.codec.DecodingException
import org.springframework.core.io.buffer.DataBufferLimitException
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClient.RequestHeadersSpec
import org.springframework.web.reactive.function.client.WebClientRequestException
import org.springframework.web.util.UriBuilder
import reactor.core.Exceptions
import reactor.core.publisher.Mono
import java.net.URI
import java.time.Duration

class Connection(
    private val configuration: ConnectionConfiguration,
    private val webClient: WebClient
) {

    companion object {
        fun of(configuration: ConnectionConfiguration, host: String): Connection {
            return Connection(configuration, configuration.getWebClient(host))
        }
    }

    private fun <T> Mono<T>.requestSettings(): Mono<T> =
        timeout(Duration.ofMillis(configuration.requestTimeout))
            .retryWhen(configuration.retryPolicy)

    private fun <T : Any> RequestHeadersSpec<*>.exchangeToBodyMono(
        responseType: ParameterizedTypeReference<T>
    ): Mono<T> {
        return exchangeToMono { response ->
            val status = response.statusCode()
            if (status.is2xxSuccessful) {
                response.bodyToMono(responseType)
            } else {
                response
                    .bodyToMono(String::class.java)
                    .defaultIfEmpty("")
                    .flatMap { body ->
                        Mono.error(ConnectionHttpException(status.value(), body))
                    }
            }
        }
            .requestSettings()
            .onErrorMap { throwable ->
                if (Exceptions.isRetryExhausted(throwable)) {
                    ConnectionRetryExhaustedException(Exceptions.unwrap(throwable))
                } else when (throwable) {
                    is WebClientRequestException -> ConnectionNetworkException(throwable)
                    is DecodingException -> ConnectionDeserializationException(throwable)
                    is DataBufferLimitException -> ConnectionDeserializationException(throwable)
                    else -> throwable
                }
            }
    }

    inline fun <reified T : Any> performPostRequest(
        uri: String,
        body: Any? = null
    ): Mono<T> = internalPerformPostRequest(uri, body, object : ParameterizedTypeReference<T>() {})

    inline fun <reified T : Any> performPostRequestWithVariable(
        uri: String,
        pathVariables: Any? = null,
        body: Any? = null
    ): Mono<T> = internalPerformPostRequest({ it.path(uri).build(pathVariables) }, body, object : ParameterizedTypeReference<T>() {})

    inline fun <reified T : Any> performPostRequestWithVariables(
        uri: String,
        pathVariables: Map<String, Any>,
        body: Any? = null
    ): Mono<T> = internalPerformPostRequest({ it.path(uri).build(pathVariables) }, body, object : ParameterizedTypeReference<T>() {})

    inline fun <reified T : Any> performPostRequestWithParams(
        uri: String,
        queryParams: Map<String, Any?>,
        body: Any? = null
    ): Mono<T> = internalPerformPostRequest({ it.buildURI(uri, queryParams) }, body, object : ParameterizedTypeReference<T>() {})

    fun <T : Any> internalPerformPostRequest(
        uri: String,
        body: Any?,
        responseType: ParameterizedTypeReference<T>
    ): Mono<T> = internalPerformPostRequest({ it.path(uri).build() }, body, responseType)

    fun <T : Any> internalPerformPostRequest(
        uri: (UriBuilder) -> URI,
        body: Any?,
        responseType: ParameterizedTypeReference<T>
    ): Mono<T> {
        return Mono.defer {
            val spec = webClient.post()
                .uri(uri)
                .contentType(configuration.mediaType)

            val headersSpec = if (body != null) spec.bodyValue(body) else spec

            headersSpec
                .accept(configuration.mediaType)
                .exchangeToBodyMono(responseType)
        }
    }

    inline fun <reified T : Any> performGetRequest(
        uri: String
    ): Mono<T> = internalPerformGetRequest({ it.path(uri).build() }, object : ParameterizedTypeReference<T>() {})

    inline fun <reified T : Any> performGetRequestWithVariable(
        uri: String,
        pathVariable: Any? = null
    ): Mono<T> = internalPerformGetRequest({ it.path(uri).build(pathVariable) }, object : ParameterizedTypeReference<T>() {})

    inline fun <reified T : Any> performGetRequestWithVariables(
        uri: String,
        pathVariables: Map<String, Any>
    ): Mono<T> = internalPerformGetRequest({ it.path(uri).build(pathVariables) }, object : ParameterizedTypeReference<T>() {})

    inline fun <reified T : Any> performGetRequestWithVariableAndParams(
        uri: String,
        pathVariable: Any,
        queryParams: Map<String, Any?>
    ): Mono<T> = internalPerformGetRequest({ it.buildURI(uri, pathVariable, queryParams) }, object : ParameterizedTypeReference<T>() {})

    inline fun <reified T : Any> performGetRequestWithParams(
        uri: String,
        queryParams: Map<String, Any?>
    ): Mono<T> = internalPerformGetRequest({ it.buildURI(uri, queryParams) }, object : ParameterizedTypeReference<T>() {})

    fun <T : Any> internalPerformGetRequest(
        uri: (UriBuilder) -> URI,
        responseType: ParameterizedTypeReference<T>
    ): Mono<T> {
        return Mono.defer {
            webClient.get()
                .uri(uri)
                .accept(configuration.mediaType)
                .exchangeToBodyMono(responseType)
        }
    }

    inline fun <reified T : Any> performDeleteRequestWithVariables(
        uri: String,
        pathVariables: Map<String, Any>
    ): Mono<T> = performDeleteRequest({ it.path(uri).build(pathVariables) }, object : ParameterizedTypeReference<T>() {})

    fun <T : Any> performDeleteRequest(
        uri: (UriBuilder) -> URI,
        responseType: ParameterizedTypeReference<T>
    ): Mono<T> {
        return Mono.defer {
            webClient.delete()
                .uri(uri)
                .accept(configuration.mediaType)
                .exchangeToBodyMono(responseType)
        }
    }

    inline fun <reified T : Any> performPatchRequest(
        uri: String,
        body: Any? = null
    ): Mono<T> = internalPerformPatchRequest(uri, body, object : ParameterizedTypeReference<T>() {})

    inline fun <reified T : Any> performPatchRequestWithVariable(
        uri: String,
        pathVariables: Any? = null,
        body: Any? = null
    ): Mono<T> = internalPerformPatchRequest({ it.path(uri).build(pathVariables) }, body, object : ParameterizedTypeReference<T>() {})

    inline fun <reified T : Any> performPatchRequestWithVariables(
        uri: String,
        pathVariables: Map<String, Any>,
        body: Any? = null
    ): Mono<T> = internalPerformPatchRequest({ it.path(uri).build(pathVariables) }, body, object : ParameterizedTypeReference<T>() {})

    inline fun <reified T : Any> performPatchRequestWithParams(
        uri: String,
        queryParams: Map<String, Any?>,
        body: Any? = null
    ): Mono<T> = internalPerformPatchRequest({ it.buildURI(uri, queryParams) }, body, object : ParameterizedTypeReference<T>() {})

    fun <T : Any> internalPerformPatchRequest(
        uri: String,
        body: Any?,
        responseType: ParameterizedTypeReference<T>
    ): Mono<T> = internalPerformPatchRequest({ it.path(uri).build() }, body, responseType)

    fun <T : Any> internalPerformPatchRequest(
        uri: (UriBuilder) -> URI,
        body: Any?,
        responseType: ParameterizedTypeReference<T>
    ): Mono<T> {
        return Mono.defer {
            val spec = webClient.patch()
                .uri(uri)
                .contentType(configuration.mediaType)

            val headersSpec = if (body != null) spec.bodyValue(body) else spec

            headersSpec
                .accept(configuration.mediaType)
                .exchangeToBodyMono(responseType)
        }
    }

    fun UriBuilder.buildURI(path: String, queryParams: Map<String, Any?>): URI {
        path(path)
        queryParams.forEach { (key, raw) ->
            when (raw) {
                null -> Unit // skip nulls
                is Iterable<*> -> {
                    val values = raw.filterNotNull().map { it as Any }.toTypedArray()
                    if (values.isNotEmpty()) queryParam(key, *values)
                }
                is Array<*> -> {
                    val values = raw.filterNotNull().map { it as Any }.toTypedArray()
                    if (values.isNotEmpty()) queryParam(key, *values)
                }
                else -> queryParam(key, raw)
            }
        }
        return build()
    }

    fun UriBuilder.buildURI(path: String, pathVariable: Any, queryParams: Map<String, Any?>): URI {
        path(path)
        queryParams.forEach { (key, raw) ->
            when (raw) {
                null -> Unit // skip nulls
                is Iterable<*> -> {
                    val values = raw.filterNotNull().map { it as Any }.toTypedArray()
                    if (values.isNotEmpty()) queryParam(key, *values)
                }
                is Array<*> -> {
                    val values = raw.filterNotNull().map { it as Any }.toTypedArray()
                    if (values.isNotEmpty()) queryParam(key, *values)
                }
                else -> queryParam(key, raw)
            }
        }
        return build(pathVariable)
    }

}

class ConnectionHttpException(
    val statusCode: Int,
    val responseBody: String
) : RuntimeException("HTTP $statusCode: $responseBody")

class ConnectionNetworkException(
    cause: Throwable
) : RuntimeException("Network error while calling remote service", cause)

class ConnectionDeserializationException(
    cause: Throwable
) : RuntimeException("Failed to deserialize response from remote service", cause)

class ConnectionRetryExhaustedException(
    cause: Throwable
) : RuntimeException("Retry attempts exhausted", cause)
