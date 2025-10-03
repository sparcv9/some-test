import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.util.UriBuilder
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

    private fun <T> Mono<T>.requestSettings(uri: String): Mono<T> = this
        .timeout(Duration.ofMillis(configuration.requestTimeout))
        .retryWhen(configuration.retryPolicy)

    inline fun <reified T : Any> performPostRequest(
        uri: String,
        body: Any? = null,
        noinline mapper: ((T) -> Any)? = null
    ): Mono<T> = internalPerformPostRequest(uri, body, mapper, object : ParameterizedTypeReference<T>() {})

    inline fun <reified T : Any> performPostRequestWithVariable(
        uri: String,
        pathVariables: Any? = null,
        body: Any? = null,
        noinline mapper: ((T) -> Any)? = null
    ): Mono<T> = internalPerformPostRequest({ it.path(uri).build(pathVariables) }, body, mapper, object : ParameterizedTypeReference<T>() {})

    inline fun <reified T : Any> performPostRequestWithVariables(
        uri: String,
        pathVariables: Map<String, Any>,
        body: Any? = null,
        noinline mapper: ((T) -> Any)? = null
    ): Mono<T> = internalPerformPostRequest({ it.path(uri).build(pathVariables) }, body, mapper, object : ParameterizedTypeReference<T>() {})

    inline fun <reified T : Any> performPostRequestWithParams(
        uri: String,
        queryParams: Map<String, Any?>,
        body: Any? = null,
        noinline mapper: ((T) -> Any)? = null
    ): Mono<T> = internalPerformPostRequest({ it.buildURI(uri, queryParams) }, body, mapper, object : ParameterizedTypeReference<T>() {})

    fun <T : Any> internalPerformPostRequest(
        uri: String,
        body: Any?,
        mapper: ((T) -> Any)?,
        responseType: ParameterizedTypeReference<T>
    ): Mono<T> = internalPerformPostRequest({ it.path(uri).build() }, body, mapper, responseType)

    fun <T : Any> internalPerformPostRequest(
        uri: (UriBuilder) -> URI,
        body: Any?,
        mapper: ((T) -> Any)?,
        responseType: ParameterizedTypeReference<T>
    ): Mono<T> {
        return Mono.defer {
            val request = webClient.post()
                .uri(uri)
                .contentType(configuration.mediaType)
                .apply { if (body != null) bodyValue(body) }
                .retrieve()
                .bodyToMono(responseType)
                .requestSettings(uri.toString())

            @Suppress("UNCHECKED_CAST")
            mapper?.let { request.map(it as (Any?) -> T) } ?: request
        }
    }

    inline fun <reified T : Any> performGetRequest(
        uri: String,
        noinline mapper: ((T) -> Any)? = null
    ): Mono<T> = internalPerformGetRequest({ it.path(uri).build() }, mapper, object : ParameterizedTypeReference<T>() {})

    inline fun <reified T : Any> performGetRequestWithVariable(
        uri: String,
        pathVariable: Any? = null,
        noinline mapper: ((T) -> Any)? = null
    ): Mono<T> = internalPerformGetRequest({ it.path(uri).build(pathVariable) }, mapper, object : ParameterizedTypeReference<T>() {})

    inline fun <reified T : Any> performGetRequestWithVariables(
        uri: String,
        pathVariables: Map<String, Any>,
        noinline mapper: ((T) -> Any)? = null
    ): Mono<T> = internalPerformGetRequest({ it.path(uri).build(pathVariables) }, mapper, object : ParameterizedTypeReference<T>() {})

    inline fun <reified T : Any> performGetRequestWithVariableAndParams(
        uri: String,
        pathVariable: Any,
        queryParams: Map<String, Any?>,
        noinline mapper: ((T) -> Any)? = null
    ): Mono<T> = internalPerformGetRequest({ it.buildURI(uri, pathVariable, queryParams) }, mapper, object : ParameterizedTypeReference<T>() {})

    inline fun <reified T : Any> performGetRequestWithParams(
        uri: String,
        queryParams: Map<String, Any?>,
        noinline mapper: ((T) -> Any)? = null
    ): Mono<T> = internalPerformGetRequest({ it.buildURI(uri, queryParams) }, mapper, object : ParameterizedTypeReference<T>() {})

    fun <T : Any> internalPerformGetRequest(
        uri: (UriBuilder) -> URI,
        mapper: ((T) -> Any)? = null,
        responseType: ParameterizedTypeReference<T>
    ): Mono<T> {
        return Mono.defer {
            val request = webClient.get()
                .uri(uri)
                .accept(configuration.mediaType)
                .retrieve()
                .bodyToMono(responseType)
                .requestSettings(uri.toString())

            @Suppress("UNCHECKED_CAST")
            mapper?.let { request.map(it as (Any?) -> T) } ?: request
        }
    }

    inline fun <reified T : Any> performDeleteRequestWithVariables(
        uri: String,
        pathVariables: Map<String, Any>,
        noinline mapper: ((T) -> Any)? = null
    ): Mono<T> = performDeleteRequest({ it.path(uri).build(pathVariables) }, mapper, object : ParameterizedTypeReference<T>() {})

    fun <T : Any> performDeleteRequest(
        uri: (UriBuilder) -> URI,
        mapper: ((T) -> Any)? = null,
        responseType: ParameterizedTypeReference<T>
    ): Mono<T> {
        return Mono.defer {
            val request = webClient.delete()
                .uri(uri)
                .retrieve()
                .bodyToMono(responseType)
                .requestSettings(uri.toString())
            @Suppress("UNCHECKED_CAST")
            mapper?.let { request.map(it as (Any?) -> T) } ?: request
        }
    }

    inline fun <reified T : Any> performPatchRequest(
        uri: String,
        body: Any? = null,
        noinline mapper: ((T) -> Any)? = null
    ): Mono<T> = internalPerformPatchRequest(uri, body, mapper, object : ParameterizedTypeReference<T>() {}
    )

    inline fun <reified T : Any> performPatchRequestWithVariable(
        uri: String,
        pathVariables: Any? = null,
        body: Any? = null,
        noinline mapper: ((T) -> Any)? = null
    ): Mono<T> = internalPerformPatchRequest({ it.path(uri).build(pathVariables) }, body, mapper, object : ParameterizedTypeReference<T>() {}
    )

    inline fun <reified T : Any> performPatchRequestWithVariables(
        uri: String,
        pathVariables: Map<String, Any>,
        body: Any? = null,
        noinline mapper: ((T) -> Any)? = null
    ): Mono<T> = internalPerformPatchRequest({ it.path(uri).build(pathVariables) }, body, mapper, object : ParameterizedTypeReference<T>() {}
    )

    inline fun <reified T : Any> performPatchRequestWithParams(
        uri: String,
        queryParams: Map<String, Any?>,
        body: Any? = null,
        noinline mapper: ((T) -> Any)? = null
    ): Mono<T> = internalPerformPatchRequest({ it.buildURI(uri, queryParams) }, body, mapper, object : ParameterizedTypeReference<T>() {}
    )

    fun <T : Any> internalPerformPatchRequest(
        uri: String,
        body: Any?,
        mapper: ((T) -> Any)?,
        responseType: ParameterizedTypeReference<T>
    ): Mono<T> = internalPerformPatchRequest({ it.path(uri).build() }, body, mapper, responseType
    )

    fun <T : Any> internalPerformPatchRequest(
        uri: (UriBuilder) -> URI,
        body: Any?,
        mapper: ((T) -> Any)?,
        responseType: ParameterizedTypeReference<T>
    ): Mono<T> {
        return Mono.defer {
            val request = webClient.patch()
                .uri(uri)
                .contentType(MediaType.APPLICATION_JSON)
                .apply { if (body != null) bodyValue(body) }
                .retrieve()
                .bodyToMono(responseType)
                .requestSettings(uri.toString())

            @Suppress("UNCHECKED_CAST")
            mapper?.let { request.map(it as (Any?) -> T) } ?: request
        }
    }

    fun UriBuilder.buildURI(path: String, queryParams: Map<String, Any?>): URI {
        path(path)
        queryParams.forEach { (key, value) ->
            when (value) {
                is Collection<*> -> queryParam(key, value)
                else -> queryParam(key, value.toString())
            }
        }
        return build()
    }

    fun UriBuilder.buildURI(path: String, pathVariable: Any, queryParams: Map<String, Any?>): URI {
        path(path)
        queryParams.forEach { (key, value) ->
            when (value) {
                is Collection<*> -> queryParam(key, value)
                else -> queryParam(key, value.toString())
            }
        }
        return build(pathVariable)
    }

}
