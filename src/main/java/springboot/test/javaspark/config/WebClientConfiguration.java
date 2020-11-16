//package springboot.test.javaspark.config;
//
//import io.netty.channel.ChannelOption;
//import io.netty.handler.timeout.ReadTimeoutHandler;
//import io.netty.handler.timeout.WriteTimeoutHandler;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.context.annotation.Bean;
//import org.springframework.http.HttpHeaders;
//import org.springframework.http.MediaType;
//import org.springframework.http.client.reactive.ReactorClientHttpConnector;
//
//import java.util.List;
//
//public class WebClientConfiguration {
//    private static final Logger logger = LoggerFactory.getLogger(WebClientConfiguration.class.getName());
//
//    @Bean
//    public WebClient webClient() {
//
//        var tcpClient = TcpClient.create()
//                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 60_000)
//                .doOnConnected(connection -> {
//                    connection
//                            .addHandlerLast(new ReadTimeoutHandler(60))
//                            .addHandlerLast(new WriteTimeoutHandler(60));
//                });
//
//        return WebClient
//                .builder()
//                .clientConnector(new ReactorClientHttpConnector(HttpClient.from(tcpClient)))
//                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
//                .defaultHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
//                .filter(logRequest())
//                .filter(logResponse())
//                .build();
//
//    }
//
//    private ExchangeFilterFunction logRequest() {
//        return (clientRequest, next) -> {
//            logger.info("Request: {} {}", clientRequest.method(), clientRequest.url());
//            logger.info("--- Http Headers: ---");
//            clientRequest.headers().forEach(this::logHeader);
//            logger.info("--- Http Cookies: ---");
//            clientRequest.cookies().forEach(this::logHeader);
//            return next.exchange(clientRequest);
//        };
//    }
//
//    private ExchangeFilterFunction logResponse() {
//        return ExchangeFilterFunction.ofResponseProcessor(clientResponse -> {
//            logger.info("Response: {} {}", clientResponse.statusCode(), clientResponse.cookies());
//            clientResponse.headers().asHttpHeaders()
//                    .forEach((name, values) -> values.forEach(value -> logger.info("{}={}", name, value)));
//            return Mono.just(clientResponse);
//        });
//    }
//
//    private void logHeader(String name, List<String> values) {
//        values.forEach(value -> logger.info("{}={}", name, value));
//    }
//
//}
