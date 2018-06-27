package com.test;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import com.test.actors.FileLoaderActor;

import java.io.IOException;
import java.util.concurrent.CompletionStage;

public class Application {
    public static final String HOST = "localhost";
    public static final int PORT = 8080;


    private static final String ACTOR_SYSTEM_NAME = "MyActorSystem";

    public static void main(String[] args) throws IOException {
        ActorSystem actorSystem = ActorSystem.create(ACTOR_SYSTEM_NAME);

        final Http http = Http.get(actorSystem);
        final ActorMaterializer materializer = ActorMaterializer.create(actorSystem);

        AppRoutes appRoutes = new AppRoutes(actorSystem.actorOf(Props.create(FileLoaderActor.class)));

        final Flow<HttpRequest, HttpResponse, NotUsed> routeFlow = appRoutes.routes().flow(actorSystem, materializer);
        final CompletionStage<ServerBinding> binding = http.bindAndHandle(routeFlow,
                ConnectHttp.toHost(HOST, PORT), materializer);

        LoggingAdapter logger = Logging.getLogger(actorSystem, Application.class);
        logger.info("Server online at http://{}:{}/", HOST, PORT);
        logger.info("Press RETURN to stop...");
        System.in.read();

        binding.thenCompose(ServerBinding::unbind)
                .thenAccept(unbound -> actorSystem.terminate());
    }
}
