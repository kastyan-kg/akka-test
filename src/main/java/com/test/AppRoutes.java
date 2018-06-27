package com.test;

import akka.actor.ActorRef;
import akka.http.javadsl.marshallers.jackson.Jackson;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.server.Route;
import akka.http.javadsl.server.directives.RouteAdapter;
import akka.pattern.PatternsCS;
import akka.util.Timeout;
import com.test.actors.FileLoaderActor;
import com.test.messages.FilePaths;
import com.test.messages.FileStatistics;
import com.test.messages.Statistics;
import lombok.AllArgsConstructor;

import java.io.File;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static akka.http.javadsl.server.Directives.*;
import static akka.http.javadsl.server.PathMatchers.segment;

@AllArgsConstructor
public class AppRoutes {
    public static final String STATISTICS_PATH = "statistics";
    public static final String DOWNLOAD_PATH = "download";
    public static final String LOAD_PATH = "load";

    private static final Timeout TIMEOUT = Timeout.apply(5, TimeUnit.MINUTES);

    private final ActorRef fileLoader;

    @SuppressWarnings("unchecked")
    public Route routes() {
        return route(
                post(() -> path(LOAD_PATH, () -> extractRequestContext(ctx ->
                        fileUpload("file", (metadata, byteSource) -> {
                            String fileName = metadata.getFileName();
                            CompletionStage<Map<String, FilePaths>> paths = PatternsCS
                                    .ask(fileLoader, new FileLoaderActor.LoadFile(byteSource, fileName), TIMEOUT)
                                    .thenApply(obj -> (Map<String, FilePaths>) obj);

                            return onSuccess(() -> paths,
                                    performed -> complete(StatusCodes.OK, performed, Jackson.marshaller())
                            );
                        })))),
                get(() -> pathPrefix(STATISTICS_PATH, () ->
                        path(segment().slash(segment()), (String loadFileName, String sortedFileName) -> {
                            CompletionStage<Optional<FileStatistics>> stats = PatternsCS
                                    .ask(fileLoader, new FileLoaderActor.GetFileStatistics(loadFileName, sortedFileName), TIMEOUT)
                                    .thenApply(obj -> obj instanceof FileStatistics ? Optional.of((FileStatistics)obj) : Optional.empty());

                            return onSuccess(() -> stats,
                                    performed -> performed.map(stat -> complete(StatusCodes.OK, performed.get(), Jackson.marshaller()))
                                    .orElseGet(() -> (RouteAdapter) complete(StatusCodes.NOT_FOUND))
                            );
                        }))),
                get(() -> pathPrefix(STATISTICS_PATH, () -> {
                    CompletionStage<Map<String, Statistics>> stats = PatternsCS
                            .ask(fileLoader, new FileLoaderActor.GetAllStatistics(), TIMEOUT)
                            .thenApply(obj -> (Map<String, Statistics>) obj);

                    return onSuccess(() -> stats,
                            performed -> complete(StatusCodes.OK, performed, Jackson.marshaller())
                    );
                })),
                get(() -> path(segment(DOWNLOAD_PATH).slash(segment().slash(segment())), (String loadFileName, String sortedFileName) ->
                        getFromFile(loadFileName + File.pathSeparator + sortedFileName)))
        );
    }
}
