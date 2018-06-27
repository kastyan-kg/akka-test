package com.test.actors;

import akka.Done;
import akka.actor.AbstractActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.function.Function2;
import akka.pattern.PatternsCS;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.*;
import akka.util.ByteString;
import com.test.AppRoutes;
import com.test.Application;
import com.test.messages.FilePaths;
import com.test.messages.FileStatistics;
import com.test.messages.Statistics;
import lombok.AllArgsConstructor;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileLoaderActor extends AbstractActor {
    private static final int PARALLELISM = 4;
    private static final int FILE_NAME_INDEX = 0;
    private static final int CONTENT_INDEX = 1;
    private static final String SEPARATOR = "\n";
    private static final Set<StandardOpenOption> FILE_WRITING_SETTINGS = Stream.of(StandardOpenOption.APPEND, StandardOpenOption.CREATE)
            .collect(Collectors.toSet());

    private final LoggingAdapter logger = Logging.getLogger(context().system(), this);

    private Map<String, Statistics> statisticsByFileName = new ConcurrentHashMap<>();

    private ActorMaterializer actorMaterializer;

    @Override
    public void preStart() throws Exception {
        super.preStart();

        actorMaterializer = ActorMaterializer.create(context());
    }

    @AllArgsConstructor
    public static class LoadFile {
        private final Source<ByteString, Object> fileSource;
        private final String fileName;
    }

    public static class GetAllStatistics {
    }

    @AllArgsConstructor
    public static class GetFileStatistics {
        private final String loadedFileName;
        private final String sortedFileName;
    }

    public static class StatisticsNotFound {
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(LoadFile.class, loadFile -> {
                    Source<ByteString, Object> fileSource = loadFile.fileSource;
                    String loadFileName = loadFile.fileName;

                    clearFiles(loadFileName);

                    Sink<ByteString, CompletionStage<Done>> writeToFilesSink = Sink.foreachParallel(PARALLELISM, line ->
                            Source.single(getContent(line))
                            .runWith(FileIO.toPath(Paths.get( loadFileName + File.pathSeparator + getFileName(line)),
                                    FILE_WRITING_SETTINGS), actorMaterializer), context().dispatcher());

                    Sink<ByteString, CompletionStage<Statistics>> statisticsSink = Sink.fold(new Statistics(loadFileName), collectStatistics());

                    CompletionStage<Statistics> statisticsCompletionStage = fileSource
                            .via(Framing.delimiter(ByteString.fromString("\n"), 1024))
                            .alsoToMat(writeToFilesSink, Keep.right())
                            .runWith(statisticsSink, actorMaterializer);

                    CompletionStage<Statistics> statistic = statisticsCompletionStage.thenApply(processPercentages());
                    statistic.thenAccept(s -> statisticsByFileName.put(s.getDownloadedFileName(), s));

                    PatternsCS.pipe(statistic.thenApply(this::convertToPaths), context().dispatcher())
                            .to(sender());
                })
                .match(GetAllStatistics.class, getAllStatistics -> {
                    sender().tell(statisticsByFileName, self());
                })
                .match(GetFileStatistics.class, getFileStatistics -> {
                    if(statisticsByFileName.containsKey(getFileStatistics.loadedFileName)
                            && statisticsByFileName.get(getFileStatistics.loadedFileName)
                            .getStatisticByFileName().containsKey(getFileStatistics.sortedFileName)){

                        FileStatistics fileStatistics = statisticsByFileName.get(getFileStatistics.loadedFileName)
                                .getStatisticByFileName().get(getFileStatistics.sortedFileName);
                        sender().tell(fileStatistics, sender());
                    }else {
                        sender().tell(new StatisticsNotFound(), self());
                    }
                })
                .build();
    }

    private void clearFiles(String loadFileName) {
        File defaultFolder = new File(FileSystems.getDefault().getPath("").toUri());
        File[] files = defaultFolder.listFiles((dir, name) -> name.contains(loadFileName + File.pathSeparator));
        if (files != null) {
            for (File file : files) {
                if(!file.delete()) {
                    logger.error("Can't remove file {}, data in file will be not correct.", file.toPath().toString());
                }
            }
        }
    }

    private Map<String, FilePaths> convertToPaths(Statistics s) {
        Map<String, FilePaths> paths = new HashMap<>();
        for (String fileName : s.getStatisticByFileName().keySet()) {
            paths.put(fileName, new FilePaths(buildPath(AppRoutes.STATISTICS_PATH, s.getDownloadedFileName(), fileName),
                    buildPath(AppRoutes.DOWNLOAD_PATH, s.getDownloadedFileName(), fileName)));
        }
        return paths;
    }

    private String buildPath(String innerPath, String loadedFileName, String fileName) {
        return String.format("http://%s:%s/%s/%s/%s", Application.HOST, Application.PORT, innerPath, loadedFileName, fileName);
    }

    private Function<Statistics, Statistics> processPercentages() {
        return statisticsResult -> {
            for (String fileName : statisticsResult.getStatisticByFileName().keySet()) {
                int entries = statisticsResult.getStatisticByFileName().get(fileName).getEntries().intValue();
                statisticsResult.getStatisticByFileName().put(fileName, new FileStatistics(new AtomicInteger(entries), calculatePercentage(entries, statisticsResult.getRecordsRead().intValue())));
            }
            return statisticsResult;
        };
    }

    private String calculatePercentage(int entries, int total) {
        return String.format("%s%%", (entries * 100.0f) / total);
    }

    private Function2<Statistics, ByteString, Statistics> collectStatistics() {
        return (stats, line) -> {
            stats.getRecordsRead().incrementAndGet();
            ConcurrentHashMap<String, FileStatistics> statisticByFileName = stats.getStatisticByFileName();
            String fileName = getFileName(line);
            if (statisticByFileName.containsKey(fileName)) {
                statisticByFileName.get(fileName).getEntries().incrementAndGet();
            } else {
                statisticByFileName.put(fileName, new FileStatistics(new AtomicInteger(1), ""));
            }
            return stats;
        };
    }

    private String getFileName(ByteString content) {
        return content.utf8String().split(",", 2)[FILE_NAME_INDEX];
    }

    private ByteString getContent(ByteString content) {
        return ByteString.fromString(content.utf8String().split(",", 2)[CONTENT_INDEX] + SEPARATOR);
    }
}
