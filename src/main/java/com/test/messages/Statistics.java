package com.test.messages;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Getter
public class Statistics {
    @JsonIgnore
    private final String downloadedFileName;
    private final AtomicInteger recordsRead = new AtomicInteger();
    private final ConcurrentHashMap<String, FileStatistics> statisticByFileName = new ConcurrentHashMap<>();

    public Statistics(String downloadedFileName) {
        this.downloadedFileName = downloadedFileName;
    }
}
