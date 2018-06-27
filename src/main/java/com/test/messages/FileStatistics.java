package com.test.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.concurrent.atomic.AtomicInteger;

@Getter
@EqualsAndHashCode
public class FileStatistics {
    private final AtomicInteger entries;
    private final String percentageToTotal;

    public FileStatistics(AtomicInteger entries, String percentageToTotal) {
        this.entries = entries;
        this.percentageToTotal = percentageToTotal;
    }
}
