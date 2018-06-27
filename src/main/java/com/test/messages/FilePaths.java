package com.test.messages;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class FilePaths {
    private final String statisticsPath;
    private final String downloadPath;
}
