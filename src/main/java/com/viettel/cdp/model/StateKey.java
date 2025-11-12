package com.viettel.cdp.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class StateKey implements Serializable {
    private String key;
    private Integer windowSize;
    private Integer slideSize;
    private TimeUnit timeUnit;
}
