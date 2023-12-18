package io.github.xiaoso456.demo.flink.watermark;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.OffsetDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Event implements Serializable {
    private Long id;
    private String name;
    private Long createTime;
}
