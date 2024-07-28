package io.github.xiaoso456.demo.flink.cdc.mysql.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventAssetLink {

    private String eventId;

    private String assetId;
}
