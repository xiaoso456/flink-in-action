package io.github.xiaoso456.demo.flink.cdc.mysql.entity;


import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import lombok.Data;

import java.util.Date;

@Data
public class Event {

    @TableId(value = "id", type = IdType.ASSIGN_UUID)
    private String id;

    private String typeId;

    private String name;

    private String type;

    private String description;

    private Date logTime;

}
