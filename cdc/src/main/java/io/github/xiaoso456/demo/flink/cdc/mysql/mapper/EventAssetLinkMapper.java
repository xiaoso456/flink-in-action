package io.github.xiaoso456.demo.flink.cdc.mysql.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import io.github.xiaoso456.demo.flink.cdc.mysql.entity.Event;
import io.github.xiaoso456.demo.flink.cdc.mysql.entity.EventAssetLink;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface EventAssetLinkMapper extends BaseMapper<EventAssetLink> {
}
