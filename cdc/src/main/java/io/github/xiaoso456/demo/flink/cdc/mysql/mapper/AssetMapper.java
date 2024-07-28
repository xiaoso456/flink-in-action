package io.github.xiaoso456.demo.flink.cdc.mysql.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import io.github.xiaoso456.demo.flink.cdc.mysql.entity.Asset;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface AssetMapper extends BaseMapper<Asset> {
}
