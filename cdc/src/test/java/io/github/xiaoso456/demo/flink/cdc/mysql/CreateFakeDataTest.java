package io.github.xiaoso456.demo.flink.cdc.mysql;

import cn.hutool.core.lang.UUID;
import com.github.javafaker.Faker;
import com.github.javafaker.Name;
import io.github.xiaoso456.demo.flink.cdc.mysql.App;
import io.github.xiaoso456.demo.flink.cdc.mysql.entity.Asset;
import io.github.xiaoso456.demo.flink.cdc.mysql.entity.Event;
import io.github.xiaoso456.demo.flink.cdc.mysql.entity.EventAssetLink;
import io.github.xiaoso456.demo.flink.cdc.mysql.mapper.AssetMapper;
import io.github.xiaoso456.demo.flink.cdc.mysql.mapper.EventAssetLinkMapper;
import io.github.xiaoso456.demo.flink.cdc.mysql.mapper.EventMapper;
import org.apache.ibatis.executor.BatchResult;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@SpringBootTest
public class CreateFakeDataTest {

    @Autowired
    private EventMapper eventMapper;

    @Autowired
    private AssetMapper assetMapper;

    @Autowired
    private EventAssetLinkMapper eventAssetLinkMapper;

    @Test
    public void testCreateFakeData() throws InterruptedException {

        Faker faker = new Faker(Locale.CHINA);
        // 使用线程池并发生成数据

        ExecutorService executor = Executors.newFixedThreadPool(16);

        for(int time = 0; time < 50000; time++){
            int finalTime = time;
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    Event event = new Event();
                    event.setId(UUID.fastUUID().toString());
                    event.setName(faker.name().username());
                    event.setDescription(faker.name().title());
                    event.setType(faker.name().lastName());
                    event.setLogTime(faker.date().past(100, TimeUnit.DAYS));


                    List<Asset> assets = new ArrayList<>(16);
                    List<EventAssetLink> eventAssetLinks = new ArrayList<>(16);
                    for(int i = 0; i < 10; i++){
                        Asset asset = new Asset();
                        asset.setId(UUID.fastUUID().toString());
                        asset.setName(faker.app().name());
                        asset.setIp(faker.internet().ipV4Address());
                        asset.setIsSubject(String.valueOf(faker.bool().bool()));
                        assets.add(asset);
                        eventAssetLinks.add(new EventAssetLink(event.getId(), asset.getId()));
                    }
                    eventMapper.insert(event);
                    assetMapper.insert(assets);
                    eventAssetLinkMapper.insert(eventAssetLinks);
                    if(finalTime % 500 == 0){
                        System.out.println(finalTime);
                    }
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);

    }

}
