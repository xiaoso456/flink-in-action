-- 创建entity下表结构
-- 请在数据库中执行此脚本
CREATE TABLE event (
                       id VARCHAR(36) PRIMARY KEY,
                       type_id VARCHAR(255),
                       name VARCHAR(255),
                       type VARCHAR(255),
                       description VARCHAR(255),
                       log_time datetime
);

CREATE TABLE asset (
                       id VARCHAR(36) PRIMARY KEY,
                       name VARCHAR(255),
                       is_subject VARCHAR(255),
                       ip VARCHAR(255)
);

CREATE TABLE event_asset_link (
                      event_id VARCHAR(36),
                      asset_id VARCHAR(36),
                      PRIMARY KEY (event_id, asset_id)
);


CREATE TABLE event_asset_readonly (
      event_id VARCHAR(36) ,
      event_type_id VARCHAR(255),
      event_name VARCHAR(255),
      event_type VARCHAR(255),
      event_description VARCHAR(255),
      event_log_time datetime,
      asset_id VARCHAR(36),
      asset_name VARCHAR(255),
      asset_is_subject VARCHAR(255),
      asset_ip VARCHAR(255),
      CONSTRAINT event_asset_readonly_PK PRIMARY KEY (event_id,asset_id)
);