insert into print_sink
select
    e.id as event_id,
    e.type_id as event_type_id,
    e.type as event_type,
    e.name as event_name,
    e.log_time as event_log_time,
    e.description as event_description,
    a.id as asset_id,
    a.name as asset_name,
    a.is_subject as asset_is_subject,
    a.ip as asset_ip
from event e
inner join event_asset_link eal on e.id = eal.event_id
inner join asset a on eal.asset_id = a.id