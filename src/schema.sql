create table shard_meta
(
    shard_id int unsigned not null,
    revision int unsigned not null default 0,
    is_migrated bool not null default false,
    primary key (shard_id)
);

create table shard_value_map
(
    entry_id int unsigned not null auto_increment,
    shard_id int unsigned not null,
    name text not null,
    value text not null,
    primary key (entry_id),
    unique (shard_id, name)
);
