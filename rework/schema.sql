create schema if not exists "{ns}";

create table {ns}.monitor (
  id serial primary key,
  domain text not null,
  options jsonb,
  lastseen timestamptz default now()
);

create index ix_{ns}_monitor_domain on {ns}.monitor (domain);
create index ix_{ns}_monitor_lastseen on {ns}.monitor (lastseen);


create table {ns}.worker (
  id serial primary key,
  host text not null,
  domain text not null default 'default',
  pid int,
  mem int default 0,
  cpu int default 0,
  debugport int,
  running bool not null default false,
  shutdown bool not null default false,
  kill bool not null default false,
  traceback text,
  deathinfo text,
  created timestamptz default now(),
  started timestamptz,
  finished timestamptz
);

create index ix_{ns}_worker_running on {ns}.worker (running);


create table {ns}.operation (
  id serial primary key,
  host text not null,
  name text not null,
  path text,
  domain text not null default 'default',
  timeout text,
  inputs jsonb,
  outputs jsonb,
  constraint unique_operation unique (host, name, path)
);

create index ix_{ns}_operation_host on {ns}.operation (host);
create index ix_{ns}_operation_name on {ns}.operation (name);


create type {ns}.status as enum ('queued', 'running', 'done');

create table {ns}.task (
  id serial primary key,
  operation int not null,
  queued timestamptz default now(),
  started timestamptz,
  finished timestamptz,
  input bytea,
  output bytea,
  traceback text,
  worker int,
  status {ns}.status,
  abort bool not null default false,
  metadata jsonb,
  constraint task_operation_fkey foreign key (operation)
    references {ns}.operation (id)
    on delete cascade,
  constraint task_worker_fkey foreign key (worker)
    references {ns}.worker (id)
    on delete cascade
);

create index ix_{ns}_task_operation on {ns}.task (operation);
create index ix_{ns}_task_status on {ns}.task (status);
create index ix_{ns}_task_worker on {ns}.task (worker);


create table {ns}.log (
  id serial primary key,
  task integer,
  tstamp integer not null,
  line text not null,
  constraint log_task_fkey foreign key (task)
    references rework.task (id)
    on delete cascade
);

create index ix_{ns}_rework_log_task on rework.log (task);


create table {ns}.sched (
  id serial primary key,
  operation int not null,
  domain text not null,
  inputdata bytea,
  host text,
  metadata jsonb,
  rule text not null,
  constraint sched_operation_fkey foreign key (operation)
    references {ns}.operation (id)
    on delete cascade
);

create index ix_{ns}_sched_operation on {ns}.sched (operation);
