CREATE TABLE pt_metropolis_ts (last_allocated int8 NOT NULL);

CREATE TABLE pt_metropolis_table_meta (
    table_name varchar(2000) NOT NULL,
    table_size BIGINT NOT NULL,
    value bytea NULL,
    gc_ts int8 DEFAULT -1,
    CONSTRAINT pk_pt_metropolis_table_meta PRIMARY KEY (table_name)
);

CREATE SEQUENCE pt_metropolis_overflow_seq START WITH 1000;;

CREATE TABLE pt_metropolis_overflow (
  id  BIGINT NOT NULL,
  val bytea NOT NULL,
  CONSTRAINT pk_pt_metropolis_overflow PRIMARY KEY (id)
);