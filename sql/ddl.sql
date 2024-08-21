CREATE DATABASE tsurugi LC_COLLATE 'C' LC_CTYPE 'C' ENCODING 'UTF8' template=template0;

\c tsurugi

CREATE SCHEMA tg_catalog;

CREATE TABLE tg_catalog.tables
(
    format_version integer NOT NULL,
    generation bigint NOT NULL,
    id bigserial NOT NULL,
    name text NOT NULL,
    namespace text,
    number_of_tuples bigint,
    PRIMARY KEY(id),
    UNIQUE(namespace, name)
);
CREATE INDEX ON tg_catalog.tables ( name );

CREATE TABLE tg_catalog.types
(
    format_version integer NOT NULL,
    generation bigint NOT NULL,
    id bigint NOT NULL,
    name text NOT NULL,
    pg_data_type bigint NOT NULL,
    pg_data_type_name text NOT NULL,
    pg_data_type_qualified_name text NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE tg_catalog.columns
(
    format_version integer NOT NULL,
    generation bigint NOT NULL,
    id bigserial NOT NULL,
    name text NOT NULL,
    table_id bigint NOT NULL REFERENCES tg_catalog.tables (id) ON DELETE CASCADE,
    column_number bigint NOT NULL,
    data_type_id bigint NOT NULL REFERENCES tg_catalog.types (id),
    data_length json,
    varying bool,
    is_not_null bool NOT NULL,
    default_expression text,
    is_funcexpr bool,
    PRIMARY KEY(id),
    UNIQUE(table_id, name),
    UNIQUE(table_id, column_number)
);
CREATE INDEX ON tg_catalog.columns ( table_id );

CREATE TABLE tg_catalog.indexes
(
    format_version integer NOT NULL,
    generation bigint NOT NULL,
    id bigserial NOT NULL,
    name text NOT NULL,
    namespace text,
    owner_id bigint,
    acl text,
    table_id bigint NOT NULL REFERENCES tg_catalog.tables (id) ON DELETE CASCADE,
    access_method bigint NOT NULL,
    is_unique bool NOT NULL,
    is_primary bool NOT NULL,
    number_of_key_column bigint,
    columns json,
    columns_id json,
    options json,
    PRIMARY KEY(id),
    UNIQUE(table_id, name)
);
CREATE INDEX ON tg_catalog.indexes ( table_id );

CREATE TABLE tg_catalog.constraints
(
    format_version integer NOT NULL,
    generation bigint NOT NULL,
    id bigserial NOT NULL,
    name text,
    table_id bigint NOT NULL REFERENCES tg_catalog.tables (id) ON DELETE CASCADE,
    type bigint NOT NULL,
    columns json NOT NULL,
    columns_id json NOT NULL,
    index_id bigint,
    expression text,
    pk_table text,
    pk_columns json,
    pk_columns_id json,
    fk_match_type bigint,
    fk_delete_action bigint,
    fk_update_action bigint,
    PRIMARY KEY(id)
);
CREATE INDEX ON tg_catalog.constraints ( table_id );

CREATE TABLE tg_catalog.statistics
(
    format_version integer NOT NULL,
    generation bigint NOT NULL,
    id bigserial NOT NULL,
    name text,
    column_id bigint NOT NULL REFERENCES tg_catalog.columns (id) ON DELETE CASCADE,
    column_statistic json,
    PRIMARY KEY (id),
    UNIQUE(column_id)
);

CREATE SEQUENCE tg_catalog.id_sequence MINVALUE 100001;

-- INT32
INSERT INTO tg_catalog.types (format_version, generation, id, name, pg_data_type, pg_data_type_name, pg_data_type_qualified_name) values (1, 1, 23, 'INT32', 23,'integer','int4');
-- INT64
INSERT INTO tg_catalog.types (format_version, generation, id, name, pg_data_type, pg_data_type_name, pg_data_type_qualified_name) values (1, 1, 20, 'INT64', 20,'bigint','int8');
-- FLOAT32
INSERT INTO tg_catalog.types (format_version, generation, id, name, pg_data_type, pg_data_type_name, pg_data_type_qualified_name) values (1, 1, 700, 'FLOAT32', 700,'real','float4');
-- FLOAT64
INSERT INTO tg_catalog.types (format_version, generation, id, name, pg_data_type, pg_data_type_name, pg_data_type_qualified_name) values (1, 1, 701, 'FLOAT64', 701,'double precision','float8');
-- CHAR : character, char
INSERT INTO tg_catalog.types (format_version, generation, id, name, pg_data_type, pg_data_type_name, pg_data_type_qualified_name) values (1, 1, 1042, 'CHAR', 1042,'char','bpchar');
-- VARCHAR : character varying, varchar
INSERT INTO tg_catalog.types (format_version, generation, id, name, pg_data_type, pg_data_type_name, pg_data_type_qualified_name) values (1, 1, 1043, 'VARCHAR', 1043,'varchar','varchar');
-- NUMERIC : numeric, decimal
INSERT INTO tg_catalog.types (format_version, generation, id, name, pg_data_type, pg_data_type_name, pg_data_type_qualified_name) values (1, 1, 1700, 'NUMERIC', 1700,'numeric','numeric');
-- DATE
INSERT INTO tg_catalog.types (format_version, generation, id, name, pg_data_type, pg_data_type_name, pg_data_type_qualified_name) values (1, 1, 1082, 'DATE', 1082,'date','date');
-- TIME
INSERT INTO tg_catalog.types (format_version, generation, id, name, pg_data_type, pg_data_type_name, pg_data_type_qualified_name) values (1, 1, 1083, 'TIME', 1083,'time','time');
-- TIMETZ
INSERT INTO tg_catalog.types (format_version, generation, id, name, pg_data_type, pg_data_type_name, pg_data_type_qualified_name) values (1, 1, 1266, 'TIMETZ', 1266,'timetz','timetz');
-- TIMESTAMP
INSERT INTO tg_catalog.types (format_version, generation, id, name, pg_data_type, pg_data_type_name, pg_data_type_qualified_name) values (1, 1, 1114, 'TIMESTAMP', 1114,'timestamp','timestamp');
-- TIMESTAMPTZ
INSERT INTO tg_catalog.types (format_version, generation, id, name, pg_data_type, pg_data_type_name, pg_data_type_qualified_name) values (1, 1, 1184, 'TIMESTAMPTZ', 1184,'timestamptz','timestamptz');

GRANT ALL ON ALL TABLES IN SCHEMA tg_catalog To current_user;
