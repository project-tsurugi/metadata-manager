CREATE DATABASE tsurugi LC_COLLATE 'C' LC_CTYPE 'C' ENCODING 'UTF8' template=template0;

\c tsurugi

CREATE SCHEMA tsurugi_catalog;

CREATE TABLE tsurugi_catalog.tsurugi_class
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
CREATE INDEX ON tsurugi_catalog.tsurugi_class ( name );

CREATE TABLE tsurugi_catalog.tsurugi_type
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

CREATE TABLE tsurugi_catalog.tsurugi_attribute
(
    format_version integer NOT NULL,
    generation bigint NOT NULL,
    id bigserial NOT NULL,
    name text NOT NULL,
    table_id bigint NOT NULL REFERENCES tsurugi_catalog.tsurugi_class (id) ON DELETE CASCADE,
    column_number bigint NOT NULL,
    data_type_id bigint NOT NULL REFERENCES tsurugi_catalog.tsurugi_type (id),
    data_length json,
    varying bool,
    is_not_null bool NOT NULL,
    default_expression text,
    PRIMARY KEY(id),
    UNIQUE(table_id, name),
    UNIQUE(table_id, column_number)
);
CREATE INDEX ON tsurugi_catalog.tsurugi_attribute ( table_id );

CREATE TABLE tsurugi_catalog.tsurugi_index
(
    format_version integer NOT NULL,
    generation bigint NOT NULL,
    id bigserial NOT NULL,
    name text NOT NULL,
    namespace text,
    owner_id bigint,
    acl text,
    table_id bigint NOT NULL REFERENCES tsurugi_catalog.tsurugi_class (id) ON DELETE CASCADE,
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
CREATE INDEX ON tsurugi_catalog.tsurugi_index ( table_id );

CREATE TABLE tsurugi_catalog.tsurugi_constraint
(
    format_version integer NOT NULL,
    generation bigint NOT NULL,
    id bigserial NOT NULL,
    name text,
    table_id bigint NOT NULL REFERENCES tsurugi_catalog.tsurugi_class (id) ON DELETE CASCADE,
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
CREATE INDEX ON tsurugi_catalog.tsurugi_constraint ( table_id );

CREATE TABLE tsurugi_catalog.tsurugi_statistic
(
    format_version integer NOT NULL,
    generation bigint NOT NULL,
    id bigserial NOT NULL,
    name text,
    column_id bigint NOT NULL REFERENCES tsurugi_catalog.tsurugi_attribute (id) ON DELETE CASCADE,
    column_statistic json,
    PRIMARY KEY (id),
    UNIQUE(column_id)
);

-- INT32
INSERT INTO tsurugi_catalog.tsurugi_type (format_version, generation, id, name, pg_data_type, pg_data_type_name, pg_data_type_qualified_name) values (1, 1, 4, 'INT32', 23,'integer','int4');
-- INT64
INSERT INTO tsurugi_catalog.tsurugi_type (format_version, generation, id, name, pg_data_type, pg_data_type_name, pg_data_type_qualified_name) values (1, 1, 6, 'INT64', 20,'bigint','int8');
-- FLOAT32
INSERT INTO tsurugi_catalog.tsurugi_type (format_version, generation, id, name, pg_data_type, pg_data_type_name, pg_data_type_qualified_name) values (1, 1, 8, 'FLOAT32', 700,'real','float4');
-- FLOAT64
INSERT INTO tsurugi_catalog.tsurugi_type (format_version, generation, id, name, pg_data_type, pg_data_type_name, pg_data_type_qualified_name) values (1, 1, 9, 'FLOAT64', 701,'double precision','float8');
-- CHAR : character, char
INSERT INTO tsurugi_catalog.tsurugi_type (format_version, generation, id, name, pg_data_type, pg_data_type_name, pg_data_type_qualified_name) values (1, 1, 13, 'CHAR', 1042,'char','bpchar');
-- VARCHAR : character varying, varchar
INSERT INTO tsurugi_catalog.tsurugi_type (format_version, generation, id, name, pg_data_type, pg_data_type_name, pg_data_type_qualified_name) values (1, 1, 14, 'VARCHAR', 1043,'varchar','varchar');

GRANT ALL ON ALL TABLES IN SCHEMA tsurugi_catalog To current_user;
