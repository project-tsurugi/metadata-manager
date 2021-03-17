CREATE DATABASE tsurugi LC_COLLATE 'C' LC_CTYPE 'C' ENCODING 'UTF8' template=template0;

\c tsurugi

CREATE SCHEMA tsurugi_catalog;

CREATE TABLE tsurugi_catalog.tsurugi_class
(
    id bigserial NOT NULL,
    name text NOT NULL,
    namespace text,
    primaryKey json,
    reltuples float4,
    PRIMARY KEY(id),
    UNIQUE(name)
);

CREATE TABLE tsurugi_catalog.tsurugi_type
(
    id bigint NOT NULL,
    name text NOT NULL,
    pg_dataType bigint NOT NULL,
    pg_dataTypeName text NOT NULL,
    pg_dataTypeQualifiedName text NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE tsurugi_catalog.tsurugi_attribute
(
    id bigserial NOT NULL,
    tableId bigint NOT NULL REFERENCES tsurugi_catalog.tsurugi_class (id) ON DELETE CASCADE,
    name text NOT NULL,
    ordinalPosition bigint NOT NULL,
    dataTypeId bigint NOT NULL REFERENCES tsurugi_catalog.tsurugi_type (id),
    dataLength json,
    varying bool,
    nullable bool NOT NULL,
    defaultExpr text,
    direction bigint,
    PRIMARY KEY(id, tableId),
    UNIQUE(tableId, name),
    UNIQUE(tableId, ordinalPosition)
);

CREATE TABLE tsurugi_catalog.tsurugi_statistic
(
    tableId bigint,
    ordinalPosition bigint,
    columnStatistic json,
    PRIMARY KEY (tableId, ordinalPosition),
    FOREIGN KEY (tableId, ordinalPosition) REFERENCES tsurugi_catalog.tsurugi_attribute (tableId, ordinalPosition) MATCH FULL ON DELETE CASCADE
);

-- INT32
INSERT INTO tsurugi_catalog.tsurugi_type (id, name, pg_dataType, pg_dataTypeName, pg_dataTypeQualifiedName) values (4, 'INT32', 23,'integer','int4');
-- INT64
INSERT INTO tsurugi_catalog.tsurugi_type (id, name, pg_dataType, pg_dataTypeName, pg_dataTypeQualifiedName) values (6, 'INT64', 20,'bigint','int8');
-- FLOAT32
INSERT INTO tsurugi_catalog.tsurugi_type (id, name, pg_dataType, pg_dataTypeName, pg_dataTypeQualifiedName) values (8, 'FLOAT32', 700,'real','float4');
-- FLOAT64
INSERT INTO tsurugi_catalog.tsurugi_type (id, name, pg_dataType, pg_dataTypeName, pg_dataTypeQualifiedName) values (9, 'FLOAT64', 701,'double precision','float8');
-- CHAR : character, char
INSERT INTO tsurugi_catalog.tsurugi_type (id, name, pg_dataType, pg_dataTypeName, pg_dataTypeQualifiedName) values (13, 'CHAR', 1042,'char','bpchar');
-- VARCHAR : character varying, varchar
INSERT INTO tsurugi_catalog.tsurugi_type (id, name, pg_dataType, pg_dataTypeName, pg_dataTypeQualifiedName) values (14, 'VARCHAR', 1043,'varchar','varchar');

GRANT ALL ON ALL TABLES IN SCHEMA tsurugi_catalog To current_user;
