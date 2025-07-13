DROP TABLE IF EXISTS  site;
DROP TABLE IF EXISTS library;

CREATE TABLE library (
    library_id SERIAL NOT NULL PRIMARY KEY,
    name character varying(255) NOT NULL,
    url character varying(255),
    public boolean NOT NULL DEFAULT TRUE,
    active boolean NOT NULL DEFAULT TRUE,
    enable_check boolean NOT NULL DEFAULT FALSE,
    check_token character varying(255),
    last_check timestamp with time zone,
    email_public character varying(254),
    email_internal character varying(254),
    opening_hours text,
    latitude numeric(10,7),
    longitude numeric(10,7),
    created timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_modified timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    description text,
    languages text,
    logo_url character varying(255),
    year_established date,
    address_city character varying(64),
    address_country character varying(64),
    address_line_1 character varying(255),
    address_line_2 character varying(255),
    address_state character varying(64),
    address_zip character varying(16),
    pgp_public_key text,
    short_desc text,
    library_type character varying(32)
);

CREATE TABLE site (
    site_id SERIAL PRIMARY KEY,
    title character varying(255) NOT NULL,
    url character varying(255),
    last_harvested timestamp with time zone,
    comment text,
    oai_set character varying(64),
    oai_metadata_format character varying(32),
    site_type character varying(32) NOT NULL,
    csv_type character varying(32),
    active boolean NOT NULL DEFAULT TRUE,
    amusewiki_formats jsonb,
    tree_path character varying(255),
    created timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_modified timestamp with time zone NOT NULL  DEFAULT CURRENT_TIMESTAMP,
    library_id int NOT NULL REFERENCES library(library_id)
);

CREATE OR REPLACE PROCEDURE insert_amw_site(library_name varchar(255), site_url varchar(255))
LANGUAGE SQL
AS $$
WITH inserted_library AS (
     INSERT INTO library (name) VALUES (library_name)
     RETURNING library_id
)
INSERT INTO site (title, url, library_id, site_type, oai_metadata_format, oai_set)
SELECT library_name, site_url, library_id, 'amusewiki', 'marc21', 'web' FROM inserted_library
RETURNING site_id
$$;

CALL insert_amw_site('Amusewiki', 'https://amusewiki.org/oai-pmh');
CALL insert_amw_site('Mycorrhiza', 'https://mycorrhiza.amusewiki.org/oai-pmh');
CALL insert_amw_site('Amusewiki Staging', 'https://staging.amusewiki.org/oai-pmh');
DROP PROCEDURE insert_amw_site;

