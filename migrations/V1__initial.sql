DROP TABLE IF EXISTS entry_agent;
DROP TABLE IF EXISTS entry_language;
DROP TABLE IF EXISTS datasource;
DROP TABLE IF EXISTS entry;
DROP TABLE IF EXISTS site;
DROP TABLE IF EXISTS library;
DROP TABLE IF EXISTS known_language;
DROP TABLE IF EXISTS agent;

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
 -- CALL insert_amw_site('Elephant', 'https://www.elephanteditions.net/oai-pmh');
 -- CALL insert_amw_site('Anarchismo', 'https://www.edizionianarchismo.net/oai-pmh');
 -- CALL insert_amw_site('Fifth Estate', 'https://fifthestate.anarchistlibraries.net/oai-pmh');
 -- CALL insert_amw_site('NightFall', 'https://nightfall.buzz/oai-pmh');
DROP PROCEDURE insert_amw_site;

CREATE TABLE entry (
    entry_id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    subtitle VARCHAR(255) NOT NULL,
    checksum VARCHAR(255) NOT NULL,
    search_text TEXT,
    original_entry_id INTEGER REFERENCES entry(entry_id) ON UPDATE CASCADE ON DELETE SET NULL,
    canonical_entry_id INTEGER REFERENCES entry(entry_id) ON UPDATE CASCADE ON DELETE SET NULL,
    created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_indexed TIMESTAMP WITH TIME ZONE NOT NULL  DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(checksum)
);

CREATE TABLE agent (
    agent_id SERIAL PRIMARY KEY,
    full_name VARCHAR(255) NOT NULL,
    search_text TEXT,
    wikidata_id VARCHAR(255),
    canonical_agent_id INTEGER REFERENCES agent(agent_id) ON UPDATE CASCADE ON DELETE SET NULL,
    created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(full_name)
);

CREATE TABLE entry_agent (
    entry_id INTEGER NOT NULL REFERENCES entry(entry_id) ON UPDATE CASCADE ON DELETE CASCADE,
    agent_id INTEGER NOT NULL REFERENCES agent(agent_id) ON UPDATE CASCADE ON DELETE CASCADE,
    created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (entry_id, agent_id)
);

CREATE TABLE datasource (
    datasource_id SERIAL PRIMARY KEY,
    site_id INTEGER NOT NULL REFERENCES site(site_id) ON UPDATE CASCADE ON DELETE CASCADE,
    oai_pmh_identifier VARCHAR(2048) NOT NULL,
    datestamp TIMESTAMP WITH TIME ZONE,
    entry_id INTEGER NOT NULL REFERENCES entry(entry_id) ON UPDATE CASCADE ON DELETE CASCADE,
    description TEXT,
    year_edition INTEGER,
    year_first_edition INTEGER,
    publisher TEXT,
    isbn TEXT,
    uri VARCHAR(2048),
    uri_label VARCHAR(2048),
    content_type VARCHAR(128),
    material_description TEXT,
    shelf_location_code VARCHAR(255),
    edition_statement TEXT,
    place_date_of_publication_distribution TEXT,
    is_aggregation BOOLEAN NOT NULL DEFAULT FALSE,
    created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(site_id, oai_pmh_identifier)
);

CREATE TABLE known_language (
    language_code VARCHAR(8) NOT NULL PRIMARY KEY,
    native_name VARCHAR(255),
    english_name VARCHAR(255),
    created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE entry_language (
    entry_id INTEGER NOT NULL REFERENCES entry(entry_id) ON UPDATE CASCADE ON DELETE CASCADE,
    language_code VARCHAR(3) NOT NULL REFERENCES known_language(language_code) ON UPDATE CASCADE ON DELETE CASCADE,
    created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(entry_id, language_code)
);
