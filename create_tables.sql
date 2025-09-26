CREATE TABLE IF NOT EXISTS client (
        id SERIAL PRIMARY KEY,
        phone int8 NOT NULL CHECK( phone >= 70000000000 AND phone <= 79999999999) ,
        mob int2 NOT NULL CHECK(mob >= 900 and mob <= 999),
        teg VARCHAR,
        timezone INTEGER NOT NULL CHECK(timezone >= -11 and timezone <= 11),
        UNIQUE ( phone, teg )
);

CREATE TABLE IF NOT EXISTS distribution  (
        id SERIAL PRIMARY KEY,
        start_date TIMESTAMP NOT NULL,
        text VARCHAR,
        mob int2 CHECK (mob >= 900 AND mob <= 999),
        teg VARCHAR,
        end_date TIMESTAMP NOT NULL,
        interval INTERVAL NOT NULL GENERATED ALWAYS AS (end_date -  start_date) STORED
);

        CREATE TABLE IF NOT EXISTS message (
        id serial PRIMARY KEY,
        start_date TIMESTAMP ,
        status VARCHAR ,
        id_distribution INTEGER REFERENCES distribution (Id) ON DELETE CASCADE,
        id_client INTEGER REFERENCES client (id) ON DELETE CASCADE

);
CREATE TABLE IF NOT EXISTS products (
        id serial PRIMARY KEY,
        model VARCHAR NOT NULL,
        company VARCHAR NOT NULL,
        price INTEGER NOT NULL
);
