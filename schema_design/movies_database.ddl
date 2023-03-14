CREATE SCHEMA IF NOT EXISTS content;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS content.film_work (
    id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    creation_date DATE,
    rating FLOAT,
    film_type TEXT NOT NULL,
    created TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    modified TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS content.genre (
    id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    modified TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS content.person (
    id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    full_name VARCHAR(255) NOT NULL,
    created TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    modified TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS content.genre_film_work (
    id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    film_work_id uuid NOT NULL,
    genre_id uuid NOT NULL,
    created TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (film_work_id, genre_id)
);

CREATE TABLE IF NOT EXISTS content.person_film_work (
    id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    film_work_id uuid NOT NULL,
    person_id uuid NOT NULL,
    role TEXT NOT NULL,
    created TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (film_work_id, person_id, role)
);

ALTER TABLE content.genre_film_work ADD CONSTRAINT fk_genre
FOREIGN KEY (genre_id)
REFERENCES content.genre(id) ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE content.genre_film_work ADD CONSTRAINT fk_film_work
FOREIGN KEY (film_work_id)
REFERENCES content.film_work(id) ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE content.person_film_work ADD CONSTRAINT fk_person
FOREIGN KEY (person_id)
REFERENCES content.person(id) ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE content.person_film_work ADD CONSTRAINT fk_film_work
FOREIGN KEY (film_work_id)
REFERENCES content.film_work(id) ON UPDATE CASCADE ON DELETE CASCADE;

ALTER ROLE app SET search_path TO content,public;
