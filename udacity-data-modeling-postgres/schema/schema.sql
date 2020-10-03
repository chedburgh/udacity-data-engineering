--
-- PostgreSQL database dump
--

-- Dumped from database version 12.3 (Debian 12.3-1.pgdg100+1)
-- Dumped by pg_dump version 12.2 (Ubuntu 12.2-4)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: artists; Type: TABLE; Schema: public; Owner: student
--

CREATE TABLE public.artists (
    artist_id text NOT NULL,
    name text NOT NULL,
    location text,
    latitude double precision,
    longitude double precision
);


ALTER TABLE public.artists OWNER TO student;

--
-- Name: songplays; Type: TABLE; Schema: public; Owner: student
--

CREATE TABLE public.songplays (
    songplay_id integer NOT NULL,
    user_id integer NOT NULL,
    song_id text,
    artist_id text,
    start_time bigint NOT NULL,
    session_id integer NOT NULL,
    level text NOT NULL,
    location text NOT NULL,
    user_agent text NOT NULL
);


ALTER TABLE public.songplays OWNER TO student;

--
-- Name: songplays_songplay_id_seq; Type: SEQUENCE; Schema: public; Owner: student
--

CREATE SEQUENCE public.songplays_songplay_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.songplays_songplay_id_seq OWNER TO student;

--
-- Name: songplays_songplay_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: student
--

ALTER SEQUENCE public.songplays_songplay_id_seq OWNED BY public.songplays.songplay_id;


--
-- Name: songs; Type: TABLE; Schema: public; Owner: student
--

CREATE TABLE public.songs (
    song_id text NOT NULL,
    artist_id text NOT NULL,
    title text NOT NULL,
    year integer,
    duration double precision
);


ALTER TABLE public.songs OWNER TO student;

--
-- Name: time; Type: TABLE; Schema: public; Owner: student
--

CREATE TABLE public."time" (
    start_time bigint NOT NULL,
    hour integer NOT NULL,
    day integer NOT NULL,
    week integer NOT NULL,
    month integer NOT NULL,
    year integer NOT NULL,
    weekday integer NOT NULL
);


ALTER TABLE public."time" OWNER TO student;

--
-- Name: users; Type: TABLE; Schema: public; Owner: student
--

CREATE TABLE public.users (
    user_id integer NOT NULL,
    first_name text,
    last_name text,
    gender text,
    level text
);


ALTER TABLE public.users OWNER TO student;

--
-- Name: songplays songplay_id; Type: DEFAULT; Schema: public; Owner: student
--

ALTER TABLE ONLY public.songplays ALTER COLUMN songplay_id SET DEFAULT nextval('public.songplays_songplay_id_seq'::regclass);


--
-- Name: artists artists_pkey; Type: CONSTRAINT; Schema: public; Owner: student
--

ALTER TABLE ONLY public.artists
    ADD CONSTRAINT artists_pkey PRIMARY KEY (artist_id);


--
-- Name: songplays songplays_pkey; Type: CONSTRAINT; Schema: public; Owner: student
--

ALTER TABLE ONLY public.songplays
    ADD CONSTRAINT songplays_pkey PRIMARY KEY (songplay_id);


--
-- Name: songs songs_pkey; Type: CONSTRAINT; Schema: public; Owner: student
--

ALTER TABLE ONLY public.songs
    ADD CONSTRAINT songs_pkey PRIMARY KEY (song_id);


--
-- Name: time time_pkey; Type: CONSTRAINT; Schema: public; Owner: student
--

ALTER TABLE ONLY public."time"
    ADD CONSTRAINT time_pkey PRIMARY KEY (start_time);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: student
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (user_id);


--
-- PostgreSQL database dump complete
--

