--
-- PostgreSQL database dump
--

\restrict Qbahcha1rG0zRBeOu5VkRb4k6An0JtSEqRHNLaiUHhwJZn4dsKytv5CIHk9tJpn

-- Dumped from database version 17.9
-- Dumped by pg_dump version 17.8 (Ubuntu 17.8-1.pgdg24.04+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: public; Type: SCHEMA; Schema: -; Owner: cloudsqlsuperuser
--

CREATE SCHEMA public;


ALTER SCHEMA public OWNER TO cloudsqlsuperuser;

--
-- Name: SCHEMA public; Type: COMMENT; Schema: -; Owner: cloudsqlsuperuser
--

COMMENT ON SCHEMA public IS 'standard public schema';


--
-- Name: achievement_cadence; Type: TYPE; Schema: public; Owner: f3slackbot
--

CREATE TYPE public.achievement_cadence AS ENUM (
    'weekly',
    'monthly',
    'quarterly',
    'yearly',
    'lifetime'
);


ALTER TYPE public.achievement_cadence OWNER TO f3slackbot;

--
-- Name: achievement_threshold_type; Type: TYPE; Schema: public; Owner: f3slackbot
--

CREATE TYPE public.achievement_threshold_type AS ENUM (
    'posts',
    'unique_aos'
);


ALTER TYPE public.achievement_threshold_type OWNER TO f3slackbot;

--
-- Name: day_of_week; Type: TYPE; Schema: public; Owner: f3slackbot
--

CREATE TYPE public.day_of_week AS ENUM (
    'monday',
    'tuesday',
    'wednesday',
    'thursday',
    'friday',
    'saturday',
    'sunday'
);


ALTER TYPE public.day_of_week OWNER TO f3slackbot;

--
-- Name: event_cadence; Type: TYPE; Schema: public; Owner: f3slackbot
--

CREATE TYPE public.event_cadence AS ENUM (
    'weekly',
    'monthly'
);


ALTER TYPE public.event_cadence OWNER TO f3slackbot;

--
-- Name: event_category; Type: TYPE; Schema: public; Owner: f3slackbot
--

CREATE TYPE public.event_category AS ENUM (
    'first_f',
    'second_f',
    'third_f'
);


ALTER TYPE public.event_category OWNER TO f3slackbot;

--
-- Name: org_type; Type: TYPE; Schema: public; Owner: f3slackbot
--

CREATE TYPE public.org_type AS ENUM (
    'ao',
    'region',
    'area',
    'sector',
    'nation'
);


ALTER TYPE public.org_type OWNER TO f3slackbot;

--
-- Name: region_role; Type: TYPE; Schema: public; Owner: f3slackbot
--

CREATE TYPE public.region_role AS ENUM (
    'user',
    'editor',
    'admin'
);


ALTER TYPE public.region_role OWNER TO f3slackbot;

--
-- Name: request_type; Type: TYPE; Schema: public; Owner: f3slackbot
--

CREATE TYPE public.request_type AS ENUM (
    'create_location',
    'create_event',
    'edit',
    'delete_event'
);


ALTER TYPE public.request_type OWNER TO f3slackbot;

--
-- Name: series_exception; Type: TYPE; Schema: public; Owner: f3slackbot
--

CREATE TYPE public.series_exception AS ENUM (
    'closed',
    'different-time',
    'miscellaneous'
);


ALTER TYPE public.series_exception OWNER TO f3slackbot;

--
-- Name: update_request_status; Type: TYPE; Schema: public; Owner: f3slackbot
--

CREATE TYPE public.update_request_status AS ENUM (
    'pending',
    'approved',
    'rejected'
);


ALTER TYPE public.update_request_status OWNER TO f3slackbot;

--
-- Name: user_role; Type: TYPE; Schema: public; Owner: f3slackbot
--

CREATE TYPE public.user_role AS ENUM (
    'user',
    'editor',
    'admin'
);


ALTER TYPE public.user_role OWNER TO f3slackbot;

--
-- Name: user_status; Type: TYPE; Schema: public; Owner: f3slackbot
--

CREATE TYPE public.user_status AS ENUM (
    'active',
    'inactive'
);


ALTER TYPE public.user_status OWNER TO f3slackbot;

--
-- Name: set_updated_column(); Type: FUNCTION; Schema: public; Owner: f3slackbot
--

CREATE FUNCTION public.set_updated_column() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.updated = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$;


ALTER FUNCTION public.set_updated_column() OWNER TO f3slackbot;

--
-- Name: toggle_ao_count_trigger(boolean); Type: FUNCTION; Schema: public; Owner: f3slackbot
--

CREATE FUNCTION public.toggle_ao_count_trigger(disable boolean) RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
  PERFORM set_config('app.disable_ao_count_trigger', disable::TEXT, FALSE);
END;
$$;


ALTER FUNCTION public.toggle_ao_count_trigger(disable boolean) OWNER TO f3slackbot;

--
-- Name: update_org_ao_counts(); Type: FUNCTION; Schema: public; Owner: f3slackbot
--

CREATE FUNCTION public.update_org_ao_counts() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  parent_id_var INTEGER;
  parent_type_var TEXT;
  grandparent_id_var INTEGER;
  grandparent_type_var TEXT;
  great_grandparent_id_var INTEGER;
  great_grandparent_type_var TEXT;
  -- Add flag to check if trigger is enabled
  is_disabled BOOLEAN;
BEGIN
  -- Check if trigger is disabled via the app_config table
  SELECT current_setting('app.disable_ao_count_trigger', TRUE)::BOOLEAN INTO is_disabled;
  IF is_disabled THEN
    RETURN NEW;
  END IF;

  -- Only run calculations when an AO is created, deleted, or its active status changes
  IF (TG_OP = 'INSERT' OR TG_OP = 'UPDATE') AND 
     ((TG_OP = 'INSERT' AND NEW.org_type = 'ao') OR 
      (TG_OP = 'UPDATE' AND (NEW.org_type = 'ao' OR OLD.org_type = 'ao'))) THEN
    
    -- Get the parent org info (typically a region)
    SELECT id, org_type INTO parent_id_var, parent_type_var
    FROM orgs
    WHERE id = COALESCE(NEW.parent_id, OLD.parent_id);
    
    -- Update parent org count (direct children that are AOs)
    IF parent_id_var IS NOT NULL THEN
      UPDATE orgs
      SET ao_count = (
        SELECT COUNT(*)
        FROM orgs
        WHERE parent_id = parent_id_var
          AND org_type = 'ao'
          AND is_active = true
      )
      WHERE id = parent_id_var;
      
      -- Get the grandparent org info (typically an area)
      SELECT id, org_type INTO grandparent_id_var, grandparent_type_var
      FROM orgs
      WHERE id = (
        SELECT parent_id
        FROM orgs
        WHERE id = parent_id_var
      );
      
      -- Update grandparent org count (grandchildren that are AOs)
      IF grandparent_id_var IS NOT NULL THEN
        UPDATE orgs
        SET ao_count = (
          SELECT COUNT(*)
          FROM orgs ao
          JOIN orgs region ON ao.parent_id = region.id
          WHERE region.parent_id = grandparent_id_var
            AND ao.org_type = 'ao'
            AND region.org_type = 'region'
            AND ao.is_active = true
            AND region.is_active = true
        )
        WHERE id = grandparent_id_var;
        
        -- Get the great-grandparent org info (typically a sector)
        SELECT id, org_type INTO great_grandparent_id_var, great_grandparent_type_var
        FROM orgs
        WHERE id = (
          SELECT parent_id
          FROM orgs
          WHERE id = grandparent_id_var
        );
        
        -- Update great-grandparent org count (great-grandchildren that are AOs)
        IF great_grandparent_id_var IS NOT NULL THEN
          UPDATE orgs
          SET ao_count = (
            SELECT COUNT(*)
            FROM orgs ao
            JOIN orgs region ON ao.parent_id = region.id
            JOIN orgs area ON region.parent_id = area.id
            WHERE area.parent_id = great_grandparent_id_var
              AND ao.org_type = 'ao'
              AND region.org_type = 'region'
              AND area.org_type = 'area'
              AND ao.is_active = true
              AND region.is_active = true
              AND area.is_active = true
          )
          WHERE id = great_grandparent_id_var;
        END IF;
      END IF;
    END IF;
  END IF;
  
  RETURN NEW;
END;
$$;


ALTER FUNCTION public.update_org_ao_counts() OWNER TO f3slackbot;

--
-- Name: update_updated_at_column(); Type: FUNCTION; Schema: public; Owner: app_codex
--

CREATE FUNCTION public.update_updated_at_column() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    BEGIN
      NEW.updated_at = now();
      RETURN NEW;
    END;
    $$;


ALTER FUNCTION public.update_updated_at_column() OWNER TO app_codex;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: achievements; Type: TABLE; Schema: public; Owner: f3slackbot
--

CREATE TABLE public.achievements (
    id integer NOT NULL,
    name character varying NOT NULL,
    description character varying,
    image_url character varying,
    created timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    updated timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    specific_org_id integer,
    auto_award boolean DEFAULT false NOT NULL,
    is_active boolean DEFAULT true NOT NULL,
    auto_cadence public.achievement_cadence,
    auto_threshold integer,
    meta json,
    auto_threshold_type character varying,
    auto_filters json
);


ALTER TABLE public.achievements OWNER TO f3slackbot;

--
-- Name: achievements_id_seq; Type: SEQUENCE; Schema: public; Owner: f3slackbot
--

CREATE SEQUENCE public.achievements_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.achievements_id_seq OWNER TO f3slackbot;

--
-- Name: achievements_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: f3slackbot
--

ALTER SEQUENCE public.achievements_id_seq OWNED BY public.achievements.id;


--
-- Name: achievements_x_users; Type: TABLE; Schema: public; Owner: f3slackbot
--

CREATE TABLE public.achievements_x_users (
    achievement_id integer NOT NULL,
    user_id integer NOT NULL,
    date_awarded timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    award_year integer DEFAULT '-1'::integer NOT NULL,
    award_period integer DEFAULT '-1'::integer NOT NULL
);


ALTER TABLE public.achievements_x_users OWNER TO f3slackbot;

--
-- Name: alembic_version; Type: TABLE; Schema: public; Owner: f3slackbot
--

CREATE TABLE public.alembic_version (
    version_num character varying(32) NOT NULL
);


ALTER TABLE public.alembic_version OWNER TO f3slackbot;

--
-- Name: api_keys; Type: TABLE; Schema: public; Owner: f3slackbot
--

CREATE TABLE public.api_keys (
    id integer NOT NULL,
    key character varying NOT NULL,
    name character varying NOT NULL,
    description character varying,
    owner_id integer,
    revoked_at timestamp without time zone,
    last_used_at timestamp without time zone,
    expires_at timestamp without time zone,
    created timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    updated timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL
);


ALTER TABLE public.api_keys OWNER TO f3slackbot;

--
-- Name: api_keys_id_seq; Type: SEQUENCE; Schema: public; Owner: f3slackbot
--

CREATE SEQUENCE public.api_keys_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.api_keys_id_seq OWNER TO f3slackbot;

--
-- Name: api_keys_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: f3slackbot
--

ALTER SEQUENCE public.api_keys_id_seq OWNED BY public.api_keys.id;


--
-- Name: attendance; Type: TABLE; Schema: public; Owner: f3slackbot
--

CREATE TABLE public.attendance (
    id integer NOT NULL,
    user_id integer NOT NULL,
    is_planned boolean NOT NULL,
    meta json,
    created timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    updated timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    event_instance_id integer NOT NULL
);


ALTER TABLE public.attendance OWNER TO f3slackbot;

--
-- Name: attendance_types; Type: TABLE; Schema: public; Owner: f3slackbot
--

CREATE TABLE public.attendance_types (
    id integer NOT NULL,
    type character varying NOT NULL,
    description character varying,
    created timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    updated timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL
);


ALTER TABLE public.attendance_types OWNER TO f3slackbot;

--
-- Name: attendance_x_attendance_types; Type: TABLE; Schema: public; Owner: f3slackbot
--

CREATE TABLE public.attendance_x_attendance_types (
    attendance_id integer NOT NULL,
    attendance_type_id integer NOT NULL
);


ALTER TABLE public.attendance_x_attendance_types OWNER TO f3slackbot;

--
-- Name: event_instances; Type: TABLE; Schema: public; Owner: f3slackbot
--

CREATE TABLE public.event_instances (
    id integer NOT NULL,
    org_id integer NOT NULL,
    location_id integer,
    series_id integer,
    is_active boolean NOT NULL,
    highlight boolean NOT NULL,
    start_date date NOT NULL,
    end_date date,
    start_time character varying,
    end_time character varying,
    name character varying NOT NULL,
    description character varying,
    email character varying,
    pax_count integer,
    fng_count integer,
    preblast character varying,
    backblast character varying,
    preblast_rich json,
    backblast_rich json,
    preblast_ts double precision,
    backblast_ts double precision,
    meta json,
    created timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    updated timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    is_private boolean DEFAULT false NOT NULL,
    series_exception public.series_exception
);


ALTER TABLE public.event_instances OWNER TO f3slackbot;

--
-- Name: orgs; Type: TABLE; Schema: public; Owner: f3slackbot
--

CREATE TABLE public.orgs (
    id integer NOT NULL,
    parent_id integer,
    default_location_id integer,
    name character varying NOT NULL,
    description character varying,
    is_active boolean NOT NULL,
    logo_url character varying,
    website character varying,
    email character varying,
    twitter character varying,
    facebook character varying,
    instagram character varying,
    last_annual_review date,
    meta json,
    created timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    updated timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    org_type public.org_type NOT NULL,
    ao_count integer DEFAULT 0
);


ALTER TABLE public.orgs OWNER TO f3slackbot;

--
-- Name: users; Type: TABLE; Schema: public; Owner: f3slackbot
--

CREATE TABLE public.users (
    id integer NOT NULL,
    f3_name character varying,
    first_name character varying,
    last_name character varying,
    email public.citext NOT NULL,
    phone character varying,
    home_region_id integer,
    avatar_url character varying,
    meta json,
    created timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    updated timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    emergency_contact character varying,
    emergency_phone character varying,
    emergency_notes character varying,
    email_verified timestamp without time zone,
    status public.user_status DEFAULT 'active'::public.user_status NOT NULL
);


ALTER TABLE public.users OWNER TO f3slackbot;

--
-- Name: attendance_expanded; Type: VIEW; Schema: public; Owner: f3slackbot
--

CREATE VIEW public.attendance_expanded AS
 SELECT a.id,
    a.user_id,
    a.event_instance_id,
    a.meta AS attendance_meta,
    a.created,
    a.updated,
    at.q_ind,
    at.coq_ind,
    u.f3_name,
    u.first_name,
    u.last_name,
    u.email,
    u.home_region_id,
    hr.name AS home_region_name,
    u.avatar_url,
    u.status AS user_status,
    ei.start_date
   FROM ((((public.attendance a
     LEFT JOIN ( SELECT x.attendance_id,
            sum(
                CASE
                    WHEN ((t.type)::text = 'Q'::text) THEN 1
                    ELSE 0
                END) AS q_ind,
            sum(
                CASE
                    WHEN ((t.type)::text = 'Co-Q'::text) THEN 1
                    ELSE 0
                END) AS coq_ind
           FROM (public.attendance_x_attendance_types x
             JOIN public.attendance_types t ON ((x.attendance_type_id = t.id)))
          GROUP BY x.attendance_id) at ON ((a.id = at.attendance_id)))
     LEFT JOIN public.users u ON ((a.user_id = u.id)))
     LEFT JOIN public.orgs hr ON ((u.home_region_id = hr.id)))
     LEFT JOIN public.event_instances ei ON ((a.event_instance_id = ei.id)))
  WHERE (a.is_planned = false);


ALTER VIEW public.attendance_expanded OWNER TO f3slackbot;

--
-- Name: attendance_id_seq; Type: SEQUENCE; Schema: public; Owner: f3slackbot
--

CREATE SEQUENCE public.attendance_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.attendance_id_seq OWNER TO f3slackbot;

--
-- Name: attendance_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: f3slackbot
--

ALTER SEQUENCE public.attendance_id_seq OWNED BY public.attendance.id;


--
-- Name: attendance_types_id_seq; Type: SEQUENCE; Schema: public; Owner: f3slackbot
--

CREATE SEQUENCE public.attendance_types_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.attendance_types_id_seq OWNER TO f3slackbot;

--
-- Name: attendance_types_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: f3slackbot
--

ALTER SEQUENCE public.attendance_types_id_seq OWNED BY public.attendance_types.id;


--
-- Name: auth_accounts; Type: TABLE; Schema: public; Owner: f3slackbot
--

CREATE TABLE public.auth_accounts (
    user_id integer NOT NULL,
    type character varying NOT NULL,
    provider character varying NOT NULL,
    provider_account_id character varying NOT NULL,
    refresh_token character varying,
    access_token character varying,
    expires_at timestamp without time zone,
    token_type character varying,
    scope character varying,
    id_token character varying,
    session_state character varying,
    created timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    updated timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL
);


ALTER TABLE public.auth_accounts OWNER TO f3slackbot;

--
-- Name: auth_sessions; Type: TABLE; Schema: public; Owner: f3slackbot
--

CREATE TABLE public.auth_sessions (
    session_token text NOT NULL,
    user_id integer NOT NULL,
    expires timestamp without time zone NOT NULL,
    created timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    updated timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL
);


ALTER TABLE public.auth_sessions OWNER TO f3slackbot;

--
-- Name: auth_verification_tokens; Type: TABLE; Schema: public; Owner: f3slackbot
--

CREATE TABLE public.auth_verification_tokens (
    identifier character varying NOT NULL,
    token character varying NOT NULL,
    expires timestamp without time zone NOT NULL,
    created timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    updated timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL
);


ALTER TABLE public.auth_verification_tokens OWNER TO f3slackbot;

--
-- Name: event_instances_x_event_types; Type: TABLE; Schema: public; Owner: f3slackbot
--

CREATE TABLE public.event_instances_x_event_types (
    event_instance_id integer NOT NULL,
    event_type_id integer NOT NULL
);


ALTER TABLE public.event_instances_x_event_types OWNER TO f3slackbot;

--
-- Name: event_tags; Type: TABLE; Schema: public; Owner: f3slackbot
--

CREATE TABLE public.event_tags (
    id integer NOT NULL,
    name character varying NOT NULL,
    description character varying,
    color character varying,
    created timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    updated timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    specific_org_id integer,
    is_active boolean DEFAULT true NOT NULL
);


ALTER TABLE public.event_tags OWNER TO f3slackbot;

--
-- Name: event_tags_x_event_instances; Type: TABLE; Schema: public; Owner: f3slackbot
--

CREATE TABLE public.event_tags_x_event_instances (
    event_instance_id integer NOT NULL,
    event_tag_id integer NOT NULL
);


ALTER TABLE public.event_tags_x_event_instances OWNER TO f3slackbot;

--
-- Name: event_types; Type: TABLE; Schema: public; Owner: f3slackbot
--

CREATE TABLE public.event_types (
    id integer NOT NULL,
    name character varying NOT NULL,
    description character varying,
    acronym character varying,
    created timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    updated timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    specific_org_id integer,
    event_category public.event_category NOT NULL,
    is_active boolean DEFAULT true NOT NULL
);


ALTER TABLE public.event_types OWNER TO f3slackbot;

--
-- Name: events; Type: TABLE; Schema: public; Owner: f3slackbot
--

CREATE TABLE public.events (
    id integer NOT NULL,
    org_id integer NOT NULL,
    location_id integer,
    series_id integer,
    is_active boolean NOT NULL,
    highlight boolean NOT NULL,
    start_date date NOT NULL,
    end_date date,
    start_time character varying,
    end_time character varying,
    day_of_week public.day_of_week,
    name character varying NOT NULL,
    description character varying,
    recurrence_pattern public.event_cadence,
    recurrence_interval integer,
    index_within_interval integer,
    meta json,
    created timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    updated timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    email character varying,
    is_private boolean DEFAULT false NOT NULL
);


ALTER TABLE public.events OWNER TO f3slackbot;

--
-- Name: locations; Type: TABLE; Schema: public; Owner: f3slackbot
--

CREATE TABLE public.locations (
    id integer NOT NULL,
    org_id integer NOT NULL,
    name character varying NOT NULL,
    description character varying,
    is_active boolean NOT NULL,
    latitude double precision,
    longitude double precision,
    address_street character varying,
    address_city character varying,
    address_state character varying,
    address_zip character varying,
    address_country character varying,
    meta json,
    created timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    updated timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    email character varying,
    address_street2 character varying
);


ALTER TABLE public.locations OWNER TO f3slackbot;

--
-- Name: event_instance_expanded; Type: VIEW; Schema: public; Owner: f3slackbot
--

CREATE VIEW public.event_instance_expanded AS
 SELECT ei.id,
    ei.org_id,
    ei.location_id,
    ei.series_id,
    ei.highlight,
    ei.start_date,
    ei.end_date,
    ei.start_time,
    ei.end_time,
    ei.name,
    ei.description,
    ei.pax_count,
    ei.fng_count,
    ei.preblast,
    ei.backblast,
    ei.meta,
    ei.created,
    ei.updated,
    e.name AS series_name,
    e.description AS series_description,
    o0.id AS ao_org_id,
    o0.name AS ao_name,
    o0.description AS ao_description,
    o0.logo_url AS ao_logo_url,
    o0.website AS ao_website,
    o0.meta AS ao_meta,
    COALESCE(o1.id, o2.id) AS region_org_id,
    COALESCE(o1.name, o2.name) AS region_name,
    COALESCE(o1.description, o2.description) AS region_description,
    COALESCE(o1.logo_url, o2.logo_url) AS region_logo_url,
    COALESCE(o1.website, o2.website) AS region_website,
    COALESCE(o1.meta, o2.meta) AS region_meta,
    o3.id AS area_org_id,
    o3.name AS area_name,
    o4.id AS sector_org_id,
    o4.name AS sector_name,
    l.name AS location_name,
    l.description AS location_description,
    l.latitude AS location_latitude,
    l.longitude AS location_longitude,
    tc.bootcamp_ind,
    tc.run_ind,
    tc.ruck_ind,
    tc.first_f_ind,
    tc.second_f_ind,
    tc.third_f_ind,
    ta.pre_workout_ind,
    ta.off_the_books_ind,
    ta.vq_ind,
    ta.convergence_ind,
    tc.all_types,
    ta.all_tags
   FROM (((((((((public.event_instances ei
     LEFT JOIN public.events e ON ((ei.series_id = e.id)))
     LEFT JOIN public.orgs o0 ON (((ei.org_id = o0.id) AND (o0.org_type = 'ao'::public.org_type))))
     LEFT JOIN public.orgs o1 ON (((ei.org_id = o1.id) AND (o1.org_type = 'region'::public.org_type))))
     LEFT JOIN public.orgs o2 ON (((o0.parent_id = o2.id) AND (o2.org_type = 'region'::public.org_type))))
     LEFT JOIN public.orgs o3 ON ((COALESCE(o1.parent_id, o2.parent_id) = o3.id)))
     LEFT JOIN public.orgs o4 ON ((o3.parent_id = o4.id)))
     LEFT JOIN public.locations l ON ((ei.location_id = l.id)))
     LEFT JOIN ( SELECT t.event_instance_id,
            sum(
                CASE
                    WHEN ((t.name)::text = 'Bootcamp'::text) THEN 1
                    ELSE 0
                END) AS bootcamp_ind,
            sum(
                CASE
                    WHEN ((t.name)::text = 'Run'::text) THEN 1
                    ELSE 0
                END) AS run_ind,
            sum(
                CASE
                    WHEN ((t.name)::text = 'Ruck'::text) THEN 1
                    ELSE 0
                END) AS ruck_ind,
            sum(
                CASE
                    WHEN (t.event_category = 'first_f'::public.event_category) THEN 1
                    ELSE 0
                END) AS first_f_ind,
            sum(
                CASE
                    WHEN (t.event_category = 'second_f'::public.event_category) THEN 1
                    ELSE 0
                END) AS second_f_ind,
            sum(
                CASE
                    WHEN (t.event_category = 'third_f'::public.event_category) THEN 1
                    ELSE 0
                END) AS third_f_ind,
            array_agg(t.name) AS all_types
           FROM ( SELECT x.event_instance_id,
                    x.event_type_id,
                    t_1.id,
                    t_1.name,
                    t_1.description,
                    t_1.acronym,
                    t_1.created,
                    t_1.updated,
                    t_1.specific_org_id,
                    t_1.event_category,
                    t_1.is_active
                   FROM (public.event_instances_x_event_types x
                     JOIN public.event_types t_1 ON ((x.event_type_id = t_1.id)))) t
          GROUP BY t.event_instance_id) tc ON ((ei.id = tc.event_instance_id)))
     LEFT JOIN ( SELECT t.event_instance_id,
            sum(
                CASE
                    WHEN ((t.name)::text = 'Pre-Workout'::text) THEN 1
                    ELSE 0
                END) AS pre_workout_ind,
            sum(
                CASE
                    WHEN ((t.name)::text = 'Off-The-Books'::text) THEN 1
                    ELSE 0
                END) AS off_the_books_ind,
            sum(
                CASE
                    WHEN ((t.name)::text = 'VQ'::text) THEN 1
                    ELSE 0
                END) AS vq_ind,
            sum(
                CASE
                    WHEN ((t.name)::text = 'Convergence'::text) THEN 1
                    ELSE 0
                END) AS convergence_ind,
            array_agg(t.name) AS all_tags
           FROM ( SELECT x.event_instance_id,
                    x.event_tag_id,
                    t_1.id,
                    t_1.name,
                    t_1.description,
                    t_1.color,
                    t_1.created,
                    t_1.updated,
                    t_1.specific_org_id
                   FROM (public.event_tags_x_event_instances x
                     JOIN public.event_tags t_1 ON ((x.event_tag_id = t_1.id)))) t
          GROUP BY t.event_instance_id) ta ON ((ei.id = ta.event_instance_id)))
  WHERE (ei.pax_count IS NOT NULL);


ALTER VIEW public.event_instance_expanded OWNER TO f3slackbot;

--
-- Name: event_instances_id_seq; Type: SEQUENCE; Schema: public; Owner: f3slackbot
--

CREATE SEQUENCE public.event_instances_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.event_instances_id_seq OWNER TO f3slackbot;

--
-- Name: event_instances_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: f3slackbot
--

ALTER SEQUENCE public.event_instances_id_seq OWNED BY public.event_instances.id;


--
-- Name: event_tags_id_seq; Type: SEQUENCE; Schema: public; Owner: f3slackbot
--

CREATE SEQUENCE public.event_tags_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.event_tags_id_seq OWNER TO f3slackbot;

--
-- Name: event_tags_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: f3slackbot
--

ALTER SEQUENCE public.event_tags_id_seq OWNED BY public.event_tags.id;


--
-- Name: event_tags_x_events; Type: TABLE; Schema: public; Owner: f3slackbot
--

CREATE TABLE public.event_tags_x_events (
    event_id integer NOT NULL,
    event_tag_id integer NOT NULL
);


ALTER TABLE public.event_tags_x_events OWNER TO f3slackbot;

--
-- Name: event_types_id_seq; Type: SEQUENCE; Schema: public; Owner: f3slackbot
--

CREATE SEQUENCE public.event_types_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.event_types_id_seq OWNER TO f3slackbot;

--
-- Name: event_types_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: f3slackbot
--

ALTER SEQUENCE public.event_types_id_seq OWNED BY public.event_types.id;


--
-- Name: events_id_seq; Type: SEQUENCE; Schema: public; Owner: f3slackbot
--

CREATE SEQUENCE public.events_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.events_id_seq OWNER TO f3slackbot;

--
-- Name: events_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: f3slackbot
--

ALTER SEQUENCE public.events_id_seq OWNED BY public.events.id;


--
-- Name: events_x_event_types; Type: TABLE; Schema: public; Owner: f3slackbot
--

CREATE TABLE public.events_x_event_types (
    event_id integer NOT NULL,
    event_type_id integer NOT NULL
);


ALTER TABLE public.events_x_event_types OWNER TO f3slackbot;

--
-- Name: expansions; Type: TABLE; Schema: public; Owner: f3slackbot
--

CREATE TABLE public.expansions (
    id integer NOT NULL,
    area character varying NOT NULL,
    pinned_lat double precision NOT NULL,
    pinned_lon double precision NOT NULL,
    user_lat double precision NOT NULL,
    user_lon double precision NOT NULL,
    interested_in_organizing boolean NOT NULL,
    created timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    updated timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL
);


ALTER TABLE public.expansions OWNER TO f3slackbot;

--
-- Name: expansions_id_seq; Type: SEQUENCE; Schema: public; Owner: f3slackbot
--

CREATE SEQUENCE public.expansions_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.expansions_id_seq OWNER TO f3slackbot;

--
-- Name: expansions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: f3slackbot
--

ALTER SEQUENCE public.expansions_id_seq OWNED BY public.expansions.id;


--
-- Name: expansions_x_users; Type: TABLE; Schema: public; Owner: f3slackbot
--

CREATE TABLE public.expansions_x_users (
    expansion_id integer NOT NULL,
    user_id integer NOT NULL,
    request_date timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    notes character varying
);


ALTER TABLE public.expansions_x_users OWNER TO f3slackbot;

--
-- Name: locations_id_seq; Type: SEQUENCE; Schema: public; Owner: f3slackbot
--

CREATE SEQUENCE public.locations_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.locations_id_seq OWNER TO f3slackbot;

--
-- Name: locations_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: f3slackbot
--

ALTER SEQUENCE public.locations_id_seq OWNED BY public.locations.id;


--
-- Name: materializedviews; Type: TABLE; Schema: public; Owner: app_materializedviewrefresher
--

CREATE TABLE public.materializedviews (
    name text NOT NULL,
    hours text NOT NULL
);


ALTER TABLE public.materializedviews OWNER TO app_materializedviewrefresher;

--
-- Name: orgs_id_seq; Type: SEQUENCE; Schema: public; Owner: f3slackbot
--

CREATE SEQUENCE public.orgs_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.orgs_id_seq OWNER TO f3slackbot;

--
-- Name: orgs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: f3slackbot
--

ALTER SEQUENCE public.orgs_id_seq OWNED BY public.orgs.id;


--
-- Name: orgs_x_slack_spaces; Type: TABLE; Schema: public; Owner: f3slackbot
--

CREATE TABLE public.orgs_x_slack_spaces (
    org_id integer NOT NULL,
    slack_space_id integer NOT NULL
);


ALTER TABLE public.orgs_x_slack_spaces OWNER TO f3slackbot;

--
-- Name: permissions; Type: TABLE; Schema: public; Owner: f3slackbot
--

CREATE TABLE public.permissions (
    id integer NOT NULL,
    name character varying NOT NULL,
    description character varying,
    created timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    updated timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL
);


ALTER TABLE public.permissions OWNER TO f3slackbot;

--
-- Name: permissions_id_seq; Type: SEQUENCE; Schema: public; Owner: f3slackbot
--

CREATE SEQUENCE public.permissions_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.permissions_id_seq OWNER TO f3slackbot;

--
-- Name: permissions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: f3slackbot
--

ALTER SEQUENCE public.permissions_id_seq OWNED BY public.permissions.id;


--
-- Name: positions; Type: TABLE; Schema: public; Owner: f3slackbot
--

CREATE TABLE public.positions (
    id integer NOT NULL,
    name character varying NOT NULL,
    description character varying,
    org_id integer,
    created timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    updated timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    org_type public.org_type,
    is_active boolean DEFAULT true NOT NULL
);


ALTER TABLE public.positions OWNER TO f3slackbot;

--
-- Name: positions_id_seq; Type: SEQUENCE; Schema: public; Owner: f3slackbot
--

CREATE SEQUENCE public.positions_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.positions_id_seq OWNER TO f3slackbot;

--
-- Name: positions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: f3slackbot
--

ALTER SEQUENCE public.positions_id_seq OWNED BY public.positions.id;


--
-- Name: positions_x_orgs_x_users; Type: TABLE; Schema: public; Owner: f3slackbot
--

CREATE TABLE public.positions_x_orgs_x_users (
    position_id integer NOT NULL,
    org_id integer NOT NULL,
    user_id integer NOT NULL
);


ALTER TABLE public.positions_x_orgs_x_users OWNER TO f3slackbot;

--
-- Name: roles; Type: TABLE; Schema: public; Owner: f3slackbot
--

CREATE TABLE public.roles (
    id integer NOT NULL,
    name public.region_role NOT NULL,
    description character varying,
    created timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    updated timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL
);


ALTER TABLE public.roles OWNER TO f3slackbot;

--
-- Name: roles_id_seq; Type: SEQUENCE; Schema: public; Owner: f3slackbot
--

CREATE SEQUENCE public.roles_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.roles_id_seq OWNER TO f3slackbot;

--
-- Name: roles_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: f3slackbot
--

ALTER SEQUENCE public.roles_id_seq OWNED BY public.roles.id;


--
-- Name: roles_x_api_keys_x_org; Type: TABLE; Schema: public; Owner: f3slackbot
--

CREATE TABLE public.roles_x_api_keys_x_org (
    role_id integer NOT NULL,
    api_key_id integer NOT NULL,
    org_id integer NOT NULL
);


ALTER TABLE public.roles_x_api_keys_x_org OWNER TO f3slackbot;

--
-- Name: roles_x_permissions; Type: TABLE; Schema: public; Owner: f3slackbot
--

CREATE TABLE public.roles_x_permissions (
    role_id integer NOT NULL,
    permission_id integer NOT NULL
);


ALTER TABLE public.roles_x_permissions OWNER TO f3slackbot;

--
-- Name: roles_x_users_x_org; Type: TABLE; Schema: public; Owner: f3slackbot
--

CREATE TABLE public.roles_x_users_x_org (
    role_id integer NOT NULL,
    user_id integer NOT NULL,
    org_id integer NOT NULL
);


ALTER TABLE public.roles_x_users_x_org OWNER TO f3slackbot;

--
-- Name: slack_spaces; Type: TABLE; Schema: public; Owner: f3slackbot
--

CREATE TABLE public.slack_spaces (
    id integer NOT NULL,
    team_id character varying NOT NULL,
    workspace_name character varying,
    bot_token character varying,
    settings json,
    created timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    updated timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL
);


ALTER TABLE public.slack_spaces OWNER TO f3slackbot;

--
-- Name: slack_spaces_id_seq; Type: SEQUENCE; Schema: public; Owner: f3slackbot
--

CREATE SEQUENCE public.slack_spaces_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.slack_spaces_id_seq OWNER TO f3slackbot;

--
-- Name: slack_spaces_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: f3slackbot
--

ALTER SEQUENCE public.slack_spaces_id_seq OWNED BY public.slack_spaces.id;


--
-- Name: slack_users; Type: TABLE; Schema: public; Owner: f3slackbot
--

CREATE TABLE public.slack_users (
    id integer NOT NULL,
    slack_id character varying NOT NULL,
    user_name character varying NOT NULL,
    email character varying NOT NULL,
    is_admin boolean NOT NULL,
    is_owner boolean NOT NULL,
    is_bot boolean NOT NULL,
    user_id integer,
    avatar_url character varying,
    slack_team_id character varying NOT NULL,
    strava_access_token character varying,
    strava_refresh_token character varying,
    strava_expires_at timestamp without time zone,
    strava_athlete_id integer,
    meta json,
    slack_updated integer,
    created timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    updated timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL
);


ALTER TABLE public.slack_users OWNER TO f3slackbot;

--
-- Name: slack_users_id_seq; Type: SEQUENCE; Schema: public; Owner: f3slackbot
--

CREATE SEQUENCE public.slack_users_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.slack_users_id_seq OWNER TO f3slackbot;

--
-- Name: slack_users_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: f3slackbot
--

ALTER SEQUENCE public.slack_users_id_seq OWNED BY public.slack_users.id;


--
-- Name: update_requests; Type: TABLE; Schema: public; Owner: f3slackbot
--

CREATE TABLE public.update_requests (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    token uuid DEFAULT gen_random_uuid() NOT NULL,
    region_id integer NOT NULL,
    event_id integer,
    event_type_ids integer[],
    event_tag character varying,
    event_series_id integer,
    event_is_series boolean,
    event_is_active boolean,
    event_highlight boolean,
    event_start_date date,
    event_end_date date,
    event_start_time character varying,
    event_end_time character varying,
    event_day_of_week public.day_of_week,
    event_name character varying NOT NULL,
    event_description character varying,
    event_recurrence_pattern public.event_cadence,
    event_recurrence_interval integer,
    event_index_within_interval integer,
    event_meta json,
    event_contact_email character varying,
    location_name character varying,
    location_description character varying,
    location_address character varying,
    location_address2 character varying,
    location_city character varying,
    location_state character varying,
    location_zip character varying,
    location_country character varying,
    location_lat real,
    location_lng real,
    location_id integer,
    location_contact_email character varying,
    ao_id integer,
    ao_name character varying,
    ao_logo character varying,
    submitted_by character varying NOT NULL,
    submitter_validated boolean,
    reviewed_by character varying,
    reviewed_at timestamp without time zone,
    status public.update_request_status DEFAULT 'pending'::public.update_request_status NOT NULL,
    meta json,
    created timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    updated timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    request_type public.request_type NOT NULL,
    ao_website character varying
);


ALTER TABLE public.update_requests OWNER TO f3slackbot;

--
-- Name: users_id_seq; Type: SEQUENCE; Schema: public; Owner: f3slackbot
--

CREATE SEQUENCE public.users_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.users_id_seq OWNER TO f3slackbot;

--
-- Name: users_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: f3slackbot
--

ALTER SEQUENCE public.users_id_seq OWNED BY public.users.id;


--
-- Name: achievements id; Type: DEFAULT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.achievements ALTER COLUMN id SET DEFAULT nextval('public.achievements_id_seq'::regclass);


--
-- Name: api_keys id; Type: DEFAULT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.api_keys ALTER COLUMN id SET DEFAULT nextval('public.api_keys_id_seq'::regclass);


--
-- Name: attendance id; Type: DEFAULT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.attendance ALTER COLUMN id SET DEFAULT nextval('public.attendance_id_seq'::regclass);


--
-- Name: attendance_types id; Type: DEFAULT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.attendance_types ALTER COLUMN id SET DEFAULT nextval('public.attendance_types_id_seq'::regclass);


--
-- Name: event_instances id; Type: DEFAULT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.event_instances ALTER COLUMN id SET DEFAULT nextval('public.event_instances_id_seq'::regclass);


--
-- Name: event_tags id; Type: DEFAULT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.event_tags ALTER COLUMN id SET DEFAULT nextval('public.event_tags_id_seq'::regclass);


--
-- Name: event_types id; Type: DEFAULT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.event_types ALTER COLUMN id SET DEFAULT nextval('public.event_types_id_seq'::regclass);


--
-- Name: events id; Type: DEFAULT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.events ALTER COLUMN id SET DEFAULT nextval('public.events_id_seq'::regclass);


--
-- Name: expansions id; Type: DEFAULT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.expansions ALTER COLUMN id SET DEFAULT nextval('public.expansions_id_seq'::regclass);


--
-- Name: locations id; Type: DEFAULT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.locations ALTER COLUMN id SET DEFAULT nextval('public.locations_id_seq'::regclass);


--
-- Name: orgs id; Type: DEFAULT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.orgs ALTER COLUMN id SET DEFAULT nextval('public.orgs_id_seq'::regclass);


--
-- Name: permissions id; Type: DEFAULT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.permissions ALTER COLUMN id SET DEFAULT nextval('public.permissions_id_seq'::regclass);


--
-- Name: positions id; Type: DEFAULT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.positions ALTER COLUMN id SET DEFAULT nextval('public.positions_id_seq'::regclass);


--
-- Name: roles id; Type: DEFAULT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.roles ALTER COLUMN id SET DEFAULT nextval('public.roles_id_seq'::regclass);


--
-- Name: slack_spaces id; Type: DEFAULT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.slack_spaces ALTER COLUMN id SET DEFAULT nextval('public.slack_spaces_id_seq'::regclass);


--
-- Name: slack_users id; Type: DEFAULT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.slack_users ALTER COLUMN id SET DEFAULT nextval('public.slack_users_id_seq'::regclass);


--
-- Name: users id; Type: DEFAULT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.users ALTER COLUMN id SET DEFAULT nextval('public.users_id_seq'::regclass);


--
-- Name: achievements achievements_pkey; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.achievements
    ADD CONSTRAINT achievements_pkey PRIMARY KEY (id);


--
-- Name: achievements_x_users achievements_x_users_pkey; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.achievements_x_users
    ADD CONSTRAINT achievements_x_users_pkey PRIMARY KEY (achievement_id, user_id, award_year, award_period);


--
-- Name: alembic_version alembic_version_pkey; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.alembic_version
    ADD CONSTRAINT alembic_version_pkey PRIMARY KEY (version_num);


--
-- Name: api_keys api_keys_key_key; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.api_keys
    ADD CONSTRAINT api_keys_key_key UNIQUE (key);


--
-- Name: api_keys api_keys_pkey; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.api_keys
    ADD CONSTRAINT api_keys_pkey PRIMARY KEY (id);


--
-- Name: attendance attendance_event_instance_id_user_id_is_planned_key; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.attendance
    ADD CONSTRAINT attendance_event_instance_id_user_id_is_planned_key UNIQUE (user_id, is_planned, event_instance_id);


--
-- Name: attendance attendance_pkey; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.attendance
    ADD CONSTRAINT attendance_pkey PRIMARY KEY (id);


--
-- Name: attendance_types attendance_types_pkey; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.attendance_types
    ADD CONSTRAINT attendance_types_pkey PRIMARY KEY (id);


--
-- Name: attendance_x_attendance_types attendance_x_attendance_types_pkey; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.attendance_x_attendance_types
    ADD CONSTRAINT attendance_x_attendance_types_pkey PRIMARY KEY (attendance_id, attendance_type_id);


--
-- Name: auth_accounts auth_accounts_pkey; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.auth_accounts
    ADD CONSTRAINT auth_accounts_pkey PRIMARY KEY (provider, provider_account_id);


--
-- Name: auth_sessions auth_sessions_pkey; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.auth_sessions
    ADD CONSTRAINT auth_sessions_pkey PRIMARY KEY (session_token);


--
-- Name: auth_verification_tokens auth_verification_tokens_pkey; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.auth_verification_tokens
    ADD CONSTRAINT auth_verification_tokens_pkey PRIMARY KEY (identifier, token);


--
-- Name: event_instances event_instances_pkey; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.event_instances
    ADD CONSTRAINT event_instances_pkey PRIMARY KEY (id);


--
-- Name: event_instances_x_event_types event_instances_x_event_types_pkey; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.event_instances_x_event_types
    ADD CONSTRAINT event_instances_x_event_types_pkey PRIMARY KEY (event_instance_id, event_type_id);


--
-- Name: event_tags event_tags_pkey; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.event_tags
    ADD CONSTRAINT event_tags_pkey PRIMARY KEY (id);


--
-- Name: event_tags_x_event_instances event_tags_x_event_instances_pkey; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.event_tags_x_event_instances
    ADD CONSTRAINT event_tags_x_event_instances_pkey PRIMARY KEY (event_instance_id, event_tag_id);


--
-- Name: event_tags_x_events event_tags_x_events_pkey; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.event_tags_x_events
    ADD CONSTRAINT event_tags_x_events_pkey PRIMARY KEY (event_id, event_tag_id);


--
-- Name: event_types event_types_pkey; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.event_types
    ADD CONSTRAINT event_types_pkey PRIMARY KEY (id);


--
-- Name: events events_pkey; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.events
    ADD CONSTRAINT events_pkey PRIMARY KEY (id);


--
-- Name: events_x_event_types events_x_event_types_pkey; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.events_x_event_types
    ADD CONSTRAINT events_x_event_types_pkey PRIMARY KEY (event_id, event_type_id);


--
-- Name: expansions expansions_pkey; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.expansions
    ADD CONSTRAINT expansions_pkey PRIMARY KEY (id);


--
-- Name: expansions_x_users expansions_x_users_pkey; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.expansions_x_users
    ADD CONSTRAINT expansions_x_users_pkey PRIMARY KEY (expansion_id, user_id);


--
-- Name: locations locations_pkey; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.locations
    ADD CONSTRAINT locations_pkey PRIMARY KEY (id);


--
-- Name: materializedviews materializedviews_pkey; Type: CONSTRAINT; Schema: public; Owner: app_materializedviewrefresher
--

ALTER TABLE ONLY public.materializedviews
    ADD CONSTRAINT materializedviews_pkey PRIMARY KEY (name);


--
-- Name: orgs orgs_pkey; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.orgs
    ADD CONSTRAINT orgs_pkey PRIMARY KEY (id);


--
-- Name: orgs_x_slack_spaces orgs_x_slack_spaces_pkey; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.orgs_x_slack_spaces
    ADD CONSTRAINT orgs_x_slack_spaces_pkey PRIMARY KEY (org_id, slack_space_id);


--
-- Name: permissions permissions_pkey; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.permissions
    ADD CONSTRAINT permissions_pkey PRIMARY KEY (id);


--
-- Name: positions positions_pkey; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.positions
    ADD CONSTRAINT positions_pkey PRIMARY KEY (id);


--
-- Name: positions_x_orgs_x_users positions_x_orgs_x_users_pkey; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.positions_x_orgs_x_users
    ADD CONSTRAINT positions_x_orgs_x_users_pkey PRIMARY KEY (position_id, org_id, user_id);


--
-- Name: roles roles_pkey; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.roles
    ADD CONSTRAINT roles_pkey PRIMARY KEY (id);


--
-- Name: roles_x_api_keys_x_org roles_x_api_keys_x_org_pkey; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.roles_x_api_keys_x_org
    ADD CONSTRAINT roles_x_api_keys_x_org_pkey PRIMARY KEY (role_id, api_key_id, org_id);


--
-- Name: roles_x_permissions roles_x_permissions_pkey; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.roles_x_permissions
    ADD CONSTRAINT roles_x_permissions_pkey PRIMARY KEY (role_id, permission_id);


--
-- Name: roles_x_users_x_org roles_x_users_x_org_pkey; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.roles_x_users_x_org
    ADD CONSTRAINT roles_x_users_x_org_pkey PRIMARY KEY (role_id, user_id, org_id);


--
-- Name: slack_spaces slack_spaces_pkey; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.slack_spaces
    ADD CONSTRAINT slack_spaces_pkey PRIMARY KEY (id);


--
-- Name: slack_spaces slack_spaces_team_id_key; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.slack_spaces
    ADD CONSTRAINT slack_spaces_team_id_key UNIQUE (team_id);


--
-- Name: slack_users slack_users_pkey; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.slack_users
    ADD CONSTRAINT slack_users_pkey PRIMARY KEY (id);


--
-- Name: update_requests update_requests_pkey; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.update_requests
    ADD CONSTRAINT update_requests_pkey PRIMARY KEY (id);


--
-- Name: users users_email_key; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_email_key UNIQUE (email);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: idx_attendance_event_instance_id; Type: INDEX; Schema: public; Owner: f3slackbot
--

CREATE INDEX idx_attendance_event_instance_id ON public.attendance USING btree (event_instance_id);


--
-- Name: idx_attendance_x_types_type_id; Type: INDEX; Schema: public; Owner: f3slackbot
--

CREATE INDEX idx_attendance_x_types_type_id ON public.attendance_x_attendance_types USING btree (attendance_type_id);


--
-- Name: idx_event_instances_is_active; Type: INDEX; Schema: public; Owner: f3slackbot
--

CREATE INDEX idx_event_instances_is_active ON public.event_instances USING btree (is_active);


--
-- Name: idx_event_instances_location_id; Type: INDEX; Schema: public; Owner: f3slackbot
--

CREATE INDEX idx_event_instances_location_id ON public.event_instances USING btree (location_id);


--
-- Name: idx_event_instances_org_id; Type: INDEX; Schema: public; Owner: f3slackbot
--

CREATE INDEX idx_event_instances_org_id ON public.event_instances USING btree (org_id);


--
-- Name: idx_event_instances_start_date; Type: INDEX; Schema: public; Owner: f3slackbot
--

CREATE INDEX idx_event_instances_start_date ON public.event_instances USING btree (start_date);


--
-- Name: idx_event_instances_start_date_active; Type: INDEX; Schema: public; Owner: f3slackbot
--

CREATE INDEX idx_event_instances_start_date_active ON public.event_instances USING btree (start_date) WHERE is_active;


--
-- Name: idx_events_is_active; Type: INDEX; Schema: public; Owner: f3slackbot
--

CREATE INDEX idx_events_is_active ON public.events USING btree (is_active);


--
-- Name: idx_events_location_id; Type: INDEX; Schema: public; Owner: f3slackbot
--

CREATE INDEX idx_events_location_id ON public.events USING btree (location_id);


--
-- Name: idx_events_org_id; Type: INDEX; Schema: public; Owner: f3slackbot
--

CREATE INDEX idx_events_org_id ON public.events USING btree (org_id);


--
-- Name: idx_events_x_event_types_event_id; Type: INDEX; Schema: public; Owner: f3slackbot
--

CREATE INDEX idx_events_x_event_types_event_id ON public.events_x_event_types USING btree (event_id);


--
-- Name: idx_events_x_event_types_event_type_id; Type: INDEX; Schema: public; Owner: f3slackbot
--

CREATE INDEX idx_events_x_event_types_event_type_id ON public.events_x_event_types USING btree (event_type_id);


--
-- Name: idx_locations_is_active; Type: INDEX; Schema: public; Owner: f3slackbot
--

CREATE INDEX idx_locations_is_active ON public.locations USING btree (is_active);


--
-- Name: idx_locations_name; Type: INDEX; Schema: public; Owner: f3slackbot
--

CREATE INDEX idx_locations_name ON public.locations USING btree (name);


--
-- Name: idx_locations_org_id; Type: INDEX; Schema: public; Owner: f3slackbot
--

CREATE INDEX idx_locations_org_id ON public.locations USING btree (org_id);


--
-- Name: idx_orgs_is_active; Type: INDEX; Schema: public; Owner: f3slackbot
--

CREATE INDEX idx_orgs_is_active ON public.orgs USING btree (is_active);


--
-- Name: idx_orgs_org_type; Type: INDEX; Schema: public; Owner: f3slackbot
--

CREATE INDEX idx_orgs_org_type ON public.orgs USING btree (org_type);


--
-- Name: idx_orgs_parent_id; Type: INDEX; Schema: public; Owner: f3slackbot
--

CREATE INDEX idx_orgs_parent_id ON public.orgs USING btree (parent_id);


--
-- Name: idx_slack_users_user_team_id; Type: INDEX; Schema: public; Owner: f3slackbot
--

CREATE INDEX idx_slack_users_user_team_id ON public.slack_users USING btree (user_id, slack_team_id);


--
-- Name: achievements set_updated_achievements; Type: TRIGGER; Schema: public; Owner: f3slackbot
--

CREATE TRIGGER set_updated_achievements BEFORE UPDATE ON public.achievements FOR EACH ROW EXECUTE FUNCTION public.set_updated_column();


--
-- Name: api_keys set_updated_api_keys; Type: TRIGGER; Schema: public; Owner: f3slackbot
--

CREATE TRIGGER set_updated_api_keys BEFORE UPDATE ON public.api_keys FOR EACH ROW EXECUTE FUNCTION public.set_updated_column();


--
-- Name: attendance set_updated_attendance; Type: TRIGGER; Schema: public; Owner: f3slackbot
--

CREATE TRIGGER set_updated_attendance BEFORE UPDATE ON public.attendance FOR EACH ROW EXECUTE FUNCTION public.set_updated_column();


--
-- Name: attendance_types set_updated_attendance_types; Type: TRIGGER; Schema: public; Owner: f3slackbot
--

CREATE TRIGGER set_updated_attendance_types BEFORE UPDATE ON public.attendance_types FOR EACH ROW EXECUTE FUNCTION public.set_updated_column();


--
-- Name: auth_accounts set_updated_auth_accounts; Type: TRIGGER; Schema: public; Owner: f3slackbot
--

CREATE TRIGGER set_updated_auth_accounts BEFORE UPDATE ON public.auth_accounts FOR EACH ROW EXECUTE FUNCTION public.set_updated_column();


--
-- Name: auth_sessions set_updated_auth_sessions; Type: TRIGGER; Schema: public; Owner: f3slackbot
--

CREATE TRIGGER set_updated_auth_sessions BEFORE UPDATE ON public.auth_sessions FOR EACH ROW EXECUTE FUNCTION public.set_updated_column();


--
-- Name: auth_verification_tokens set_updated_auth_verification_tokens; Type: TRIGGER; Schema: public; Owner: f3slackbot
--

CREATE TRIGGER set_updated_auth_verification_tokens BEFORE UPDATE ON public.auth_verification_tokens FOR EACH ROW EXECUTE FUNCTION public.set_updated_column();


--
-- Name: event_instances set_updated_event_instances; Type: TRIGGER; Schema: public; Owner: f3slackbot
--

CREATE TRIGGER set_updated_event_instances BEFORE UPDATE ON public.event_instances FOR EACH ROW EXECUTE FUNCTION public.set_updated_column();


--
-- Name: event_tags set_updated_event_tags; Type: TRIGGER; Schema: public; Owner: f3slackbot
--

CREATE TRIGGER set_updated_event_tags BEFORE UPDATE ON public.event_tags FOR EACH ROW EXECUTE FUNCTION public.set_updated_column();


--
-- Name: event_types set_updated_event_types; Type: TRIGGER; Schema: public; Owner: f3slackbot
--

CREATE TRIGGER set_updated_event_types BEFORE UPDATE ON public.event_types FOR EACH ROW EXECUTE FUNCTION public.set_updated_column();


--
-- Name: events set_updated_events; Type: TRIGGER; Schema: public; Owner: f3slackbot
--

CREATE TRIGGER set_updated_events BEFORE UPDATE ON public.events FOR EACH ROW EXECUTE FUNCTION public.set_updated_column();


--
-- Name: expansions set_updated_expansions; Type: TRIGGER; Schema: public; Owner: f3slackbot
--

CREATE TRIGGER set_updated_expansions BEFORE UPDATE ON public.expansions FOR EACH ROW EXECUTE FUNCTION public.set_updated_column();


--
-- Name: locations set_updated_locations; Type: TRIGGER; Schema: public; Owner: f3slackbot
--

CREATE TRIGGER set_updated_locations BEFORE UPDATE ON public.locations FOR EACH ROW EXECUTE FUNCTION public.set_updated_column();


--
-- Name: orgs set_updated_orgs; Type: TRIGGER; Schema: public; Owner: f3slackbot
--

CREATE TRIGGER set_updated_orgs BEFORE UPDATE ON public.orgs FOR EACH ROW EXECUTE FUNCTION public.set_updated_column();


--
-- Name: permissions set_updated_permissions; Type: TRIGGER; Schema: public; Owner: f3slackbot
--

CREATE TRIGGER set_updated_permissions BEFORE UPDATE ON public.permissions FOR EACH ROW EXECUTE FUNCTION public.set_updated_column();


--
-- Name: positions set_updated_positions; Type: TRIGGER; Schema: public; Owner: f3slackbot
--

CREATE TRIGGER set_updated_positions BEFORE UPDATE ON public.positions FOR EACH ROW EXECUTE FUNCTION public.set_updated_column();


--
-- Name: roles set_updated_roles; Type: TRIGGER; Schema: public; Owner: f3slackbot
--

CREATE TRIGGER set_updated_roles BEFORE UPDATE ON public.roles FOR EACH ROW EXECUTE FUNCTION public.set_updated_column();


--
-- Name: slack_spaces set_updated_slack_spaces; Type: TRIGGER; Schema: public; Owner: f3slackbot
--

CREATE TRIGGER set_updated_slack_spaces BEFORE UPDATE ON public.slack_spaces FOR EACH ROW EXECUTE FUNCTION public.set_updated_column();


--
-- Name: slack_users set_updated_slack_users; Type: TRIGGER; Schema: public; Owner: f3slackbot
--

CREATE TRIGGER set_updated_slack_users BEFORE UPDATE ON public.slack_users FOR EACH ROW EXECUTE FUNCTION public.set_updated_column();


--
-- Name: update_requests set_updated_update_requests; Type: TRIGGER; Schema: public; Owner: f3slackbot
--

CREATE TRIGGER set_updated_update_requests BEFORE UPDATE ON public.update_requests FOR EACH ROW EXECUTE FUNCTION public.set_updated_column();


--
-- Name: users set_updated_users; Type: TRIGGER; Schema: public; Owner: f3slackbot
--

CREATE TRIGGER set_updated_users BEFORE UPDATE ON public.users FOR EACH ROW EXECUTE FUNCTION public.set_updated_column();


--
-- Name: orgs update_org_ao_counts_trigger; Type: TRIGGER; Schema: public; Owner: f3slackbot
--

CREATE TRIGGER update_org_ao_counts_trigger AFTER INSERT OR DELETE OR UPDATE ON public.orgs FOR EACH ROW EXECUTE FUNCTION public.update_org_ao_counts();


--
-- Name: achievements achievements_specific_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.achievements
    ADD CONSTRAINT achievements_specific_org_id_fkey FOREIGN KEY (specific_org_id) REFERENCES public.orgs(id);


--
-- Name: achievements_x_users achievements_x_users_achievement_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.achievements_x_users
    ADD CONSTRAINT achievements_x_users_achievement_id_fkey FOREIGN KEY (achievement_id) REFERENCES public.achievements(id);


--
-- Name: achievements_x_users achievements_x_users_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.achievements_x_users
    ADD CONSTRAINT achievements_x_users_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id);


--
-- Name: api_keys api_keys_owner_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.api_keys
    ADD CONSTRAINT api_keys_owner_id_fkey FOREIGN KEY (owner_id) REFERENCES public.users(id);


--
-- Name: attendance attendance_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.attendance
    ADD CONSTRAINT attendance_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id);


--
-- Name: attendance_x_attendance_types attendance_x_attendance_types_attendance_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.attendance_x_attendance_types
    ADD CONSTRAINT attendance_x_attendance_types_attendance_id_fkey FOREIGN KEY (attendance_id) REFERENCES public.attendance(id) ON DELETE CASCADE;


--
-- Name: attendance_x_attendance_types attendance_x_attendance_types_attendance_type_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.attendance_x_attendance_types
    ADD CONSTRAINT attendance_x_attendance_types_attendance_type_id_fkey FOREIGN KEY (attendance_type_id) REFERENCES public.attendance_types(id);


--
-- Name: auth_accounts auth_accounts_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.auth_accounts
    ADD CONSTRAINT auth_accounts_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id);


--
-- Name: auth_sessions auth_sessions_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.auth_sessions
    ADD CONSTRAINT auth_sessions_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: attendance event_instance_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.attendance
    ADD CONSTRAINT event_instance_id_fkey FOREIGN KEY (event_instance_id) REFERENCES public.event_instances(id) ON DELETE CASCADE;


--
-- Name: event_instances event_instances_location_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.event_instances
    ADD CONSTRAINT event_instances_location_id_fkey FOREIGN KEY (location_id) REFERENCES public.locations(id);


--
-- Name: event_instances event_instances_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.event_instances
    ADD CONSTRAINT event_instances_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.orgs(id);


--
-- Name: event_instances event_instances_series_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.event_instances
    ADD CONSTRAINT event_instances_series_id_fkey FOREIGN KEY (series_id) REFERENCES public.events(id) ON DELETE CASCADE;


--
-- Name: event_instances_x_event_types event_instances_x_event_types_event_instance_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.event_instances_x_event_types
    ADD CONSTRAINT event_instances_x_event_types_event_instance_id_fkey FOREIGN KEY (event_instance_id) REFERENCES public.event_instances(id) ON DELETE CASCADE;


--
-- Name: event_instances_x_event_types event_instances_x_event_types_event_type_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.event_instances_x_event_types
    ADD CONSTRAINT event_instances_x_event_types_event_type_id_fkey FOREIGN KEY (event_type_id) REFERENCES public.event_types(id);


--
-- Name: event_tags event_tags_specific_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.event_tags
    ADD CONSTRAINT event_tags_specific_org_id_fkey FOREIGN KEY (specific_org_id) REFERENCES public.orgs(id);


--
-- Name: event_tags_x_event_instances event_tags_x_event_instances_event_instance_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.event_tags_x_event_instances
    ADD CONSTRAINT event_tags_x_event_instances_event_instance_id_fkey FOREIGN KEY (event_instance_id) REFERENCES public.event_instances(id) ON DELETE CASCADE;


--
-- Name: event_tags_x_event_instances event_tags_x_event_instances_event_tag_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.event_tags_x_event_instances
    ADD CONSTRAINT event_tags_x_event_instances_event_tag_id_fkey FOREIGN KEY (event_tag_id) REFERENCES public.event_tags(id);


--
-- Name: event_tags_x_events event_tags_x_events_event_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.event_tags_x_events
    ADD CONSTRAINT event_tags_x_events_event_id_fkey FOREIGN KEY (event_id) REFERENCES public.events(id) ON DELETE CASCADE;


--
-- Name: event_tags_x_events event_tags_x_events_event_tag_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.event_tags_x_events
    ADD CONSTRAINT event_tags_x_events_event_tag_id_fkey FOREIGN KEY (event_tag_id) REFERENCES public.event_tags(id);


--
-- Name: event_types event_types_specific_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.event_types
    ADD CONSTRAINT event_types_specific_org_id_fkey FOREIGN KEY (specific_org_id) REFERENCES public.orgs(id);


--
-- Name: events events_location_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.events
    ADD CONSTRAINT events_location_id_fkey FOREIGN KEY (location_id) REFERENCES public.locations(id);


--
-- Name: events events_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.events
    ADD CONSTRAINT events_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.orgs(id);


--
-- Name: events events_series_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.events
    ADD CONSTRAINT events_series_id_fkey FOREIGN KEY (series_id) REFERENCES public.events(id);


--
-- Name: events_x_event_types events_x_event_types_event_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.events_x_event_types
    ADD CONSTRAINT events_x_event_types_event_id_fkey FOREIGN KEY (event_id) REFERENCES public.events(id) ON DELETE CASCADE;


--
-- Name: events_x_event_types events_x_event_types_event_type_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.events_x_event_types
    ADD CONSTRAINT events_x_event_types_event_type_id_fkey FOREIGN KEY (event_type_id) REFERENCES public.event_types(id);


--
-- Name: expansions_x_users expansions_x_users_expansion_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.expansions_x_users
    ADD CONSTRAINT expansions_x_users_expansion_id_fkey FOREIGN KEY (expansion_id) REFERENCES public.expansions(id);


--
-- Name: expansions_x_users expansions_x_users_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.expansions_x_users
    ADD CONSTRAINT expansions_x_users_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id);


--
-- Name: locations locations_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.locations
    ADD CONSTRAINT locations_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.orgs(id);


--
-- Name: orgs orgs_parent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.orgs
    ADD CONSTRAINT orgs_parent_id_fkey FOREIGN KEY (parent_id) REFERENCES public.orgs(id);


--
-- Name: orgs_x_slack_spaces orgs_x_slack_spaces_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.orgs_x_slack_spaces
    ADD CONSTRAINT orgs_x_slack_spaces_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.orgs(id);


--
-- Name: orgs_x_slack_spaces orgs_x_slack_spaces_slack_space_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.orgs_x_slack_spaces
    ADD CONSTRAINT orgs_x_slack_spaces_slack_space_id_fkey FOREIGN KEY (slack_space_id) REFERENCES public.slack_spaces(id);


--
-- Name: positions positions_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.positions
    ADD CONSTRAINT positions_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.orgs(id);


--
-- Name: positions_x_orgs_x_users positions_x_orgs_x_users_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.positions_x_orgs_x_users
    ADD CONSTRAINT positions_x_orgs_x_users_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.orgs(id);


--
-- Name: positions_x_orgs_x_users positions_x_orgs_x_users_position_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.positions_x_orgs_x_users
    ADD CONSTRAINT positions_x_orgs_x_users_position_id_fkey FOREIGN KEY (position_id) REFERENCES public.positions(id);


--
-- Name: positions_x_orgs_x_users positions_x_orgs_x_users_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.positions_x_orgs_x_users
    ADD CONSTRAINT positions_x_orgs_x_users_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id);


--
-- Name: roles_x_api_keys_x_org roles_x_api_keys_x_org_api_key_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.roles_x_api_keys_x_org
    ADD CONSTRAINT roles_x_api_keys_x_org_api_key_id_fkey FOREIGN KEY (api_key_id) REFERENCES public.api_keys(id) ON DELETE CASCADE;


--
-- Name: roles_x_api_keys_x_org roles_x_api_keys_x_org_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.roles_x_api_keys_x_org
    ADD CONSTRAINT roles_x_api_keys_x_org_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.orgs(id);


--
-- Name: roles_x_api_keys_x_org roles_x_api_keys_x_org_role_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.roles_x_api_keys_x_org
    ADD CONSTRAINT roles_x_api_keys_x_org_role_id_fkey FOREIGN KEY (role_id) REFERENCES public.roles(id);


--
-- Name: roles_x_permissions roles_x_permissions_permission_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.roles_x_permissions
    ADD CONSTRAINT roles_x_permissions_permission_id_fkey FOREIGN KEY (permission_id) REFERENCES public.permissions(id);


--
-- Name: roles_x_permissions roles_x_permissions_role_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.roles_x_permissions
    ADD CONSTRAINT roles_x_permissions_role_id_fkey FOREIGN KEY (role_id) REFERENCES public.roles(id);


--
-- Name: roles_x_users_x_org roles_x_users_x_org_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.roles_x_users_x_org
    ADD CONSTRAINT roles_x_users_x_org_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.orgs(id);


--
-- Name: roles_x_users_x_org roles_x_users_x_org_role_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.roles_x_users_x_org
    ADD CONSTRAINT roles_x_users_x_org_role_id_fkey FOREIGN KEY (role_id) REFERENCES public.roles(id);


--
-- Name: roles_x_users_x_org roles_x_users_x_org_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.roles_x_users_x_org
    ADD CONSTRAINT roles_x_users_x_org_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id);


--
-- Name: slack_users slack_users_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.slack_users
    ADD CONSTRAINT slack_users_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id);


--
-- Name: update_requests update_requests_event_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.update_requests
    ADD CONSTRAINT update_requests_event_id_fkey FOREIGN KEY (event_id) REFERENCES public.events(id);


--
-- Name: update_requests update_requests_location_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.update_requests
    ADD CONSTRAINT update_requests_location_id_fkey FOREIGN KEY (location_id) REFERENCES public.locations(id);


--
-- Name: update_requests update_requests_region_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.update_requests
    ADD CONSTRAINT update_requests_region_id_fkey FOREIGN KEY (region_id) REFERENCES public.orgs(id);


--
-- Name: users users_home_region_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: f3slackbot
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_home_region_id_fkey FOREIGN KEY (home_region_id) REFERENCES public.orgs(id);


--
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: cloudsqlsuperuser
--

REVOKE USAGE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO cloudsqladmin;
GRANT USAGE ON SCHEMA public TO pg_database_owner;
GRANT USAGE ON SCHEMA public TO pg_read_all_data;
GRANT USAGE ON SCHEMA public TO pg_write_all_data;
GRANT USAGE ON SCHEMA public TO pg_monitor;
GRANT USAGE ON SCHEMA public TO pg_read_all_settings;
GRANT USAGE ON SCHEMA public TO pg_read_all_stats;
GRANT USAGE ON SCHEMA public TO pg_stat_scan_tables;
GRANT USAGE ON SCHEMA public TO pg_read_server_files;
GRANT USAGE ON SCHEMA public TO pg_write_server_files;
GRANT USAGE ON SCHEMA public TO pg_execute_server_program;
GRANT USAGE ON SCHEMA public TO pg_signal_backend;
GRANT USAGE ON SCHEMA public TO pg_checkpoint;
GRANT USAGE ON SCHEMA public TO pg_use_reserved_connections;
GRANT USAGE ON SCHEMA public TO pg_create_subscription;
GRANT USAGE ON SCHEMA public TO cloudsqlagent;
GRANT USAGE ON SCHEMA public TO cloudsqlimportexport;
GRANT USAGE ON SCHEMA public TO cloudsqlreplica;
GRANT USAGE ON SCHEMA public TO cloudsqlobservability;
GRANT USAGE ON SCHEMA public TO cloudsqliamserviceaccount;
GRANT USAGE ON SCHEMA public TO cloudsqliamgroup;
GRANT USAGE ON SCHEMA public TO cloudsqliamgroupuser;
GRANT USAGE ON SCHEMA public TO cloudsqliamuser;
GRANT USAGE ON SCHEMA public TO cloudsqllogical;
GRANT USAGE ON SCHEMA public TO cloudsqliamgroupserviceaccount;
GRANT USAGE ON SCHEMA public TO f3_test;
GRANT USAGE ON SCHEMA public TO map;
GRANT USAGE ON SCHEMA public TO cloudsqlconnpooladmin;
GRANT USAGE ON SCHEMA public TO group_readonly;
GRANT ALL ON SCHEMA public TO app_codex;
GRANT USAGE ON SCHEMA public TO datastream_user;
GRANT USAGE ON SCHEMA public TO app_auth;
GRANT USAGE ON SCHEMA public TO squatter;


--
-- Name: TABLE achievements; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.achievements TO group_readonly;
GRANT SELECT ON TABLE public.achievements TO datastream_user;
GRANT ALL ON TABLE public.achievements TO squatter;


--
-- Name: SEQUENCE achievements_id_seq; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT ALL ON SEQUENCE public.achievements_id_seq TO cloudsqladmin;
GRANT ALL ON SEQUENCE public.achievements_id_seq TO pg_monitor;
GRANT ALL ON SEQUENCE public.achievements_id_seq TO pg_read_all_settings;
GRANT ALL ON SEQUENCE public.achievements_id_seq TO pg_read_all_stats;
GRANT ALL ON SEQUENCE public.achievements_id_seq TO pg_stat_scan_tables;
GRANT ALL ON SEQUENCE public.achievements_id_seq TO pg_signal_backend;
GRANT ALL ON SEQUENCE public.achievements_id_seq TO pg_checkpoint;
GRANT ALL ON SEQUENCE public.achievements_id_seq TO pg_use_reserved_connections;
GRANT ALL ON SEQUENCE public.achievements_id_seq TO pg_read_server_files;
GRANT ALL ON SEQUENCE public.achievements_id_seq TO pg_write_server_files;
GRANT ALL ON SEQUENCE public.achievements_id_seq TO pg_execute_server_program;
GRANT ALL ON SEQUENCE public.achievements_id_seq TO pg_database_owner;
GRANT ALL ON SEQUENCE public.achievements_id_seq TO pg_read_all_data;
GRANT ALL ON SEQUENCE public.achievements_id_seq TO pg_write_all_data;
GRANT ALL ON SEQUENCE public.achievements_id_seq TO pg_create_subscription;
GRANT ALL ON SEQUENCE public.achievements_id_seq TO cloudsqlagent;
GRANT ALL ON SEQUENCE public.achievements_id_seq TO cloudsqlimportexport;
GRANT ALL ON SEQUENCE public.achievements_id_seq TO cloudsqlreplica;
GRANT ALL ON SEQUENCE public.achievements_id_seq TO cloudsqlobservability;
GRANT ALL ON SEQUENCE public.achievements_id_seq TO cloudsqliamserviceaccount;
GRANT ALL ON SEQUENCE public.achievements_id_seq TO cloudsqliamgroup;
GRANT ALL ON SEQUENCE public.achievements_id_seq TO cloudsqliamgroupuser;
GRANT ALL ON SEQUENCE public.achievements_id_seq TO cloudsqliamuser;
GRANT ALL ON SEQUENCE public.achievements_id_seq TO cloudsqllogical;
GRANT ALL ON SEQUENCE public.achievements_id_seq TO cloudsqliamgroupserviceaccount;
GRANT ALL ON SEQUENCE public.achievements_id_seq TO f3_test;
GRANT ALL ON SEQUENCE public.achievements_id_seq TO map;
GRANT ALL ON SEQUENCE public.achievements_id_seq TO cloudsqlconnpooladmin;
GRANT ALL ON SEQUENCE public.achievements_id_seq TO backslash;
GRANT SELECT ON SEQUENCE public.achievements_id_seq TO squatter;


--
-- Name: TABLE achievements_x_users; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements_x_users TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements_x_users TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements_x_users TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements_x_users TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements_x_users TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements_x_users TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements_x_users TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements_x_users TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements_x_users TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements_x_users TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements_x_users TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements_x_users TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements_x_users TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements_x_users TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements_x_users TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements_x_users TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements_x_users TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements_x_users TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements_x_users TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements_x_users TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements_x_users TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements_x_users TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements_x_users TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements_x_users TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements_x_users TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements_x_users TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements_x_users TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.achievements_x_users TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.achievements_x_users TO group_readonly;
GRANT SELECT ON TABLE public.achievements_x_users TO datastream_user;
GRANT ALL ON TABLE public.achievements_x_users TO squatter;


--
-- Name: TABLE alembic_version; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.alembic_version TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.alembic_version TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.alembic_version TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.alembic_version TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.alembic_version TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.alembic_version TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.alembic_version TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.alembic_version TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.alembic_version TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.alembic_version TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.alembic_version TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.alembic_version TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.alembic_version TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.alembic_version TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.alembic_version TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.alembic_version TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.alembic_version TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.alembic_version TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.alembic_version TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.alembic_version TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.alembic_version TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.alembic_version TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.alembic_version TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.alembic_version TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.alembic_version TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.alembic_version TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.alembic_version TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.alembic_version TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.alembic_version TO group_readonly;
GRANT ALL ON TABLE public.alembic_version TO squatter;


--
-- Name: TABLE api_keys; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.api_keys TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.api_keys TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.api_keys TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.api_keys TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.api_keys TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.api_keys TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.api_keys TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.api_keys TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.api_keys TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.api_keys TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.api_keys TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.api_keys TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.api_keys TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.api_keys TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.api_keys TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.api_keys TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.api_keys TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.api_keys TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.api_keys TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.api_keys TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.api_keys TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.api_keys TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.api_keys TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.api_keys TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.api_keys TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.api_keys TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.api_keys TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.api_keys TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.api_keys TO group_readonly;
GRANT ALL ON TABLE public.api_keys TO squatter;


--
-- Name: SEQUENCE api_keys_id_seq; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT ALL ON SEQUENCE public.api_keys_id_seq TO cloudsqladmin;
GRANT ALL ON SEQUENCE public.api_keys_id_seq TO pg_monitor;
GRANT ALL ON SEQUENCE public.api_keys_id_seq TO pg_read_all_settings;
GRANT ALL ON SEQUENCE public.api_keys_id_seq TO pg_read_all_stats;
GRANT ALL ON SEQUENCE public.api_keys_id_seq TO pg_stat_scan_tables;
GRANT ALL ON SEQUENCE public.api_keys_id_seq TO pg_signal_backend;
GRANT ALL ON SEQUENCE public.api_keys_id_seq TO pg_checkpoint;
GRANT ALL ON SEQUENCE public.api_keys_id_seq TO pg_use_reserved_connections;
GRANT ALL ON SEQUENCE public.api_keys_id_seq TO pg_read_server_files;
GRANT ALL ON SEQUENCE public.api_keys_id_seq TO pg_write_server_files;
GRANT ALL ON SEQUENCE public.api_keys_id_seq TO pg_execute_server_program;
GRANT ALL ON SEQUENCE public.api_keys_id_seq TO pg_database_owner;
GRANT ALL ON SEQUENCE public.api_keys_id_seq TO pg_read_all_data;
GRANT ALL ON SEQUENCE public.api_keys_id_seq TO pg_write_all_data;
GRANT ALL ON SEQUENCE public.api_keys_id_seq TO pg_create_subscription;
GRANT ALL ON SEQUENCE public.api_keys_id_seq TO cloudsqlagent;
GRANT ALL ON SEQUENCE public.api_keys_id_seq TO cloudsqlimportexport;
GRANT ALL ON SEQUENCE public.api_keys_id_seq TO cloudsqlreplica;
GRANT ALL ON SEQUENCE public.api_keys_id_seq TO cloudsqlobservability;
GRANT ALL ON SEQUENCE public.api_keys_id_seq TO cloudsqliamserviceaccount;
GRANT ALL ON SEQUENCE public.api_keys_id_seq TO cloudsqliamgroup;
GRANT ALL ON SEQUENCE public.api_keys_id_seq TO cloudsqliamgroupuser;
GRANT ALL ON SEQUENCE public.api_keys_id_seq TO cloudsqliamuser;
GRANT ALL ON SEQUENCE public.api_keys_id_seq TO cloudsqllogical;
GRANT ALL ON SEQUENCE public.api_keys_id_seq TO cloudsqliamgroupserviceaccount;
GRANT ALL ON SEQUENCE public.api_keys_id_seq TO f3_test;
GRANT ALL ON SEQUENCE public.api_keys_id_seq TO map;
GRANT ALL ON SEQUENCE public.api_keys_id_seq TO cloudsqlconnpooladmin;
GRANT SELECT ON SEQUENCE public.api_keys_id_seq TO squatter;


--
-- Name: TABLE attendance; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.attendance TO group_readonly;
GRANT SELECT ON TABLE public.attendance TO datastream_user;
GRANT ALL ON TABLE public.attendance TO squatter;


--
-- Name: TABLE attendance_types; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_types TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_types TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_types TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_types TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_types TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_types TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_types TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_types TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_types TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_types TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_types TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_types TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_types TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_types TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_types TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_types TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_types TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_types TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_types TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_types TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_types TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_types TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_types TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_types TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_types TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_types TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_types TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_types TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.attendance_types TO group_readonly;
GRANT SELECT ON TABLE public.attendance_types TO datastream_user;
GRANT ALL ON TABLE public.attendance_types TO squatter;


--
-- Name: TABLE attendance_x_attendance_types; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_x_attendance_types TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_x_attendance_types TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_x_attendance_types TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_x_attendance_types TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_x_attendance_types TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_x_attendance_types TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_x_attendance_types TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_x_attendance_types TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_x_attendance_types TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_x_attendance_types TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_x_attendance_types TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_x_attendance_types TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_x_attendance_types TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_x_attendance_types TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_x_attendance_types TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_x_attendance_types TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_x_attendance_types TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_x_attendance_types TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_x_attendance_types TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_x_attendance_types TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_x_attendance_types TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_x_attendance_types TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_x_attendance_types TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_x_attendance_types TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_x_attendance_types TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_x_attendance_types TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_x_attendance_types TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_x_attendance_types TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.attendance_x_attendance_types TO group_readonly;
GRANT SELECT ON TABLE public.attendance_x_attendance_types TO datastream_user;
GRANT ALL ON TABLE public.attendance_x_attendance_types TO squatter;


--
-- Name: TABLE event_instances; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.event_instances TO group_readonly;
GRANT SELECT ON TABLE public.event_instances TO datastream_user;
GRANT ALL ON TABLE public.event_instances TO squatter;


--
-- Name: TABLE orgs; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.orgs TO group_readonly;
GRANT SELECT ON TABLE public.orgs TO datastream_user;
GRANT ALL ON TABLE public.orgs TO squatter;


--
-- Name: TABLE users; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.users TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.users TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.users TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.users TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.users TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.users TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.users TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.users TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.users TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.users TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.users TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.users TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.users TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.users TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.users TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.users TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.users TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.users TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.users TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.users TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.users TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.users TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.users TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.users TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.users TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.users TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.users TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.users TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.users TO group_readonly;
GRANT SELECT ON TABLE public.users TO datastream_user;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.users TO app_auth;
GRANT ALL ON TABLE public.users TO squatter;


--
-- Name: TABLE attendance_expanded; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_expanded TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_expanded TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_expanded TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_expanded TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_expanded TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_expanded TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_expanded TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_expanded TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_expanded TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_expanded TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_expanded TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_expanded TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_expanded TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_expanded TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_expanded TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_expanded TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_expanded TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_expanded TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_expanded TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_expanded TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_expanded TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_expanded TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_expanded TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_expanded TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_expanded TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_expanded TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_expanded TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.attendance_expanded TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.attendance_expanded TO group_readonly;


--
-- Name: SEQUENCE attendance_id_seq; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT ALL ON SEQUENCE public.attendance_id_seq TO cloudsqladmin;
GRANT ALL ON SEQUENCE public.attendance_id_seq TO pg_monitor;
GRANT ALL ON SEQUENCE public.attendance_id_seq TO pg_read_all_settings;
GRANT ALL ON SEQUENCE public.attendance_id_seq TO pg_read_all_stats;
GRANT ALL ON SEQUENCE public.attendance_id_seq TO pg_stat_scan_tables;
GRANT ALL ON SEQUENCE public.attendance_id_seq TO pg_signal_backend;
GRANT ALL ON SEQUENCE public.attendance_id_seq TO pg_checkpoint;
GRANT ALL ON SEQUENCE public.attendance_id_seq TO pg_use_reserved_connections;
GRANT ALL ON SEQUENCE public.attendance_id_seq TO pg_read_server_files;
GRANT ALL ON SEQUENCE public.attendance_id_seq TO pg_write_server_files;
GRANT ALL ON SEQUENCE public.attendance_id_seq TO pg_execute_server_program;
GRANT ALL ON SEQUENCE public.attendance_id_seq TO pg_database_owner;
GRANT ALL ON SEQUENCE public.attendance_id_seq TO pg_read_all_data;
GRANT ALL ON SEQUENCE public.attendance_id_seq TO pg_write_all_data;
GRANT ALL ON SEQUENCE public.attendance_id_seq TO pg_create_subscription;
GRANT ALL ON SEQUENCE public.attendance_id_seq TO cloudsqlagent;
GRANT ALL ON SEQUENCE public.attendance_id_seq TO cloudsqlimportexport;
GRANT ALL ON SEQUENCE public.attendance_id_seq TO cloudsqlreplica;
GRANT ALL ON SEQUENCE public.attendance_id_seq TO cloudsqlobservability;
GRANT ALL ON SEQUENCE public.attendance_id_seq TO cloudsqliamserviceaccount;
GRANT ALL ON SEQUENCE public.attendance_id_seq TO cloudsqliamgroup;
GRANT ALL ON SEQUENCE public.attendance_id_seq TO cloudsqliamgroupuser;
GRANT ALL ON SEQUENCE public.attendance_id_seq TO cloudsqliamuser;
GRANT ALL ON SEQUENCE public.attendance_id_seq TO cloudsqllogical;
GRANT ALL ON SEQUENCE public.attendance_id_seq TO cloudsqliamgroupserviceaccount;
GRANT ALL ON SEQUENCE public.attendance_id_seq TO f3_test;
GRANT ALL ON SEQUENCE public.attendance_id_seq TO map;
GRANT ALL ON SEQUENCE public.attendance_id_seq TO cloudsqlconnpooladmin;
GRANT ALL ON SEQUENCE public.attendance_id_seq TO backslash;
GRANT SELECT ON SEQUENCE public.attendance_id_seq TO squatter;


--
-- Name: SEQUENCE attendance_types_id_seq; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT ALL ON SEQUENCE public.attendance_types_id_seq TO cloudsqladmin;
GRANT ALL ON SEQUENCE public.attendance_types_id_seq TO pg_monitor;
GRANT ALL ON SEQUENCE public.attendance_types_id_seq TO pg_read_all_settings;
GRANT ALL ON SEQUENCE public.attendance_types_id_seq TO pg_read_all_stats;
GRANT ALL ON SEQUENCE public.attendance_types_id_seq TO pg_stat_scan_tables;
GRANT ALL ON SEQUENCE public.attendance_types_id_seq TO pg_signal_backend;
GRANT ALL ON SEQUENCE public.attendance_types_id_seq TO pg_checkpoint;
GRANT ALL ON SEQUENCE public.attendance_types_id_seq TO pg_use_reserved_connections;
GRANT ALL ON SEQUENCE public.attendance_types_id_seq TO pg_read_server_files;
GRANT ALL ON SEQUENCE public.attendance_types_id_seq TO pg_write_server_files;
GRANT ALL ON SEQUENCE public.attendance_types_id_seq TO pg_execute_server_program;
GRANT ALL ON SEQUENCE public.attendance_types_id_seq TO pg_database_owner;
GRANT ALL ON SEQUENCE public.attendance_types_id_seq TO pg_read_all_data;
GRANT ALL ON SEQUENCE public.attendance_types_id_seq TO pg_write_all_data;
GRANT ALL ON SEQUENCE public.attendance_types_id_seq TO pg_create_subscription;
GRANT ALL ON SEQUENCE public.attendance_types_id_seq TO cloudsqlagent;
GRANT ALL ON SEQUENCE public.attendance_types_id_seq TO cloudsqlimportexport;
GRANT ALL ON SEQUENCE public.attendance_types_id_seq TO cloudsqlreplica;
GRANT ALL ON SEQUENCE public.attendance_types_id_seq TO cloudsqlobservability;
GRANT ALL ON SEQUENCE public.attendance_types_id_seq TO cloudsqliamserviceaccount;
GRANT ALL ON SEQUENCE public.attendance_types_id_seq TO cloudsqliamgroup;
GRANT ALL ON SEQUENCE public.attendance_types_id_seq TO cloudsqliamgroupuser;
GRANT ALL ON SEQUENCE public.attendance_types_id_seq TO cloudsqliamuser;
GRANT ALL ON SEQUENCE public.attendance_types_id_seq TO cloudsqllogical;
GRANT ALL ON SEQUENCE public.attendance_types_id_seq TO cloudsqliamgroupserviceaccount;
GRANT ALL ON SEQUENCE public.attendance_types_id_seq TO f3_test;
GRANT ALL ON SEQUENCE public.attendance_types_id_seq TO map;
GRANT ALL ON SEQUENCE public.attendance_types_id_seq TO cloudsqlconnpooladmin;
GRANT ALL ON SEQUENCE public.attendance_types_id_seq TO backslash;
GRANT SELECT ON SEQUENCE public.attendance_types_id_seq TO squatter;


--
-- Name: TABLE auth_accounts; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_accounts TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_accounts TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_accounts TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_accounts TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_accounts TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_accounts TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_accounts TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_accounts TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_accounts TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_accounts TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_accounts TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_accounts TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_accounts TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_accounts TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_accounts TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_accounts TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_accounts TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_accounts TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_accounts TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_accounts TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_accounts TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_accounts TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_accounts TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_accounts TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_accounts TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_accounts TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_accounts TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_accounts TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.auth_accounts TO group_readonly;
GRANT ALL ON TABLE public.auth_accounts TO squatter;


--
-- Name: TABLE auth_sessions; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_sessions TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_sessions TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_sessions TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_sessions TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_sessions TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_sessions TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_sessions TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_sessions TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_sessions TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_sessions TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_sessions TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_sessions TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_sessions TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_sessions TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_sessions TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_sessions TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_sessions TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_sessions TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_sessions TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_sessions TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_sessions TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_sessions TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_sessions TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_sessions TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_sessions TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_sessions TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_sessions TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_sessions TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.auth_sessions TO group_readonly;
GRANT ALL ON TABLE public.auth_sessions TO squatter;


--
-- Name: TABLE auth_verification_tokens; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_verification_tokens TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_verification_tokens TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_verification_tokens TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_verification_tokens TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_verification_tokens TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_verification_tokens TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_verification_tokens TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_verification_tokens TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_verification_tokens TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_verification_tokens TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_verification_tokens TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_verification_tokens TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_verification_tokens TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_verification_tokens TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_verification_tokens TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_verification_tokens TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_verification_tokens TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_verification_tokens TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_verification_tokens TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_verification_tokens TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_verification_tokens TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_verification_tokens TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_verification_tokens TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_verification_tokens TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_verification_tokens TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_verification_tokens TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_verification_tokens TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.auth_verification_tokens TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.auth_verification_tokens TO group_readonly;
GRANT ALL ON TABLE public.auth_verification_tokens TO squatter;


--
-- Name: TABLE event_instances_x_event_types; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances_x_event_types TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances_x_event_types TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances_x_event_types TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances_x_event_types TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances_x_event_types TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances_x_event_types TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances_x_event_types TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances_x_event_types TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances_x_event_types TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances_x_event_types TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances_x_event_types TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances_x_event_types TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances_x_event_types TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances_x_event_types TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances_x_event_types TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances_x_event_types TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances_x_event_types TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances_x_event_types TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances_x_event_types TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances_x_event_types TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances_x_event_types TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances_x_event_types TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances_x_event_types TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances_x_event_types TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances_x_event_types TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances_x_event_types TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances_x_event_types TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instances_x_event_types TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.event_instances_x_event_types TO group_readonly;
GRANT SELECT ON TABLE public.event_instances_x_event_types TO datastream_user;
GRANT ALL ON TABLE public.event_instances_x_event_types TO squatter;


--
-- Name: TABLE event_tags; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.event_tags TO group_readonly;
GRANT SELECT ON TABLE public.event_tags TO datastream_user;
GRANT ALL ON TABLE public.event_tags TO squatter;


--
-- Name: TABLE event_tags_x_event_instances; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_event_instances TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_event_instances TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_event_instances TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_event_instances TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_event_instances TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_event_instances TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_event_instances TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_event_instances TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_event_instances TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_event_instances TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_event_instances TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_event_instances TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_event_instances TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_event_instances TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_event_instances TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_event_instances TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_event_instances TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_event_instances TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_event_instances TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_event_instances TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_event_instances TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_event_instances TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_event_instances TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_event_instances TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_event_instances TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_event_instances TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_event_instances TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_event_instances TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.event_tags_x_event_instances TO group_readonly;
GRANT SELECT ON TABLE public.event_tags_x_event_instances TO datastream_user;
GRANT ALL ON TABLE public.event_tags_x_event_instances TO squatter;


--
-- Name: TABLE event_types; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_types TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_types TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_types TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_types TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_types TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_types TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_types TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_types TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_types TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_types TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_types TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_types TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_types TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_types TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_types TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_types TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_types TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_types TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_types TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_types TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_types TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_types TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_types TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_types TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_types TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_types TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_types TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_types TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.event_types TO group_readonly;
GRANT SELECT ON TABLE public.event_types TO datastream_user;
GRANT ALL ON TABLE public.event_types TO squatter;


--
-- Name: TABLE events; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.events TO group_readonly;
GRANT SELECT ON TABLE public.events TO datastream_user;
GRANT ALL ON TABLE public.events TO squatter;


--
-- Name: TABLE locations; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.locations TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.locations TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.locations TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.locations TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.locations TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.locations TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.locations TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.locations TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.locations TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.locations TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.locations TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.locations TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.locations TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.locations TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.locations TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.locations TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.locations TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.locations TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.locations TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.locations TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.locations TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.locations TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.locations TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.locations TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.locations TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.locations TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.locations TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.locations TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.locations TO group_readonly;
GRANT SELECT ON TABLE public.locations TO datastream_user;
GRANT ALL ON TABLE public.locations TO squatter;


--
-- Name: TABLE event_instance_expanded; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instance_expanded TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instance_expanded TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instance_expanded TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instance_expanded TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instance_expanded TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instance_expanded TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instance_expanded TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instance_expanded TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instance_expanded TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instance_expanded TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instance_expanded TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instance_expanded TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instance_expanded TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instance_expanded TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instance_expanded TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instance_expanded TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instance_expanded TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instance_expanded TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instance_expanded TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instance_expanded TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instance_expanded TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instance_expanded TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instance_expanded TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instance_expanded TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instance_expanded TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instance_expanded TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instance_expanded TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_instance_expanded TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.event_instance_expanded TO group_readonly;


--
-- Name: SEQUENCE event_instances_id_seq; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT ALL ON SEQUENCE public.event_instances_id_seq TO cloudsqladmin;
GRANT ALL ON SEQUENCE public.event_instances_id_seq TO pg_monitor;
GRANT ALL ON SEQUENCE public.event_instances_id_seq TO pg_read_all_settings;
GRANT ALL ON SEQUENCE public.event_instances_id_seq TO pg_read_all_stats;
GRANT ALL ON SEQUENCE public.event_instances_id_seq TO pg_stat_scan_tables;
GRANT ALL ON SEQUENCE public.event_instances_id_seq TO pg_signal_backend;
GRANT ALL ON SEQUENCE public.event_instances_id_seq TO pg_checkpoint;
GRANT ALL ON SEQUENCE public.event_instances_id_seq TO pg_use_reserved_connections;
GRANT ALL ON SEQUENCE public.event_instances_id_seq TO pg_read_server_files;
GRANT ALL ON SEQUENCE public.event_instances_id_seq TO pg_write_server_files;
GRANT ALL ON SEQUENCE public.event_instances_id_seq TO pg_execute_server_program;
GRANT ALL ON SEQUENCE public.event_instances_id_seq TO pg_database_owner;
GRANT ALL ON SEQUENCE public.event_instances_id_seq TO pg_read_all_data;
GRANT ALL ON SEQUENCE public.event_instances_id_seq TO pg_write_all_data;
GRANT ALL ON SEQUENCE public.event_instances_id_seq TO pg_create_subscription;
GRANT ALL ON SEQUENCE public.event_instances_id_seq TO cloudsqlagent;
GRANT ALL ON SEQUENCE public.event_instances_id_seq TO cloudsqlimportexport;
GRANT ALL ON SEQUENCE public.event_instances_id_seq TO cloudsqlreplica;
GRANT ALL ON SEQUENCE public.event_instances_id_seq TO cloudsqlobservability;
GRANT ALL ON SEQUENCE public.event_instances_id_seq TO cloudsqliamserviceaccount;
GRANT ALL ON SEQUENCE public.event_instances_id_seq TO cloudsqliamgroup;
GRANT ALL ON SEQUENCE public.event_instances_id_seq TO cloudsqliamgroupuser;
GRANT ALL ON SEQUENCE public.event_instances_id_seq TO cloudsqliamuser;
GRANT ALL ON SEQUENCE public.event_instances_id_seq TO cloudsqllogical;
GRANT ALL ON SEQUENCE public.event_instances_id_seq TO cloudsqliamgroupserviceaccount;
GRANT ALL ON SEQUENCE public.event_instances_id_seq TO f3_test;
GRANT ALL ON SEQUENCE public.event_instances_id_seq TO map;
GRANT ALL ON SEQUENCE public.event_instances_id_seq TO cloudsqlconnpooladmin;
GRANT ALL ON SEQUENCE public.event_instances_id_seq TO backslash;
GRANT SELECT ON SEQUENCE public.event_instances_id_seq TO squatter;


--
-- Name: SEQUENCE event_tags_id_seq; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT ALL ON SEQUENCE public.event_tags_id_seq TO cloudsqladmin;
GRANT ALL ON SEQUENCE public.event_tags_id_seq TO pg_monitor;
GRANT ALL ON SEQUENCE public.event_tags_id_seq TO pg_read_all_settings;
GRANT ALL ON SEQUENCE public.event_tags_id_seq TO pg_read_all_stats;
GRANT ALL ON SEQUENCE public.event_tags_id_seq TO pg_stat_scan_tables;
GRANT ALL ON SEQUENCE public.event_tags_id_seq TO pg_signal_backend;
GRANT ALL ON SEQUENCE public.event_tags_id_seq TO pg_checkpoint;
GRANT ALL ON SEQUENCE public.event_tags_id_seq TO pg_use_reserved_connections;
GRANT ALL ON SEQUENCE public.event_tags_id_seq TO pg_read_server_files;
GRANT ALL ON SEQUENCE public.event_tags_id_seq TO pg_write_server_files;
GRANT ALL ON SEQUENCE public.event_tags_id_seq TO pg_execute_server_program;
GRANT ALL ON SEQUENCE public.event_tags_id_seq TO pg_database_owner;
GRANT ALL ON SEQUENCE public.event_tags_id_seq TO pg_read_all_data;
GRANT ALL ON SEQUENCE public.event_tags_id_seq TO pg_write_all_data;
GRANT ALL ON SEQUENCE public.event_tags_id_seq TO pg_create_subscription;
GRANT ALL ON SEQUENCE public.event_tags_id_seq TO cloudsqlagent;
GRANT ALL ON SEQUENCE public.event_tags_id_seq TO cloudsqlimportexport;
GRANT ALL ON SEQUENCE public.event_tags_id_seq TO cloudsqlreplica;
GRANT ALL ON SEQUENCE public.event_tags_id_seq TO cloudsqlobservability;
GRANT ALL ON SEQUENCE public.event_tags_id_seq TO cloudsqliamserviceaccount;
GRANT ALL ON SEQUENCE public.event_tags_id_seq TO cloudsqliamgroup;
GRANT ALL ON SEQUENCE public.event_tags_id_seq TO cloudsqliamgroupuser;
GRANT ALL ON SEQUENCE public.event_tags_id_seq TO cloudsqliamuser;
GRANT ALL ON SEQUENCE public.event_tags_id_seq TO cloudsqllogical;
GRANT ALL ON SEQUENCE public.event_tags_id_seq TO cloudsqliamgroupserviceaccount;
GRANT ALL ON SEQUENCE public.event_tags_id_seq TO f3_test;
GRANT ALL ON SEQUENCE public.event_tags_id_seq TO map;
GRANT ALL ON SEQUENCE public.event_tags_id_seq TO cloudsqlconnpooladmin;
GRANT ALL ON SEQUENCE public.event_tags_id_seq TO backslash;
GRANT SELECT ON SEQUENCE public.event_tags_id_seq TO squatter;


--
-- Name: TABLE event_tags_x_events; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_events TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_events TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_events TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_events TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_events TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_events TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_events TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_events TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_events TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_events TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_events TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_events TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_events TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_events TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_events TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_events TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_events TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_events TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_events TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_events TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_events TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_events TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_events TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_events TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_events TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_events TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_events TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.event_tags_x_events TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.event_tags_x_events TO group_readonly;
GRANT SELECT ON TABLE public.event_tags_x_events TO datastream_user;
GRANT ALL ON TABLE public.event_tags_x_events TO squatter;


--
-- Name: SEQUENCE event_types_id_seq; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT ALL ON SEQUENCE public.event_types_id_seq TO cloudsqladmin;
GRANT ALL ON SEQUENCE public.event_types_id_seq TO pg_monitor;
GRANT ALL ON SEQUENCE public.event_types_id_seq TO pg_read_all_settings;
GRANT ALL ON SEQUENCE public.event_types_id_seq TO pg_read_all_stats;
GRANT ALL ON SEQUENCE public.event_types_id_seq TO pg_stat_scan_tables;
GRANT ALL ON SEQUENCE public.event_types_id_seq TO pg_signal_backend;
GRANT ALL ON SEQUENCE public.event_types_id_seq TO pg_checkpoint;
GRANT ALL ON SEQUENCE public.event_types_id_seq TO pg_use_reserved_connections;
GRANT ALL ON SEQUENCE public.event_types_id_seq TO pg_read_server_files;
GRANT ALL ON SEQUENCE public.event_types_id_seq TO pg_write_server_files;
GRANT ALL ON SEQUENCE public.event_types_id_seq TO pg_execute_server_program;
GRANT ALL ON SEQUENCE public.event_types_id_seq TO pg_database_owner;
GRANT ALL ON SEQUENCE public.event_types_id_seq TO pg_read_all_data;
GRANT ALL ON SEQUENCE public.event_types_id_seq TO pg_write_all_data;
GRANT ALL ON SEQUENCE public.event_types_id_seq TO pg_create_subscription;
GRANT ALL ON SEQUENCE public.event_types_id_seq TO cloudsqlagent;
GRANT ALL ON SEQUENCE public.event_types_id_seq TO cloudsqlimportexport;
GRANT ALL ON SEQUENCE public.event_types_id_seq TO cloudsqlreplica;
GRANT ALL ON SEQUENCE public.event_types_id_seq TO cloudsqlobservability;
GRANT ALL ON SEQUENCE public.event_types_id_seq TO cloudsqliamserviceaccount;
GRANT ALL ON SEQUENCE public.event_types_id_seq TO cloudsqliamgroup;
GRANT ALL ON SEQUENCE public.event_types_id_seq TO cloudsqliamgroupuser;
GRANT ALL ON SEQUENCE public.event_types_id_seq TO cloudsqliamuser;
GRANT ALL ON SEQUENCE public.event_types_id_seq TO cloudsqllogical;
GRANT ALL ON SEQUENCE public.event_types_id_seq TO cloudsqliamgroupserviceaccount;
GRANT ALL ON SEQUENCE public.event_types_id_seq TO f3_test;
GRANT ALL ON SEQUENCE public.event_types_id_seq TO map;
GRANT ALL ON SEQUENCE public.event_types_id_seq TO cloudsqlconnpooladmin;
GRANT ALL ON SEQUENCE public.event_types_id_seq TO backslash;
GRANT SELECT ON SEQUENCE public.event_types_id_seq TO squatter;


--
-- Name: SEQUENCE events_id_seq; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT ALL ON SEQUENCE public.events_id_seq TO cloudsqladmin;
GRANT ALL ON SEQUENCE public.events_id_seq TO pg_monitor;
GRANT ALL ON SEQUENCE public.events_id_seq TO pg_read_all_settings;
GRANT ALL ON SEQUENCE public.events_id_seq TO pg_read_all_stats;
GRANT ALL ON SEQUENCE public.events_id_seq TO pg_stat_scan_tables;
GRANT ALL ON SEQUENCE public.events_id_seq TO pg_signal_backend;
GRANT ALL ON SEQUENCE public.events_id_seq TO pg_checkpoint;
GRANT ALL ON SEQUENCE public.events_id_seq TO pg_use_reserved_connections;
GRANT ALL ON SEQUENCE public.events_id_seq TO pg_read_server_files;
GRANT ALL ON SEQUENCE public.events_id_seq TO pg_write_server_files;
GRANT ALL ON SEQUENCE public.events_id_seq TO pg_execute_server_program;
GRANT ALL ON SEQUENCE public.events_id_seq TO pg_database_owner;
GRANT ALL ON SEQUENCE public.events_id_seq TO pg_read_all_data;
GRANT ALL ON SEQUENCE public.events_id_seq TO pg_write_all_data;
GRANT ALL ON SEQUENCE public.events_id_seq TO pg_create_subscription;
GRANT ALL ON SEQUENCE public.events_id_seq TO cloudsqlagent;
GRANT ALL ON SEQUENCE public.events_id_seq TO cloudsqlimportexport;
GRANT ALL ON SEQUENCE public.events_id_seq TO cloudsqlreplica;
GRANT ALL ON SEQUENCE public.events_id_seq TO cloudsqlobservability;
GRANT ALL ON SEQUENCE public.events_id_seq TO cloudsqliamserviceaccount;
GRANT ALL ON SEQUENCE public.events_id_seq TO cloudsqliamgroup;
GRANT ALL ON SEQUENCE public.events_id_seq TO cloudsqliamgroupuser;
GRANT ALL ON SEQUENCE public.events_id_seq TO cloudsqliamuser;
GRANT ALL ON SEQUENCE public.events_id_seq TO cloudsqllogical;
GRANT ALL ON SEQUENCE public.events_id_seq TO cloudsqliamgroupserviceaccount;
GRANT ALL ON SEQUENCE public.events_id_seq TO f3_test;
GRANT ALL ON SEQUENCE public.events_id_seq TO map;
GRANT ALL ON SEQUENCE public.events_id_seq TO cloudsqlconnpooladmin;
GRANT ALL ON SEQUENCE public.events_id_seq TO backslash;
GRANT SELECT ON SEQUENCE public.events_id_seq TO squatter;


--
-- Name: TABLE events_x_event_types; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events_x_event_types TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events_x_event_types TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events_x_event_types TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events_x_event_types TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events_x_event_types TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events_x_event_types TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events_x_event_types TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events_x_event_types TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events_x_event_types TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events_x_event_types TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events_x_event_types TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events_x_event_types TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events_x_event_types TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events_x_event_types TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events_x_event_types TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events_x_event_types TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events_x_event_types TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events_x_event_types TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events_x_event_types TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events_x_event_types TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events_x_event_types TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events_x_event_types TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events_x_event_types TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events_x_event_types TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events_x_event_types TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events_x_event_types TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events_x_event_types TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.events_x_event_types TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.events_x_event_types TO group_readonly;
GRANT SELECT ON TABLE public.events_x_event_types TO datastream_user;
GRANT ALL ON TABLE public.events_x_event_types TO squatter;


--
-- Name: TABLE expansions; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.expansions TO group_readonly;
GRANT ALL ON TABLE public.expansions TO squatter;


--
-- Name: SEQUENCE expansions_id_seq; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT ALL ON SEQUENCE public.expansions_id_seq TO cloudsqladmin;
GRANT ALL ON SEQUENCE public.expansions_id_seq TO pg_monitor;
GRANT ALL ON SEQUENCE public.expansions_id_seq TO pg_read_all_settings;
GRANT ALL ON SEQUENCE public.expansions_id_seq TO pg_read_all_stats;
GRANT ALL ON SEQUENCE public.expansions_id_seq TO pg_stat_scan_tables;
GRANT ALL ON SEQUENCE public.expansions_id_seq TO pg_signal_backend;
GRANT ALL ON SEQUENCE public.expansions_id_seq TO pg_checkpoint;
GRANT ALL ON SEQUENCE public.expansions_id_seq TO pg_use_reserved_connections;
GRANT ALL ON SEQUENCE public.expansions_id_seq TO pg_read_server_files;
GRANT ALL ON SEQUENCE public.expansions_id_seq TO pg_write_server_files;
GRANT ALL ON SEQUENCE public.expansions_id_seq TO pg_execute_server_program;
GRANT ALL ON SEQUENCE public.expansions_id_seq TO pg_database_owner;
GRANT ALL ON SEQUENCE public.expansions_id_seq TO pg_read_all_data;
GRANT ALL ON SEQUENCE public.expansions_id_seq TO pg_write_all_data;
GRANT ALL ON SEQUENCE public.expansions_id_seq TO pg_create_subscription;
GRANT ALL ON SEQUENCE public.expansions_id_seq TO cloudsqlagent;
GRANT ALL ON SEQUENCE public.expansions_id_seq TO cloudsqlimportexport;
GRANT ALL ON SEQUENCE public.expansions_id_seq TO cloudsqlreplica;
GRANT ALL ON SEQUENCE public.expansions_id_seq TO cloudsqlobservability;
GRANT ALL ON SEQUENCE public.expansions_id_seq TO cloudsqliamserviceaccount;
GRANT ALL ON SEQUENCE public.expansions_id_seq TO cloudsqliamgroup;
GRANT ALL ON SEQUENCE public.expansions_id_seq TO cloudsqliamgroupuser;
GRANT ALL ON SEQUENCE public.expansions_id_seq TO cloudsqliamuser;
GRANT ALL ON SEQUENCE public.expansions_id_seq TO cloudsqllogical;
GRANT ALL ON SEQUENCE public.expansions_id_seq TO cloudsqliamgroupserviceaccount;
GRANT ALL ON SEQUENCE public.expansions_id_seq TO f3_test;
GRANT ALL ON SEQUENCE public.expansions_id_seq TO map;
GRANT ALL ON SEQUENCE public.expansions_id_seq TO cloudsqlconnpooladmin;
GRANT ALL ON SEQUENCE public.expansions_id_seq TO backslash;
GRANT SELECT ON SEQUENCE public.expansions_id_seq TO squatter;


--
-- Name: TABLE expansions_x_users; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions_x_users TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions_x_users TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions_x_users TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions_x_users TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions_x_users TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions_x_users TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions_x_users TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions_x_users TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions_x_users TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions_x_users TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions_x_users TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions_x_users TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions_x_users TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions_x_users TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions_x_users TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions_x_users TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions_x_users TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions_x_users TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions_x_users TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions_x_users TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions_x_users TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions_x_users TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions_x_users TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions_x_users TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions_x_users TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions_x_users TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions_x_users TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.expansions_x_users TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.expansions_x_users TO group_readonly;
GRANT ALL ON TABLE public.expansions_x_users TO squatter;


--
-- Name: SEQUENCE locations_id_seq; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT ALL ON SEQUENCE public.locations_id_seq TO cloudsqladmin;
GRANT ALL ON SEQUENCE public.locations_id_seq TO pg_monitor;
GRANT ALL ON SEQUENCE public.locations_id_seq TO pg_read_all_settings;
GRANT ALL ON SEQUENCE public.locations_id_seq TO pg_read_all_stats;
GRANT ALL ON SEQUENCE public.locations_id_seq TO pg_stat_scan_tables;
GRANT ALL ON SEQUENCE public.locations_id_seq TO pg_signal_backend;
GRANT ALL ON SEQUENCE public.locations_id_seq TO pg_checkpoint;
GRANT ALL ON SEQUENCE public.locations_id_seq TO pg_use_reserved_connections;
GRANT ALL ON SEQUENCE public.locations_id_seq TO pg_read_server_files;
GRANT ALL ON SEQUENCE public.locations_id_seq TO pg_write_server_files;
GRANT ALL ON SEQUENCE public.locations_id_seq TO pg_execute_server_program;
GRANT ALL ON SEQUENCE public.locations_id_seq TO pg_database_owner;
GRANT ALL ON SEQUENCE public.locations_id_seq TO pg_read_all_data;
GRANT ALL ON SEQUENCE public.locations_id_seq TO pg_write_all_data;
GRANT ALL ON SEQUENCE public.locations_id_seq TO pg_create_subscription;
GRANT ALL ON SEQUENCE public.locations_id_seq TO cloudsqlagent;
GRANT ALL ON SEQUENCE public.locations_id_seq TO cloudsqlimportexport;
GRANT ALL ON SEQUENCE public.locations_id_seq TO cloudsqlreplica;
GRANT ALL ON SEQUENCE public.locations_id_seq TO cloudsqlobservability;
GRANT ALL ON SEQUENCE public.locations_id_seq TO cloudsqliamserviceaccount;
GRANT ALL ON SEQUENCE public.locations_id_seq TO cloudsqliamgroup;
GRANT ALL ON SEQUENCE public.locations_id_seq TO cloudsqliamgroupuser;
GRANT ALL ON SEQUENCE public.locations_id_seq TO cloudsqliamuser;
GRANT ALL ON SEQUENCE public.locations_id_seq TO cloudsqllogical;
GRANT ALL ON SEQUENCE public.locations_id_seq TO cloudsqliamgroupserviceaccount;
GRANT ALL ON SEQUENCE public.locations_id_seq TO f3_test;
GRANT ALL ON SEQUENCE public.locations_id_seq TO map;
GRANT ALL ON SEQUENCE public.locations_id_seq TO cloudsqlconnpooladmin;
GRANT ALL ON SEQUENCE public.locations_id_seq TO backslash;
GRANT SELECT ON SEQUENCE public.locations_id_seq TO squatter;


--
-- Name: TABLE materializedviews; Type: ACL; Schema: public; Owner: app_materializedviewrefresher
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.materializedviews TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.materializedviews TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.materializedviews TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.materializedviews TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.materializedviews TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.materializedviews TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.materializedviews TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.materializedviews TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.materializedviews TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.materializedviews TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.materializedviews TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.materializedviews TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.materializedviews TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.materializedviews TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.materializedviews TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.materializedviews TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.materializedviews TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.materializedviews TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.materializedviews TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.materializedviews TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.materializedviews TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.materializedviews TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.materializedviews TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.materializedviews TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.materializedviews TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.materializedviews TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.materializedviews TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.materializedviews TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.materializedviews TO group_readonly;
GRANT ALL ON TABLE public.materializedviews TO squatter;


--
-- Name: SEQUENCE orgs_id_seq; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT ALL ON SEQUENCE public.orgs_id_seq TO cloudsqladmin;
GRANT ALL ON SEQUENCE public.orgs_id_seq TO pg_monitor;
GRANT ALL ON SEQUENCE public.orgs_id_seq TO pg_read_all_settings;
GRANT ALL ON SEQUENCE public.orgs_id_seq TO pg_read_all_stats;
GRANT ALL ON SEQUENCE public.orgs_id_seq TO pg_stat_scan_tables;
GRANT ALL ON SEQUENCE public.orgs_id_seq TO pg_signal_backend;
GRANT ALL ON SEQUENCE public.orgs_id_seq TO pg_checkpoint;
GRANT ALL ON SEQUENCE public.orgs_id_seq TO pg_use_reserved_connections;
GRANT ALL ON SEQUENCE public.orgs_id_seq TO pg_read_server_files;
GRANT ALL ON SEQUENCE public.orgs_id_seq TO pg_write_server_files;
GRANT ALL ON SEQUENCE public.orgs_id_seq TO pg_execute_server_program;
GRANT ALL ON SEQUENCE public.orgs_id_seq TO pg_database_owner;
GRANT ALL ON SEQUENCE public.orgs_id_seq TO pg_read_all_data;
GRANT ALL ON SEQUENCE public.orgs_id_seq TO pg_write_all_data;
GRANT ALL ON SEQUENCE public.orgs_id_seq TO pg_create_subscription;
GRANT ALL ON SEQUENCE public.orgs_id_seq TO cloudsqlagent;
GRANT ALL ON SEQUENCE public.orgs_id_seq TO cloudsqlimportexport;
GRANT ALL ON SEQUENCE public.orgs_id_seq TO cloudsqlreplica;
GRANT ALL ON SEQUENCE public.orgs_id_seq TO cloudsqlobservability;
GRANT ALL ON SEQUENCE public.orgs_id_seq TO cloudsqliamserviceaccount;
GRANT ALL ON SEQUENCE public.orgs_id_seq TO cloudsqliamgroup;
GRANT ALL ON SEQUENCE public.orgs_id_seq TO cloudsqliamgroupuser;
GRANT ALL ON SEQUENCE public.orgs_id_seq TO cloudsqliamuser;
GRANT ALL ON SEQUENCE public.orgs_id_seq TO cloudsqllogical;
GRANT ALL ON SEQUENCE public.orgs_id_seq TO cloudsqliamgroupserviceaccount;
GRANT ALL ON SEQUENCE public.orgs_id_seq TO f3_test;
GRANT ALL ON SEQUENCE public.orgs_id_seq TO map;
GRANT ALL ON SEQUENCE public.orgs_id_seq TO cloudsqlconnpooladmin;
GRANT ALL ON SEQUENCE public.orgs_id_seq TO backslash;
GRANT SELECT ON SEQUENCE public.orgs_id_seq TO squatter;


--
-- Name: TABLE orgs_x_slack_spaces; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs_x_slack_spaces TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs_x_slack_spaces TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs_x_slack_spaces TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs_x_slack_spaces TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs_x_slack_spaces TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs_x_slack_spaces TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs_x_slack_spaces TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs_x_slack_spaces TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs_x_slack_spaces TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs_x_slack_spaces TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs_x_slack_spaces TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs_x_slack_spaces TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs_x_slack_spaces TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs_x_slack_spaces TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs_x_slack_spaces TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs_x_slack_spaces TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs_x_slack_spaces TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs_x_slack_spaces TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs_x_slack_spaces TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs_x_slack_spaces TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs_x_slack_spaces TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs_x_slack_spaces TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs_x_slack_spaces TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs_x_slack_spaces TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs_x_slack_spaces TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs_x_slack_spaces TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs_x_slack_spaces TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.orgs_x_slack_spaces TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.orgs_x_slack_spaces TO group_readonly;
GRANT SELECT ON TABLE public.orgs_x_slack_spaces TO datastream_user;
GRANT ALL ON TABLE public.orgs_x_slack_spaces TO squatter;


--
-- Name: TABLE permissions; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.permissions TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.permissions TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.permissions TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.permissions TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.permissions TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.permissions TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.permissions TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.permissions TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.permissions TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.permissions TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.permissions TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.permissions TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.permissions TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.permissions TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.permissions TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.permissions TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.permissions TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.permissions TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.permissions TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.permissions TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.permissions TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.permissions TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.permissions TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.permissions TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.permissions TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.permissions TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.permissions TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.permissions TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.permissions TO group_readonly;
GRANT SELECT ON TABLE public.permissions TO datastream_user;
GRANT ALL ON TABLE public.permissions TO squatter;


--
-- Name: SEQUENCE permissions_id_seq; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT ALL ON SEQUENCE public.permissions_id_seq TO cloudsqladmin;
GRANT ALL ON SEQUENCE public.permissions_id_seq TO pg_monitor;
GRANT ALL ON SEQUENCE public.permissions_id_seq TO pg_read_all_settings;
GRANT ALL ON SEQUENCE public.permissions_id_seq TO pg_read_all_stats;
GRANT ALL ON SEQUENCE public.permissions_id_seq TO pg_stat_scan_tables;
GRANT ALL ON SEQUENCE public.permissions_id_seq TO pg_signal_backend;
GRANT ALL ON SEQUENCE public.permissions_id_seq TO pg_checkpoint;
GRANT ALL ON SEQUENCE public.permissions_id_seq TO pg_use_reserved_connections;
GRANT ALL ON SEQUENCE public.permissions_id_seq TO pg_read_server_files;
GRANT ALL ON SEQUENCE public.permissions_id_seq TO pg_write_server_files;
GRANT ALL ON SEQUENCE public.permissions_id_seq TO pg_execute_server_program;
GRANT ALL ON SEQUENCE public.permissions_id_seq TO pg_database_owner;
GRANT ALL ON SEQUENCE public.permissions_id_seq TO pg_read_all_data;
GRANT ALL ON SEQUENCE public.permissions_id_seq TO pg_write_all_data;
GRANT ALL ON SEQUENCE public.permissions_id_seq TO pg_create_subscription;
GRANT ALL ON SEQUENCE public.permissions_id_seq TO cloudsqlagent;
GRANT ALL ON SEQUENCE public.permissions_id_seq TO cloudsqlimportexport;
GRANT ALL ON SEQUENCE public.permissions_id_seq TO cloudsqlreplica;
GRANT ALL ON SEQUENCE public.permissions_id_seq TO cloudsqlobservability;
GRANT ALL ON SEQUENCE public.permissions_id_seq TO cloudsqliamserviceaccount;
GRANT ALL ON SEQUENCE public.permissions_id_seq TO cloudsqliamgroup;
GRANT ALL ON SEQUENCE public.permissions_id_seq TO cloudsqliamgroupuser;
GRANT ALL ON SEQUENCE public.permissions_id_seq TO cloudsqliamuser;
GRANT ALL ON SEQUENCE public.permissions_id_seq TO cloudsqllogical;
GRANT ALL ON SEQUENCE public.permissions_id_seq TO cloudsqliamgroupserviceaccount;
GRANT ALL ON SEQUENCE public.permissions_id_seq TO f3_test;
GRANT ALL ON SEQUENCE public.permissions_id_seq TO map;
GRANT ALL ON SEQUENCE public.permissions_id_seq TO cloudsqlconnpooladmin;
GRANT ALL ON SEQUENCE public.permissions_id_seq TO backslash;
GRANT SELECT ON SEQUENCE public.permissions_id_seq TO squatter;


--
-- Name: TABLE positions; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.positions TO group_readonly;
GRANT SELECT ON TABLE public.positions TO datastream_user;
GRANT ALL ON TABLE public.positions TO squatter;


--
-- Name: SEQUENCE positions_id_seq; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT ALL ON SEQUENCE public.positions_id_seq TO cloudsqladmin;
GRANT ALL ON SEQUENCE public.positions_id_seq TO pg_monitor;
GRANT ALL ON SEQUENCE public.positions_id_seq TO pg_read_all_settings;
GRANT ALL ON SEQUENCE public.positions_id_seq TO pg_read_all_stats;
GRANT ALL ON SEQUENCE public.positions_id_seq TO pg_stat_scan_tables;
GRANT ALL ON SEQUENCE public.positions_id_seq TO pg_signal_backend;
GRANT ALL ON SEQUENCE public.positions_id_seq TO pg_checkpoint;
GRANT ALL ON SEQUENCE public.positions_id_seq TO pg_use_reserved_connections;
GRANT ALL ON SEQUENCE public.positions_id_seq TO pg_read_server_files;
GRANT ALL ON SEQUENCE public.positions_id_seq TO pg_write_server_files;
GRANT ALL ON SEQUENCE public.positions_id_seq TO pg_execute_server_program;
GRANT ALL ON SEQUENCE public.positions_id_seq TO pg_database_owner;
GRANT ALL ON SEQUENCE public.positions_id_seq TO pg_read_all_data;
GRANT ALL ON SEQUENCE public.positions_id_seq TO pg_write_all_data;
GRANT ALL ON SEQUENCE public.positions_id_seq TO pg_create_subscription;
GRANT ALL ON SEQUENCE public.positions_id_seq TO cloudsqlagent;
GRANT ALL ON SEQUENCE public.positions_id_seq TO cloudsqlimportexport;
GRANT ALL ON SEQUENCE public.positions_id_seq TO cloudsqlreplica;
GRANT ALL ON SEQUENCE public.positions_id_seq TO cloudsqlobservability;
GRANT ALL ON SEQUENCE public.positions_id_seq TO cloudsqliamserviceaccount;
GRANT ALL ON SEQUENCE public.positions_id_seq TO cloudsqliamgroup;
GRANT ALL ON SEQUENCE public.positions_id_seq TO cloudsqliamgroupuser;
GRANT ALL ON SEQUENCE public.positions_id_seq TO cloudsqliamuser;
GRANT ALL ON SEQUENCE public.positions_id_seq TO cloudsqllogical;
GRANT ALL ON SEQUENCE public.positions_id_seq TO cloudsqliamgroupserviceaccount;
GRANT ALL ON SEQUENCE public.positions_id_seq TO f3_test;
GRANT ALL ON SEQUENCE public.positions_id_seq TO map;
GRANT ALL ON SEQUENCE public.positions_id_seq TO cloudsqlconnpooladmin;
GRANT ALL ON SEQUENCE public.positions_id_seq TO backslash;
GRANT SELECT ON SEQUENCE public.positions_id_seq TO squatter;


--
-- Name: TABLE positions_x_orgs_x_users; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions_x_orgs_x_users TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions_x_orgs_x_users TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions_x_orgs_x_users TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions_x_orgs_x_users TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions_x_orgs_x_users TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions_x_orgs_x_users TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions_x_orgs_x_users TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions_x_orgs_x_users TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions_x_orgs_x_users TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions_x_orgs_x_users TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions_x_orgs_x_users TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions_x_orgs_x_users TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions_x_orgs_x_users TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions_x_orgs_x_users TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions_x_orgs_x_users TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions_x_orgs_x_users TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions_x_orgs_x_users TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions_x_orgs_x_users TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions_x_orgs_x_users TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions_x_orgs_x_users TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions_x_orgs_x_users TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions_x_orgs_x_users TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions_x_orgs_x_users TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions_x_orgs_x_users TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions_x_orgs_x_users TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions_x_orgs_x_users TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions_x_orgs_x_users TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.positions_x_orgs_x_users TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.positions_x_orgs_x_users TO group_readonly;
GRANT SELECT ON TABLE public.positions_x_orgs_x_users TO datastream_user;
GRANT ALL ON TABLE public.positions_x_orgs_x_users TO squatter;


--
-- Name: TABLE roles; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.roles TO group_readonly;
GRANT SELECT ON TABLE public.roles TO datastream_user;
GRANT ALL ON TABLE public.roles TO squatter;


--
-- Name: SEQUENCE roles_id_seq; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT ALL ON SEQUENCE public.roles_id_seq TO cloudsqladmin;
GRANT ALL ON SEQUENCE public.roles_id_seq TO pg_monitor;
GRANT ALL ON SEQUENCE public.roles_id_seq TO pg_read_all_settings;
GRANT ALL ON SEQUENCE public.roles_id_seq TO pg_read_all_stats;
GRANT ALL ON SEQUENCE public.roles_id_seq TO pg_stat_scan_tables;
GRANT ALL ON SEQUENCE public.roles_id_seq TO pg_signal_backend;
GRANT ALL ON SEQUENCE public.roles_id_seq TO pg_checkpoint;
GRANT ALL ON SEQUENCE public.roles_id_seq TO pg_use_reserved_connections;
GRANT ALL ON SEQUENCE public.roles_id_seq TO pg_read_server_files;
GRANT ALL ON SEQUENCE public.roles_id_seq TO pg_write_server_files;
GRANT ALL ON SEQUENCE public.roles_id_seq TO pg_execute_server_program;
GRANT ALL ON SEQUENCE public.roles_id_seq TO pg_database_owner;
GRANT ALL ON SEQUENCE public.roles_id_seq TO pg_read_all_data;
GRANT ALL ON SEQUENCE public.roles_id_seq TO pg_write_all_data;
GRANT ALL ON SEQUENCE public.roles_id_seq TO pg_create_subscription;
GRANT ALL ON SEQUENCE public.roles_id_seq TO cloudsqlagent;
GRANT ALL ON SEQUENCE public.roles_id_seq TO cloudsqlimportexport;
GRANT ALL ON SEQUENCE public.roles_id_seq TO cloudsqlreplica;
GRANT ALL ON SEQUENCE public.roles_id_seq TO cloudsqlobservability;
GRANT ALL ON SEQUENCE public.roles_id_seq TO cloudsqliamserviceaccount;
GRANT ALL ON SEQUENCE public.roles_id_seq TO cloudsqliamgroup;
GRANT ALL ON SEQUENCE public.roles_id_seq TO cloudsqliamgroupuser;
GRANT ALL ON SEQUENCE public.roles_id_seq TO cloudsqliamuser;
GRANT ALL ON SEQUENCE public.roles_id_seq TO cloudsqllogical;
GRANT ALL ON SEQUENCE public.roles_id_seq TO cloudsqliamgroupserviceaccount;
GRANT ALL ON SEQUENCE public.roles_id_seq TO f3_test;
GRANT ALL ON SEQUENCE public.roles_id_seq TO map;
GRANT ALL ON SEQUENCE public.roles_id_seq TO cloudsqlconnpooladmin;
GRANT ALL ON SEQUENCE public.roles_id_seq TO backslash;
GRANT SELECT ON SEQUENCE public.roles_id_seq TO squatter;


--
-- Name: TABLE roles_x_api_keys_x_org; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_api_keys_x_org TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_api_keys_x_org TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_api_keys_x_org TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_api_keys_x_org TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_api_keys_x_org TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_api_keys_x_org TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_api_keys_x_org TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_api_keys_x_org TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_api_keys_x_org TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_api_keys_x_org TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_api_keys_x_org TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_api_keys_x_org TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_api_keys_x_org TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_api_keys_x_org TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_api_keys_x_org TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_api_keys_x_org TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_api_keys_x_org TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_api_keys_x_org TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_api_keys_x_org TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_api_keys_x_org TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_api_keys_x_org TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_api_keys_x_org TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_api_keys_x_org TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_api_keys_x_org TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_api_keys_x_org TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_api_keys_x_org TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_api_keys_x_org TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_api_keys_x_org TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.roles_x_api_keys_x_org TO group_readonly;
GRANT ALL ON TABLE public.roles_x_api_keys_x_org TO squatter;


--
-- Name: TABLE roles_x_permissions; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_permissions TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_permissions TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_permissions TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_permissions TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_permissions TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_permissions TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_permissions TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_permissions TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_permissions TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_permissions TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_permissions TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_permissions TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_permissions TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_permissions TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_permissions TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_permissions TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_permissions TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_permissions TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_permissions TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_permissions TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_permissions TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_permissions TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_permissions TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_permissions TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_permissions TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_permissions TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_permissions TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_permissions TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.roles_x_permissions TO group_readonly;
GRANT SELECT ON TABLE public.roles_x_permissions TO datastream_user;
GRANT ALL ON TABLE public.roles_x_permissions TO squatter;


--
-- Name: TABLE roles_x_users_x_org; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_users_x_org TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_users_x_org TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_users_x_org TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_users_x_org TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_users_x_org TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_users_x_org TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_users_x_org TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_users_x_org TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_users_x_org TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_users_x_org TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_users_x_org TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_users_x_org TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_users_x_org TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_users_x_org TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_users_x_org TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_users_x_org TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_users_x_org TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_users_x_org TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_users_x_org TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_users_x_org TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_users_x_org TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_users_x_org TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_users_x_org TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_users_x_org TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_users_x_org TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_users_x_org TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_users_x_org TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.roles_x_users_x_org TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.roles_x_users_x_org TO group_readonly;
GRANT SELECT ON TABLE public.roles_x_users_x_org TO datastream_user;
GRANT ALL ON TABLE public.roles_x_users_x_org TO squatter;


--
-- Name: TABLE slack_spaces; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_spaces TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_spaces TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_spaces TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_spaces TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_spaces TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_spaces TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_spaces TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_spaces TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_spaces TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_spaces TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_spaces TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_spaces TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_spaces TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_spaces TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_spaces TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_spaces TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_spaces TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_spaces TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_spaces TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_spaces TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_spaces TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_spaces TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_spaces TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_spaces TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_spaces TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_spaces TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_spaces TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_spaces TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.slack_spaces TO group_readonly;
GRANT SELECT ON TABLE public.slack_spaces TO datastream_user;
GRANT ALL ON TABLE public.slack_spaces TO squatter;


--
-- Name: SEQUENCE slack_spaces_id_seq; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT ALL ON SEQUENCE public.slack_spaces_id_seq TO cloudsqladmin;
GRANT ALL ON SEQUENCE public.slack_spaces_id_seq TO pg_monitor;
GRANT ALL ON SEQUENCE public.slack_spaces_id_seq TO pg_read_all_settings;
GRANT ALL ON SEQUENCE public.slack_spaces_id_seq TO pg_read_all_stats;
GRANT ALL ON SEQUENCE public.slack_spaces_id_seq TO pg_stat_scan_tables;
GRANT ALL ON SEQUENCE public.slack_spaces_id_seq TO pg_signal_backend;
GRANT ALL ON SEQUENCE public.slack_spaces_id_seq TO pg_checkpoint;
GRANT ALL ON SEQUENCE public.slack_spaces_id_seq TO pg_use_reserved_connections;
GRANT ALL ON SEQUENCE public.slack_spaces_id_seq TO pg_read_server_files;
GRANT ALL ON SEQUENCE public.slack_spaces_id_seq TO pg_write_server_files;
GRANT ALL ON SEQUENCE public.slack_spaces_id_seq TO pg_execute_server_program;
GRANT ALL ON SEQUENCE public.slack_spaces_id_seq TO pg_database_owner;
GRANT ALL ON SEQUENCE public.slack_spaces_id_seq TO pg_read_all_data;
GRANT ALL ON SEQUENCE public.slack_spaces_id_seq TO pg_write_all_data;
GRANT ALL ON SEQUENCE public.slack_spaces_id_seq TO pg_create_subscription;
GRANT ALL ON SEQUENCE public.slack_spaces_id_seq TO cloudsqlagent;
GRANT ALL ON SEQUENCE public.slack_spaces_id_seq TO cloudsqlimportexport;
GRANT ALL ON SEQUENCE public.slack_spaces_id_seq TO cloudsqlreplica;
GRANT ALL ON SEQUENCE public.slack_spaces_id_seq TO cloudsqlobservability;
GRANT ALL ON SEQUENCE public.slack_spaces_id_seq TO cloudsqliamserviceaccount;
GRANT ALL ON SEQUENCE public.slack_spaces_id_seq TO cloudsqliamgroup;
GRANT ALL ON SEQUENCE public.slack_spaces_id_seq TO cloudsqliamgroupuser;
GRANT ALL ON SEQUENCE public.slack_spaces_id_seq TO cloudsqliamuser;
GRANT ALL ON SEQUENCE public.slack_spaces_id_seq TO cloudsqllogical;
GRANT ALL ON SEQUENCE public.slack_spaces_id_seq TO cloudsqliamgroupserviceaccount;
GRANT ALL ON SEQUENCE public.slack_spaces_id_seq TO f3_test;
GRANT ALL ON SEQUENCE public.slack_spaces_id_seq TO map;
GRANT ALL ON SEQUENCE public.slack_spaces_id_seq TO cloudsqlconnpooladmin;
GRANT ALL ON SEQUENCE public.slack_spaces_id_seq TO backslash;
GRANT SELECT ON SEQUENCE public.slack_spaces_id_seq TO squatter;


--
-- Name: TABLE slack_users; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_users TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_users TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_users TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_users TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_users TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_users TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_users TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_users TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_users TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_users TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_users TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_users TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_users TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_users TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_users TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_users TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_users TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_users TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_users TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_users TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_users TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_users TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_users TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_users TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_users TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_users TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_users TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.slack_users TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.slack_users TO group_readonly;
GRANT SELECT ON TABLE public.slack_users TO datastream_user;
GRANT ALL ON TABLE public.slack_users TO squatter;


--
-- Name: SEQUENCE slack_users_id_seq; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT ALL ON SEQUENCE public.slack_users_id_seq TO cloudsqladmin;
GRANT ALL ON SEQUENCE public.slack_users_id_seq TO pg_monitor;
GRANT ALL ON SEQUENCE public.slack_users_id_seq TO pg_read_all_settings;
GRANT ALL ON SEQUENCE public.slack_users_id_seq TO pg_read_all_stats;
GRANT ALL ON SEQUENCE public.slack_users_id_seq TO pg_stat_scan_tables;
GRANT ALL ON SEQUENCE public.slack_users_id_seq TO pg_signal_backend;
GRANT ALL ON SEQUENCE public.slack_users_id_seq TO pg_checkpoint;
GRANT ALL ON SEQUENCE public.slack_users_id_seq TO pg_use_reserved_connections;
GRANT ALL ON SEQUENCE public.slack_users_id_seq TO pg_read_server_files;
GRANT ALL ON SEQUENCE public.slack_users_id_seq TO pg_write_server_files;
GRANT ALL ON SEQUENCE public.slack_users_id_seq TO pg_execute_server_program;
GRANT ALL ON SEQUENCE public.slack_users_id_seq TO pg_database_owner;
GRANT ALL ON SEQUENCE public.slack_users_id_seq TO pg_read_all_data;
GRANT ALL ON SEQUENCE public.slack_users_id_seq TO pg_write_all_data;
GRANT ALL ON SEQUENCE public.slack_users_id_seq TO pg_create_subscription;
GRANT ALL ON SEQUENCE public.slack_users_id_seq TO cloudsqlagent;
GRANT ALL ON SEQUENCE public.slack_users_id_seq TO cloudsqlimportexport;
GRANT ALL ON SEQUENCE public.slack_users_id_seq TO cloudsqlreplica;
GRANT ALL ON SEQUENCE public.slack_users_id_seq TO cloudsqlobservability;
GRANT ALL ON SEQUENCE public.slack_users_id_seq TO cloudsqliamserviceaccount;
GRANT ALL ON SEQUENCE public.slack_users_id_seq TO cloudsqliamgroup;
GRANT ALL ON SEQUENCE public.slack_users_id_seq TO cloudsqliamgroupuser;
GRANT ALL ON SEQUENCE public.slack_users_id_seq TO cloudsqliamuser;
GRANT ALL ON SEQUENCE public.slack_users_id_seq TO cloudsqllogical;
GRANT ALL ON SEQUENCE public.slack_users_id_seq TO cloudsqliamgroupserviceaccount;
GRANT ALL ON SEQUENCE public.slack_users_id_seq TO f3_test;
GRANT ALL ON SEQUENCE public.slack_users_id_seq TO map;
GRANT ALL ON SEQUENCE public.slack_users_id_seq TO cloudsqlconnpooladmin;
GRANT ALL ON SEQUENCE public.slack_users_id_seq TO backslash;
GRANT SELECT ON SEQUENCE public.slack_users_id_seq TO squatter;


--
-- Name: TABLE update_requests; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.update_requests TO cloudsqladmin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.update_requests TO pg_monitor;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.update_requests TO pg_read_all_settings;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.update_requests TO pg_read_all_stats;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.update_requests TO pg_stat_scan_tables;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.update_requests TO pg_signal_backend;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.update_requests TO pg_checkpoint;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.update_requests TO pg_use_reserved_connections;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.update_requests TO pg_read_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.update_requests TO pg_write_server_files;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.update_requests TO pg_execute_server_program;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.update_requests TO pg_database_owner;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.update_requests TO pg_read_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.update_requests TO pg_write_all_data;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.update_requests TO pg_create_subscription;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.update_requests TO cloudsqlagent;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.update_requests TO cloudsqlimportexport;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.update_requests TO cloudsqlreplica;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.update_requests TO cloudsqlobservability;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.update_requests TO cloudsqliamserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.update_requests TO cloudsqliamgroup;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.update_requests TO cloudsqliamgroupuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.update_requests TO cloudsqliamuser;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.update_requests TO cloudsqllogical;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.update_requests TO cloudsqliamgroupserviceaccount;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.update_requests TO f3_test;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.update_requests TO map;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.update_requests TO cloudsqlconnpooladmin;
GRANT SELECT ON TABLE public.update_requests TO group_readonly;
GRANT ALL ON TABLE public.update_requests TO squatter;


--
-- Name: SEQUENCE users_id_seq; Type: ACL; Schema: public; Owner: f3slackbot
--

GRANT ALL ON SEQUENCE public.users_id_seq TO cloudsqladmin;
GRANT ALL ON SEQUENCE public.users_id_seq TO pg_monitor;
GRANT ALL ON SEQUENCE public.users_id_seq TO pg_read_all_settings;
GRANT ALL ON SEQUENCE public.users_id_seq TO pg_read_all_stats;
GRANT ALL ON SEQUENCE public.users_id_seq TO pg_stat_scan_tables;
GRANT ALL ON SEQUENCE public.users_id_seq TO pg_signal_backend;
GRANT ALL ON SEQUENCE public.users_id_seq TO pg_checkpoint;
GRANT ALL ON SEQUENCE public.users_id_seq TO pg_use_reserved_connections;
GRANT ALL ON SEQUENCE public.users_id_seq TO pg_read_server_files;
GRANT ALL ON SEQUENCE public.users_id_seq TO pg_write_server_files;
GRANT ALL ON SEQUENCE public.users_id_seq TO pg_execute_server_program;
GRANT ALL ON SEQUENCE public.users_id_seq TO pg_database_owner;
GRANT ALL ON SEQUENCE public.users_id_seq TO pg_read_all_data;
GRANT ALL ON SEQUENCE public.users_id_seq TO pg_write_all_data;
GRANT ALL ON SEQUENCE public.users_id_seq TO pg_create_subscription;
GRANT ALL ON SEQUENCE public.users_id_seq TO cloudsqlagent;
GRANT ALL ON SEQUENCE public.users_id_seq TO cloudsqlimportexport;
GRANT ALL ON SEQUENCE public.users_id_seq TO cloudsqlreplica;
GRANT ALL ON SEQUENCE public.users_id_seq TO cloudsqlobservability;
GRANT ALL ON SEQUENCE public.users_id_seq TO cloudsqliamserviceaccount;
GRANT ALL ON SEQUENCE public.users_id_seq TO cloudsqliamgroup;
GRANT ALL ON SEQUENCE public.users_id_seq TO cloudsqliamgroupuser;
GRANT ALL ON SEQUENCE public.users_id_seq TO cloudsqliamuser;
GRANT ALL ON SEQUENCE public.users_id_seq TO cloudsqllogical;
GRANT ALL ON SEQUENCE public.users_id_seq TO cloudsqliamgroupserviceaccount;
GRANT ALL ON SEQUENCE public.users_id_seq TO f3_test;
GRANT ALL ON SEQUENCE public.users_id_seq TO map;
GRANT ALL ON SEQUENCE public.users_id_seq TO cloudsqlconnpooladmin;
GRANT ALL ON SEQUENCE public.users_id_seq TO backslash;
GRANT ALL ON SEQUENCE public.users_id_seq TO app_auth;
GRANT SELECT ON SEQUENCE public.users_id_seq TO squatter;


--
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: public; Owner: f3slackbot
--

ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT ALL ON SEQUENCES TO f3slackbot;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT ALL ON SEQUENCES TO cloudsqladmin;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT ALL ON SEQUENCES TO pg_monitor;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT ALL ON SEQUENCES TO pg_read_all_settings;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT ALL ON SEQUENCES TO pg_read_all_stats;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT ALL ON SEQUENCES TO pg_stat_scan_tables;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT ALL ON SEQUENCES TO pg_signal_backend;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT ALL ON SEQUENCES TO pg_checkpoint;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT ALL ON SEQUENCES TO pg_use_reserved_connections;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT ALL ON SEQUENCES TO pg_read_server_files;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT ALL ON SEQUENCES TO pg_write_server_files;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT ALL ON SEQUENCES TO pg_execute_server_program;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT ALL ON SEQUENCES TO pg_database_owner;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT ALL ON SEQUENCES TO pg_read_all_data;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT ALL ON SEQUENCES TO pg_write_all_data;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT ALL ON SEQUENCES TO pg_create_subscription;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT ALL ON SEQUENCES TO cloudsqlagent;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT ALL ON SEQUENCES TO cloudsqlimportexport;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT ALL ON SEQUENCES TO cloudsqlreplica;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT ALL ON SEQUENCES TO cloudsqlobservability;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT ALL ON SEQUENCES TO cloudsqliamserviceaccount;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT ALL ON SEQUENCES TO cloudsqliamgroup;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT ALL ON SEQUENCES TO cloudsqliamgroupuser;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT ALL ON SEQUENCES TO cloudsqliamuser;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT ALL ON SEQUENCES TO cloudsqllogical;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT ALL ON SEQUENCES TO cloudsqliamgroupserviceaccount;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT ALL ON SEQUENCES TO f3_test;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT ALL ON SEQUENCES TO map;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT ALL ON SEQUENCES TO cloudsqlconnpooladmin;


--
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: public; Owner: tackle
--

ALTER DEFAULT PRIVILEGES FOR ROLE tackle IN SCHEMA public GRANT SELECT ON SEQUENCES TO group_readonly;
ALTER DEFAULT PRIVILEGES FOR ROLE tackle IN SCHEMA public GRANT SELECT ON SEQUENCES TO squatter;


--
-- Name: DEFAULT PRIVILEGES FOR FUNCTIONS; Type: DEFAULT ACL; Schema: public; Owner: tackle
--

ALTER DEFAULT PRIVILEGES FOR ROLE tackle IN SCHEMA public GRANT ALL ON FUNCTIONS TO squatter;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: public; Owner: f3slackbot
--

ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO f3slackbot;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO cloudsqladmin;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO pg_monitor;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO pg_read_all_settings;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO pg_read_all_stats;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO pg_stat_scan_tables;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO pg_signal_backend;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO pg_checkpoint;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO pg_use_reserved_connections;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO pg_read_server_files;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO pg_write_server_files;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO pg_execute_server_program;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO pg_database_owner;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO pg_read_all_data;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO pg_write_all_data;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO pg_create_subscription;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO cloudsqlagent;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO cloudsqlimportexport;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO cloudsqlreplica;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO cloudsqlobservability;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO cloudsqliamserviceaccount;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO cloudsqliamgroup;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO cloudsqliamgroupuser;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO cloudsqliamuser;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO cloudsqllogical;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO cloudsqliamgroupserviceaccount;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO f3_test;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO map;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO cloudsqlconnpooladmin;
ALTER DEFAULT PRIVILEGES FOR ROLE f3slackbot IN SCHEMA public GRANT SELECT ON TABLES TO group_readonly;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: public; Owner: tackle
--

ALTER DEFAULT PRIVILEGES FOR ROLE tackle IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO tackle;
ALTER DEFAULT PRIVILEGES FOR ROLE tackle IN SCHEMA public GRANT ALL ON TABLES TO squatter;


--
-- PostgreSQL database dump complete
--

\unrestrict Qbahcha1rG0zRBeOu5VkRb4k6An0JtSEqRHNLaiUHhwJZn4dsKytv5CIHk9tJpn

