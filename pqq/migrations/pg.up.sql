--
-- These are the migrations for the PostgreSQL queue. They are only used for upgrades to the latest version.
-- based on https://metacpan.org/release/SRI/Minion-10.25/source/lib/Minion/Backend/resources/migrations/pg.sql
-- and: https://dev.to/mikevv/simple-queue-with-postgresql-1ngc
--
-- 18 up

-- DROP TYPE IF EXISTS job_state;
CREATE TYPE job_state AS ENUM ('inactive', 'active', 'failed', 'finished');
DO
$$
BEGIN
  IF NOT EXISTS (SELECT *
                        FROM pg_type typ
                             INNER JOIN pg_namespace nsp
                                        ON nsp.oid = typ.typnamespace
                        WHERE nsp.nspname = current_schema()
                              AND typ.typname = 'job_state') THEN
    CREATE TYPE job_state AS ENUM ('inactive', 'active', 'failed', 'finished');
  END IF;
END;
$$
LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS base_queue (
  id       BIGSERIAL NOT NULL PRIMARY KEY,
  payload   JSONB NOT NULL CHECK(JSONB_TYPEOF(payload) = 'object'),
  alias   TEXT DEFAULT NULL,
  try_count INT NOT NULL DEFAULT 0,
  max_tries INT NOT NULL DEFAULT 3,
  timeout INT NOT NULL DEFAULT 60,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
  priority INT DEFAULT 0 NOT NULL,
  -- parents  BIGINT[] NOT NULL DEFAULT '{}',
  state    job_state NOT NULL DEFAULT 'inactive'::JOB_STATE,
  -- task     TEXT NOT NULL,
  -- worker   BIGINT
  result   JSONB

);

CREATE TABLE IF NOT EXISTS default_queue ()
    INHERITS (base_queue);

