--
-- These are the migrations for the PostgreSQL queue. They are only used for upgrades to the latest version.
-- based on https://metacpan.org/release/SRI/Minion-10.25/source/lib/Minion/Backend/resources/migrations/pg.sql
-- and: https://dev.to/mikevv/simple-queue-with-postgresql-1ngc
--
-- 18 up

-- DROP TYPE IF EXISTS job_state;
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

-- from https://stackoverflow.com/questions/3970795/how-do-you-create-a-random-string-thats-suitable-for-a-session-id-in-postgresql
CREATE or REPLACE FUNCTION random_string(length integer) returns text as
$$
declare
  chars text[] := '{0,1,2,3,4,5,6,7,8,9,A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z,a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z,-,_}';
  result text := '';
  i integer := 0;
begin
  if length < 0 then
    raise exception 'Given length cannot be less than 0';
  end if;
  for i in 1..length loop
    result := result || chars[1+random()*(array_length(chars, 1)-1)];
  end loop;
  return result;
end;
$$ language plpgsql;


CREATE TABLE IF NOT EXISTS pqq_base (
  id       BIGSERIAL NOT NULL PRIMARY KEY,
  payload   JSONB NOT NULL CHECK(JSONB_TYPEOF(payload) = 'object'),
  alias   TEXT DEFAULT NULL,
  jobid   TEXT DEFAULT random_string(8),
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

CREATE INDEX IF NOT EXISTS pqq_state_idx ON pqq_base (state);
CREATE INDEX IF NOT EXISTS pqq_state_idx ON pqq_base (jobid);
CREATE INDEX IF NOT EXISTS pqq_created_at_idx ON pqq_base (created_at);

CREATE TABLE IF NOT EXISTS pqq_default (
    LIKE pqq_base INCLUDING INDEXES
   ) INHERITS (pqq_base);

