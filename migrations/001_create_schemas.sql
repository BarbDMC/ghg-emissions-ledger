-- Base schemas for the medallion layers. dbt also creates its schemas on demand,
-- but creating them here keeps a fresh warehouse (local or Supabase) predictable.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;

CREATE TABLE IF NOT EXISTS public.schema_migrations (
    version VARCHAR(100) PRIMARY KEY,
    applied_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Supabase exposes the public schema through its Data API; with RLS enabled and no
-- policies, the anon/authenticated API roles cannot read this table. No effect locally.
ALTER TABLE public.schema_migrations ENABLE ROW LEVEL SECURITY;
