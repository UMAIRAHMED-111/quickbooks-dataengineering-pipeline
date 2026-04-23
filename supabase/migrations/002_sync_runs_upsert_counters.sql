alter table public.sync_runs
  add column if not exists inserted_count int,
  add column if not exists updated_count int,
  add column if not exists unchanged_count int;

comment on column public.sync_runs.inserted_count is
  'Total rows inserted during incremental upsert across customers, invoices, payments, and allocations.';
comment on column public.sync_runs.updated_count is
  'Total rows updated during incremental upsert across customers, invoices, payments, and allocations.';
comment on column public.sync_runs.unchanged_count is
  'Total rows detected as unchanged (no-op) during incremental upsert across customers, invoices, payments, and allocations.';
