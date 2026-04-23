-- QuickBooks → analytics warehouse (single company). Incremental upsert each run.

create extension if not exists "pgcrypto";

create table if not exists public.sync_runs (
  id uuid primary key default gen_random_uuid(),
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  status text not null default 'running' check (status in ('running', 'success', 'failed')),
  customer_count int,
  invoice_count int,
  payment_count int,
  allocation_count int,
  error_message text
);

create table if not exists public.customers (
  id uuid primary key default gen_random_uuid(),
  qbo_id text not null unique,
  display_name text,
  company_name text,
  given_name text,
  family_name text,
  fully_qualified_name text,
  primary_email text,
  primary_phone text,
  balance numeric(18, 6) not null default 0,
  balance_with_jobs numeric(18, 6) not null default 0,
  currency_code text,
  active boolean,
  taxable boolean,
  bill_address jsonb,
  ship_address jsonb,
  qbo_create_time timestamptz,
  qbo_last_updated_time timestamptz,
  loaded_at timestamptz not null default now()
);

create table if not exists public.invoices (
  id uuid primary key default gen_random_uuid(),
  qbo_id text not null unique,
  customer_id uuid not null references public.customers (id) on delete cascade,
  doc_number text,
  txn_date date,
  due_date date,
  total_amount numeric(18, 6) not null default 0,
  balance numeric(18, 6) not null default 0,
  currency_code text,
  email_status text,
  print_status text,
  is_email_sent boolean not null default false,
  bill_email text,
  qbo_create_time timestamptz,
  qbo_last_updated_time timestamptz,
  loaded_at timestamptz not null default now()
);

create index if not exists invoices_customer_id_idx on public.invoices (customer_id);
create index if not exists invoices_due_date_idx on public.invoices (due_date);
create index if not exists invoices_txn_date_idx on public.invoices (txn_date);

create table if not exists public.payments (
  id uuid primary key default gen_random_uuid(),
  qbo_id text not null unique,
  customer_id uuid not null references public.customers (id) on delete cascade,
  txn_date date,
  total_amount numeric(18, 6) not null default 0,
  unapplied_amount numeric(18, 6) not null default 0,
  currency_code text,
  qbo_create_time timestamptz,
  qbo_last_updated_time timestamptz,
  loaded_at timestamptz not null default now()
);

create index if not exists payments_customer_id_idx on public.payments (customer_id);
create index if not exists payments_txn_date_idx on public.payments (txn_date);

create table if not exists public.payment_invoice_allocations (
  id uuid primary key default gen_random_uuid(),
  payment_id uuid not null references public.payments (id) on delete cascade,
  invoice_id uuid not null references public.invoices (id) on delete cascade,
  amount numeric(18, 6) not null,
  loaded_at timestamptz not null default now(),
  unique (payment_id, invoice_id)
);

create index if not exists payment_invoice_allocations_invoice_id_idx
  on public.payment_invoice_allocations (invoice_id);

comment on table public.customers is 'QuickBooks customers; incremental upsert each pipeline run.';
comment on table public.invoices is 'is_email_sent: QBO EmailStatus in Sent, EmailSent, NeedToSend by default; override QBO_IS_EMAIL_SENT_STATUSES.';
comment on table public.payment_invoice_allocations is 'Links payments to invoices for on-time payment analysis.';
