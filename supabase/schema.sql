-- Weviko Supabase schema
-- Apply in Supabase SQL Editor.

create extension if not exists pgcrypto;

-- 1. Formal parts repository
create table if not exists public.parts (
  part_number text primary key,
  market text,
  document_type text,
  source_type text,
  spec_data jsonb not null default '{}'::jsonb,
  translations jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_parts_market on public.parts (market);
create index if not exists idx_parts_created_at on public.parts (created_at desc);

-- 2. Pending review queue
create table if not exists public.pending_data (
  id uuid primary key default gen_random_uuid(),
  part_number text,
  market text,
  document_type text,
  source_type text,
  raw_json jsonb not null default '{}'::jsonb,
  status text not null default 'pending' check (status in ('pending', 'approved', 'rejected')),
  created_at timestamptz not null default now(),
  approved_at timestamptz,
  rejected_at timestamptz
);

create index if not exists idx_pending_data_status on public.pending_data (status);
create index if not exists idx_pending_data_created_at on public.pending_data (created_at desc);

-- 3. Prompt/config store
create table if not exists public.configs (
  prompt_key text primary key,
  prompt_value text not null,
  updated_at timestamptz not null default now()
);

-- 4. Crawl/dead-letter error store
create table if not exists public.dead_letters (
  id uuid primary key default gen_random_uuid(),
  url text not null,
  error_reason text,
  resolved boolean not null default false,
  created_at timestamptz not null default now()
);

create index if not exists idx_dead_letters_resolved on public.dead_letters (resolved);
create index if not exists idx_dead_letters_created_at on public.dead_letters (created_at desc);

-- 5. Crawl hash/cache log for the Playwright pipeline
create table if not exists public.crawling_logs (
  id uuid primary key default gen_random_uuid(),
  url text not null,
  content_hash text not null,
  updated_at timestamptz not null default now()
);

create unique index if not exists idx_crawling_logs_content_hash on public.crawling_logs (content_hash);
create index if not exists idx_crawling_logs_url on public.crawling_logs (url);

-- Seed default prompts used by the current v4 Streamlit app.
insert into public.configs (prompt_key, prompt_value)
values
  (
    'crawling_ecommerce',
    '자동차 부품 상세 페이지에서 부품번호, 차종, 연식, 호환 조건, 규격, 토크, 치수, 중량을 추출해 구조화된 JSON으로 정리하세요.'
  ),
  (
    'vision_gsw',
    '자동차 정비 매뉴얼, 도해도, 회로도, 토크 표 이미지입니다. 저작권을 회피하고 원문을 길게 재현하지 말고, 수치화 가능한 팩트와 정비 핵심 정보만 JSON으로 추출하세요.'
  ),
  (
    'translation_vn',
    '자동차 정비/부품 구조화 데이터를 영어(en)와 베트남어(vn)로 번역하세요. 전문 정비 용어를 사용하고, 숫자, 단위, 부품번호는 원형을 유지하세요.'
  )
on conflict (prompt_key) do update
set
  prompt_value = excluded.prompt_value,
  updated_at = now();
