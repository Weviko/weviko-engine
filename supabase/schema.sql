-- Weviko Supabase schema
-- Apply in Supabase SQL Editor.

create extension if not exists pgcrypto;

-- 1. Formal parts repository
create table if not exists public.parts (
  part_number text primary key,
  oem_brand text,
  schema_key text,
  source_path_hint text,
  market text,
  document_type text,
  source_type text,
  status text,
  spec_data jsonb not null default '{}'::jsonb,
  translations jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table public.parts add column if not exists oem_brand text;
alter table public.parts add column if not exists schema_key text;
alter table public.parts add column if not exists source_path_hint text;
alter table public.parts add column if not exists market text;
alter table public.parts add column if not exists document_type text;
alter table public.parts add column if not exists source_type text;
alter table public.parts add column if not exists status text;
alter table public.parts add column if not exists spec_data jsonb not null default '{}'::jsonb;
alter table public.parts add column if not exists translations jsonb;
alter table public.parts add column if not exists created_at timestamptz not null default now();
alter table public.parts add column if not exists updated_at timestamptz not null default now();

create index if not exists idx_parts_market on public.parts (market);
create index if not exists idx_parts_oem_brand on public.parts (oem_brand);
create index if not exists idx_parts_schema_key on public.parts (schema_key);
create index if not exists idx_parts_status on public.parts (status);
create index if not exists idx_parts_created_at on public.parts (created_at desc);

-- 2. Pending review queue
create table if not exists public.pending_data (
  id uuid primary key default gen_random_uuid(),
  part_number text,
  oem_brand text,
  schema_key text,
  source_path_hint text,
  market text,
  document_type text,
  source_type text,
  raw_json jsonb not null default '{}'::jsonb,
  status text not null default 'pending' check (status in ('pending', 'approved', 'rejected')),
  created_at timestamptz not null default now(),
  approved_at timestamptz,
  rejected_at timestamptz
);

alter table public.pending_data add column if not exists part_number text;
alter table public.pending_data add column if not exists oem_brand text;
alter table public.pending_data add column if not exists schema_key text;
alter table public.pending_data add column if not exists source_path_hint text;
alter table public.pending_data add column if not exists market text;
alter table public.pending_data add column if not exists document_type text;
alter table public.pending_data add column if not exists source_type text;
alter table public.pending_data add column if not exists raw_json jsonb not null default '{}'::jsonb;
alter table public.pending_data add column if not exists status text not null default 'pending';
alter table public.pending_data add column if not exists created_at timestamptz not null default now();
alter table public.pending_data add column if not exists approved_at timestamptz;
alter table public.pending_data add column if not exists rejected_at timestamptz;

create index if not exists idx_pending_data_status on public.pending_data (status);
create index if not exists idx_pending_data_oem_brand on public.pending_data (oem_brand);
create index if not exists idx_pending_data_schema_key on public.pending_data (schema_key);
create index if not exists idx_pending_data_created_at on public.pending_data (created_at desc);

-- 3. Prompt/config store
create table if not exists public.configs (
  prompt_key text primary key,
  prompt_value text not null,
  updated_at timestamptz not null default now()
);

-- 3a. Legacy prompt store used by helper APIs
create table if not exists public.system_prompts (
  name text primary key,
  prompt_text text not null,
  updated_at timestamptz not null default now()
);

-- 3b. Review audit log
create table if not exists public.review_decisions (
  id uuid primary key default gen_random_uuid(),
  source_url text,
  final_url text,
  part_number text,
  oem_brand text,
  schema_key text,
  source_path_hint text,
  decision text not null,
  notes text,
  review_payload jsonb not null default '{}'::jsonb,
  reviewed_at timestamptz not null default now()
);

create index if not exists idx_review_decisions_part_number on public.review_decisions (part_number);
create index if not exists idx_review_decisions_reviewed_at on public.review_decisions (reviewed_at desc);

-- 3c. Vision extraction log
create table if not exists public.vision_analysis (
  id uuid primary key default gen_random_uuid(),
  part_number text,
  oem_brand text,
  schema_key text,
  source_path_hint text,
  document_type text,
  analysis jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_vision_analysis_part_number on public.vision_analysis (part_number);
create index if not exists idx_vision_analysis_created_at on public.vision_analysis (created_at desc);

-- 3d. Translation event log
create table if not exists public.part_translations (
  id uuid primary key default gen_random_uuid(),
  source_url text,
  part_number text,
  oem_brand text,
  schema_key text,
  source_path_hint text,
  translations jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_part_translations_part_number on public.part_translations (part_number);
create index if not exists idx_part_translations_created_at on public.part_translations (created_at desc);

-- 3e. GSW document master
create table if not exists public.gsw_documents (
  id uuid primary key default gen_random_uuid(),
  source_fingerprint text unique not null,
  source_system text not null default 'hyundai_gsw',
  part_number text,
  oem_brand text,
  brand text,
  market text,
  vehicle_model text,
  vehicle_year text,
  vehicle_trim text,
  engine_code text,
  transmission_code text,
  menu_family text,
  schema_key text,
  document_type text,
  title text,
  breadcrumb_text text,
  breadcrumb_path jsonb not null default '[]'::jsonb,
  source_url text,
  source_path_hint text,
  capture_type text,
  source_type text,
  page_ref text,
  summary text,
  document_payload jsonb not null default '{}'::jsonb,
  status text not null default 'approved',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table public.gsw_documents add column if not exists source_fingerprint text;
alter table public.gsw_documents add column if not exists source_system text;
alter table public.gsw_documents add column if not exists part_number text;
alter table public.gsw_documents add column if not exists oem_brand text;
alter table public.gsw_documents add column if not exists brand text;
alter table public.gsw_documents add column if not exists market text;
alter table public.gsw_documents add column if not exists vehicle_model text;
alter table public.gsw_documents add column if not exists vehicle_year text;
alter table public.gsw_documents add column if not exists vehicle_trim text;
alter table public.gsw_documents add column if not exists engine_code text;
alter table public.gsw_documents add column if not exists transmission_code text;
alter table public.gsw_documents add column if not exists menu_family text;
alter table public.gsw_documents add column if not exists schema_key text;
alter table public.gsw_documents add column if not exists document_type text;
alter table public.gsw_documents add column if not exists title text;
alter table public.gsw_documents add column if not exists breadcrumb_text text;
alter table public.gsw_documents add column if not exists breadcrumb_path jsonb not null default '[]'::jsonb;
alter table public.gsw_documents add column if not exists source_url text;
alter table public.gsw_documents add column if not exists source_path_hint text;
alter table public.gsw_documents add column if not exists capture_type text;
alter table public.gsw_documents add column if not exists source_type text;
alter table public.gsw_documents add column if not exists page_ref text;
alter table public.gsw_documents add column if not exists summary text;
alter table public.gsw_documents add column if not exists document_payload jsonb not null default '{}'::jsonb;
alter table public.gsw_documents add column if not exists status text not null default 'approved';
alter table public.gsw_documents add column if not exists created_at timestamptz not null default now();
alter table public.gsw_documents add column if not exists updated_at timestamptz not null default now();

create unique index if not exists idx_gsw_documents_source_fingerprint on public.gsw_documents (source_fingerprint);
create index if not exists idx_gsw_documents_schema_key on public.gsw_documents (schema_key);
create index if not exists idx_gsw_documents_document_type on public.gsw_documents (document_type);
create index if not exists idx_gsw_documents_vehicle_model on public.gsw_documents (vehicle_model);
create index if not exists idx_gsw_documents_status on public.gsw_documents (status);
create index if not exists idx_gsw_documents_created_at on public.gsw_documents (created_at desc);

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
  ),
  (
    'path_manual',
    '정비 지침서 성격의 자료입니다. 작업 순서, 공구, 토크, 주의사항, 분해/조립 절차를 구조화하세요.'
  ),
  (
    'path_body_manual',
    '차체매뉴얼 자료입니다. 패널명, 탈거/장착 순서, 체결부, 실러/접착, 조정 포인트, 주의사항을 구조화하세요.'
  ),
  (
    'path_detail',
    '부품 제원/호환성 페이지입니다. 부품번호, 규격, OEM 정보, 적용 차종, 연식, 호환 조건을 우선 추출하세요.'
  ),
  (
    'path_connector',
    '와이어링 커넥터 자료입니다. 커넥터명, 위치, 핀 수, 핀맵, 배선색, 신호명, 연결 대상만 구조화하세요.'
  ),
  (
    'path_vehicle_id',
    '차량 식별/VIN/페인트 코드/엔진 코드 해설 자료입니다. 부품번호를 억지로 만들지 말고, VIN 예시, 차대번호 규칙, 페인트 코드, 엔진/변속기 코드, 시리얼/라벨 구조 같은 차량 식별 팩트만 JSON으로 정리하세요.'
  ),
  (
    'path_wiring',
    '회로도/배선도 자료입니다. 커넥터, 핀, 회로명, 전압/저항 등 계측 가능한 사실만 구조화하세요.'
  ),
  (
    'path_community',
    '포럼/실전 팁 자료입니다. 검증 가능한 정비 팁, 증상, 해결법, 반복되는 오류 패턴만 요약하세요.'
  ),
  (
    'path_dtc',
    '고장 코드(DTC) 자료입니다. 코드, 증상, 원인, 점검 절차, 권장 조치를 구조화하세요.'
  ),
  (
    'confidence_threshold',
    '90'
  )
on conflict (prompt_key) do update
set
  prompt_value = excluded.prompt_value,
  updated_at = now();
