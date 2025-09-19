-- Books - canonical record
create table if not exists books (
  id text primary key,
  title text,
  description text,
  "s3-image-path" text,
  external_image_url text,
  isbn10 text unique,
  isbn13 text unique,
  published_date date,
  language text,
  publisher text,
  info_link text,
  preview_link text,
  purchase_link text,
  list_price numeric,
  currency_code text,
  average_rating numeric,
  ratings_count integer,
  page_count integer,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_books_isbn13 on books(isbn13);
create index if not exists idx_books_isbn10 on books(isbn10);

-- External IDs mapping for many-to-one association to canonical books
create table if not exists book_external_ids (
  book_id text not null references books(id) on delete cascade,
  source text not null,
  external_id text not null,
  primary key (source, external_id)
);

create index if not exists idx_book_external_ids_book_id on book_external_ids(book_id);
create index if not exists idx_book_external_ids_external_id on book_external_ids(external_id);

-- Minimal schema for books table matching JDBC upsert/select types
create table if not exists books (
  id text primary key,
  title text,
  description text,
  "s3-image-path" text,
  external_image_url text,
  isbn10 text unique,
  isbn13 text unique,
  published_date date,
  language text,
  publisher text,
  info_link text,
  preview_link text,
  purchase_link text,
  list_price numeric,
  currency_code text,
  average_rating numeric,
  ratings_count integer,
  page_count integer,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

-- Helpful indexes (unique constraints also create indexes, these are safe no-ops if already present)
create index if not exists idx_books_isbn13 on books(isbn13);
create index if not exists idx_books_isbn10 on books(isbn10);

-- External IDs mapping table for canonical books
create table if not exists book_external_ids (
  book_id text not null references books(id) on delete cascade,
  source text not null,           -- e.g., 'GOOGLE_BOOKS', 'NYT', 'OPEN_LIBRARY', 'ISBN10', 'ISBN13'
  external_id text not null,
  primary key (source, external_id)
);

create index if not exists idx_book_external_ids_book_id on book_external_ids(book_id);
create index if not exists idx_book_external_ids_external_id on book_external_ids(external_id);


-- Neutral provider-agnostic book lists (for NYT and other providers)
create table if not exists book_lists (
  list_id            text primary key,
  source             text not null,
  provider_list_id   text,
  provider_list_code text not null,
  display_name       text,
  bestsellers_date   date,
  published_date     date not null,
  updated_frequency  text,
  raw_list_json      jsonb,
  created_at         timestamptz not null default now(),
  updated_at         timestamptz not null default now()
);

create unique index if not exists uq_book_lists_source_code_pubdate
  on book_lists (source, provider_list_code, published_date);

create index if not exists idx_book_lists_latest
  on book_lists (source, provider_list_code, published_date desc);

-- Join table linking lists to canonical books
create table if not exists book_lists_join (
  list_id           text not null references book_lists(list_id) on delete cascade,
  book_id           text not null references books(id) on delete cascade,
  position          integer,
  weeks_on_list     integer,
  provider_isbn13   text,
  provider_isbn10   text,
  provider_book_ref text,
  raw_item_json     jsonb,
  created_at        timestamptz not null default now(),
  updated_at        timestamptz not null default now(),
  primary key (list_id, book_id)
);

create unique index if not exists uq_book_lists_join_list_position
  on book_lists_join (list_id, position)
  where position is not null;

create index if not exists idx_book_lists_join_book
  on book_lists_join (book_id);

