create table if not exists markets (
  id text primary key,
  condition_id text,
  question text not null,
  slug text,
  category text,
  active boolean not null default true,
  closed boolean not null default false,
  end_time timestamptz,
  volume numeric not null default 0,
  liquidity numeric not null default 0,
  yes_token_id text,
  no_token_id text,
  yes_price numeric not null default 0,
  no_price numeric not null default 0,
  best_bid numeric not null default 0,
  best_ask numeric not null default 0,
  spread numeric not null default 0,
  orderbook jsonb not null default '{}'::jsonb,
  created_at timestamptz not null,
  updated_at timestamptz not null
);

create table if not exists live_market_states (
  market_id text primary key references markets(id) on delete cascade,
  asset_id text,
  best_bid numeric not null default 0,
  best_ask numeric not null default 0,
  last_price numeric not null default 0,
  mid_price numeric not null default 0,
  spread numeric not null default 0,
  orderbook_bids jsonb not null default '[]'::jsonb,
  orderbook_asks jsonb not null default '[]'::jsonb,
  updated_at timestamptz not null
);

create table if not exists news_articles (
  id text primary key,
  source text not null,
  title text not null,
  url text,
  content text,
  summary text,
  published_at timestamptz,
  fetched_at timestamptz not null,
  hash text unique not null,
  entities jsonb not null default '[]'::jsonb,
  keywords jsonb not null default '[]'::jsonb
);

create table if not exists news_market_matches (
  id text primary key,
  news_id text not null references news_articles(id) on delete cascade,
  market_id text not null references markets(id) on delete cascade,
  keyword_score numeric not null,
  entity_score numeric not null,
  embedding_score numeric not null,
  final_score numeric not null,
  reason text,
  created_at timestamptz not null
);

create table if not exists ai_signals (
  id text primary key,
  market_id text not null references markets(id) on delete cascade,
  news_id text not null references news_articles(id) on delete cascade,
  related boolean not null,
  direction text not null,
  probability_delta numeric not null,
  confidence numeric not null,
  source_reliability numeric not null,
  priced_in_risk text not null,
  reasoning text,
  raw_response text,
  disabled boolean not null default false,
  created_at timestamptz not null
);

create table if not exists probability_decisions (
  id text primary key,
  market_id text not null references markets(id) on delete cascade,
  news_id text,
  market_probability numeric not null,
  executable_price numeric not null,
  our_probability numeric not null,
  edge numeric not null,
  confidence numeric not null,
  components_json jsonb not null default '{}'::jsonb,
  decision text not null,
  reason text,
  side text not null default 'buy',
  outcome text not null default 'yes',
  created_at timestamptz not null
);

create table if not exists risk_decisions (
  id text primary key,
  market_id text not null references markets(id) on delete cascade,
  probability_decision_id text not null references probability_decisions(id) on delete cascade,
  approved boolean not null,
  position_size_usd numeric not null,
  max_loss_usd numeric not null,
  checks_json jsonb not null default '{}'::jsonb,
  reject_reason text,
  reason text,
  created_at timestamptz not null
);

create table if not exists trades (
  id text primary key,
  mode text not null,
  market_id text not null references markets(id) on delete cascade,
  outcome text not null,
  side text not null,
  status text not null,
  entry_price numeric not null default 0,
  exit_price numeric not null default 0,
  size_usd numeric not null default 0,
  quantity numeric not null default 0,
  fees_usd numeric not null default 0,
  slippage_usd numeric not null default 0,
  realized_pnl_usd numeric not null default 0,
  unrealized_pnl_usd numeric not null default 0,
  reason text,
  opened_at timestamptz,
  closed_at timestamptz,
  created_at timestamptz not null,
  updated_at timestamptz not null
);

create table if not exists positions (
  id text primary key,
  market_id text not null references markets(id) on delete cascade,
  outcome text not null,
  quantity numeric not null,
  avg_entry_price numeric not null,
  current_price numeric not null,
  exposure_usd numeric not null,
  unrealized_pnl_usd numeric not null default 0,
  status text not null,
  opened_at timestamptz not null,
  updated_at timestamptz not null
);

create table if not exists portfolio_snapshots (
  id text primary key,
  cash_usd numeric not null,
  exposure_usd numeric not null,
  open_pnl_usd numeric not null,
  realized_pnl_usd numeric not null,
  created_at timestamptz not null
);

create table if not exists audit_logs (
  id text primary key,
  event text not null,
  entity_id text,
  payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null
);

create index if not exists idx_markets_updated_at on markets(updated_at desc);
create index if not exists idx_news_articles_published_at on news_articles(published_at desc);
create index if not exists idx_news_market_matches_news_id on news_market_matches(news_id);
create index if not exists idx_ai_signals_market_news on ai_signals(market_id, news_id, created_at desc);
create index if not exists idx_probability_decisions_created_at on probability_decisions(created_at desc);
create index if not exists idx_risk_decisions_created_at on risk_decisions(created_at desc);
create index if not exists idx_trades_created_at on trades(created_at desc);
create index if not exists idx_positions_status on positions(status);
create index if not exists idx_audit_logs_created_at on audit_logs(created_at desc);
