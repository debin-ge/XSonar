CREATE SCHEMA IF NOT EXISTS access;

CREATE TABLE IF NOT EXISTS access.tenants (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS access.console_users (
    id TEXT PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'platform_admin',
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS access.secret_materials (
    id TEXT PRIMARY KEY,
    app_id TEXT NOT NULL,
    secret_ciphertext TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS access.plan_templates (
    id TEXT PRIMARY KEY,
    plan_key TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    daily_quota BIGINT NOT NULL DEFAULT 0,
    qps_limit INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS access.tenant_apps (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES access.tenants(id),
    name TEXT NOT NULL,
    app_key TEXT NOT NULL UNIQUE,
    secret_material_id TEXT REFERENCES access.secret_materials(id),
    daily_quota BIGINT NOT NULL DEFAULT 0,
    qps_limit INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tenant_apps_tenant_id ON access.tenant_apps (tenant_id);

CREATE TABLE IF NOT EXISTS access.usage_stats (
    bucket_start TIMESTAMPTZ NOT NULL,
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    policy_key TEXT NOT NULL,
    total_count BIGINT NOT NULL DEFAULT 0,
    success_count BIGINT NOT NULL DEFAULT 0,
    failure_count BIGINT NOT NULL DEFAULT 0,
    duration_sum_ms BIGINT NOT NULL DEFAULT 0,
    upstream_duration_sum_ms BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (bucket_start, tenant_id, app_id, policy_key)
);

CREATE INDEX IF NOT EXISTS idx_usage_stats_tenant_app_policy
    ON access.usage_stats (tenant_id, app_id, policy_key, bucket_start DESC);
