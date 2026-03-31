CREATE SCHEMA IF NOT EXISTS policy;

CREATE TABLE IF NOT EXISTS policy.provider_credentials (
    id TEXT PRIMARY KEY,
    provider_name TEXT NOT NULL,
    credential_name TEXT NOT NULL,
    api_key_ciphertext TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS policy.policy_definitions (
    policy_key TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    public_method TEXT NOT NULL,
    public_path TEXT NOT NULL,
    upstream_method TEXT NOT NULL,
    upstream_path TEXT NOT NULL,
    allowed_params JSONB NOT NULL DEFAULT '[]'::jsonb,
    denied_params JSONB NOT NULL DEFAULT '[]'::jsonb,
    default_params JSONB NOT NULL DEFAULT '{}'::jsonb,
    provider_credential_id TEXT REFERENCES policy.provider_credentials(id),
    status TEXT NOT NULL DEFAULT 'draft',
    version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE policy.policy_definitions
    ADD COLUMN IF NOT EXISTS public_method TEXT;

ALTER TABLE policy.policy_definitions
    ADD COLUMN IF NOT EXISTS public_path TEXT;

CREATE TABLE IF NOT EXISTS policy.app_policy_bindings (
    app_id TEXT NOT NULL,
    policy_key TEXT NOT NULL REFERENCES policy.policy_definitions(policy_key),
    status TEXT NOT NULL DEFAULT 'enabled',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (app_id, policy_key)
);

CREATE INDEX IF NOT EXISTS idx_policy_bindings_policy_key
    ON policy.app_policy_bindings (policy_key);

CREATE INDEX IF NOT EXISTS idx_policy_definitions_public_route
    ON policy.policy_definitions (public_method, public_path);
