-- ISAAC Record Portal - Database Schema
-- Run: psql -h <host> -U <user> -d <db> -f init_db.sql
-- Note: Tables are also auto-created by the portal on startup

-- Templates table (for form presets)
CREATE TABLE IF NOT EXISTS templates (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_templates_name ON templates(name);

-- Trigger to auto-update updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS templates_updated_at ON templates;
CREATE TRIGGER templates_updated_at
    BEFORE UPDATE ON templates
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ISAAC Records table
CREATE TABLE IF NOT EXISTS records (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    record_id CHAR(26) UNIQUE NOT NULL,
    record_type VARCHAR(50) NOT NULL,
    record_domain VARCHAR(50) NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_records_record_id ON records(record_id);
CREATE INDEX IF NOT EXISTS idx_records_type ON records(record_type);
CREATE INDEX IF NOT EXISTS idx_records_domain ON records(record_domain);
CREATE INDEX IF NOT EXISTS idx_records_created ON records(created_at);
CREATE INDEX IF NOT EXISTS idx_records_data_gin ON records USING GIN (data);
