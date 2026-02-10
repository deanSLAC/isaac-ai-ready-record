"""
ISAAC AI-Ready Record - Database Connection Module
PostgreSQL connection for vocabulary, templates, and records storage
"""

import os
import json
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor


def get_db_connection():
    """Create a database connection using environment variables"""
    return psycopg2.connect(
        host=os.environ.get('PGHOST', 'localhost'),
        port=os.environ.get('PGPORT', '5432'),
        database=os.environ.get('PGDATABASE', 'app'),
        user=os.environ.get('PGUSER', 'postgres'),
        password=os.environ.get('PGPASSWORD', ''),
        cursor_factory=RealDictCursor
    )


def is_db_configured():
    """Check if database environment variables are configured"""
    return bool(os.environ.get('PGHOST'))


def test_db_connection():
    """Test if database connection is working"""
    if not is_db_configured():
        return False
    try:
        conn = get_db_connection()
        conn.close()
        return True
    except Exception:
        return False


def init_tables():
    """Initialize database tables if they don't exist"""
    if not is_db_configured():
        return False

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Create templates table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS templates (
                id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                name VARCHAR(255) UNIQUE NOT NULL,
                data JSONB NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        ''')

        cur.execute('CREATE INDEX IF NOT EXISTS idx_templates_name ON templates(name)')

        # Create updated_at trigger function
        cur.execute('''
            CREATE OR REPLACE FUNCTION update_updated_at_column()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql
        ''')

        # Create trigger (drop first to avoid errors)
        cur.execute('DROP TRIGGER IF EXISTS templates_updated_at ON templates')
        cur.execute('''
            CREATE TRIGGER templates_updated_at
                BEFORE UPDATE ON templates
                FOR EACH ROW
                EXECUTE FUNCTION update_updated_at_column()
        ''')

        # Create records table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS records (
                id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                record_id CHAR(26) UNIQUE NOT NULL,
                record_type VARCHAR(50) NOT NULL,
                record_domain VARCHAR(50) NOT NULL,
                data JSONB NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        ''')

        cur.execute('CREATE INDEX IF NOT EXISTS idx_records_record_id ON records(record_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_records_type ON records(record_type)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_records_domain ON records(record_domain)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_records_created ON records(created_at)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_records_data_gin ON records USING GIN (data)')

        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Error initializing tables: {e}")
        return False


# =============================================================================
# Record Operations
# =============================================================================

def save_record(record_data: dict) -> str:
    """
    Save an ISAAC record to the database.

    Args:
        record_data: The complete ISAAC record as a dictionary

    Returns:
        The record_id of the saved record

    Raises:
        ValueError: If required fields are missing
        Exception: If database operation fails
    """
    record_id = record_data.get('record_id')
    record_type = record_data.get('record_type')
    record_domain = record_data.get('record_domain')

    if not record_id:
        raise ValueError("record_id is required")
    if not record_type:
        raise ValueError("record_type is required")
    if not record_domain:
        raise ValueError("record_domain is required")

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute('''
            INSERT INTO records (record_id, record_type, record_domain, data)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (record_id) DO UPDATE SET
                record_type = EXCLUDED.record_type,
                record_domain = EXCLUDED.record_domain,
                data = EXCLUDED.data
            RETURNING record_id
        ''', (record_id, record_type, record_domain, json.dumps(record_data)))

        result = cur.fetchone()
        conn.commit()
        return result['record_id'].strip()
    finally:
        cur.close()
        conn.close()


def get_record(record_id: str) -> dict:
    """
    Retrieve a record by its ID.

    Args:
        record_id: The 26-character ULID record identifier

    Returns:
        The record data as a dictionary, or None if not found
    """
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute('SELECT data, created_at FROM records WHERE record_id = %s', (record_id,))
        row = cur.fetchone()

        if not row:
            return None

        return row['data']
    finally:
        cur.close()
        conn.close()


def list_records(limit: int = 100, offset: int = 0) -> list:
    """
    List all records with pagination.

    Args:
        limit: Maximum number of records to return
        offset: Number of records to skip

    Returns:
        List of record summaries (record_id, record_type, record_domain, created_at)
    """
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute('''
            SELECT record_id, record_type, record_domain, created_at
            FROM records
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
        ''', (limit, offset))

        rows = cur.fetchall()
        return [{
            'record_id': row['record_id'].strip(),
            'record_type': row['record_type'],
            'record_domain': row['record_domain'],
            'created_at': row['created_at'].isoformat() if row['created_at'] else None
        } for row in rows]
    finally:
        cur.close()
        conn.close()


def delete_record(record_id: str) -> bool:
    """
    Delete a record by its ID.

    Args:
        record_id: The record identifier to delete

    Returns:
        True if deleted, False if not found
    """
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute('DELETE FROM records WHERE record_id = %s RETURNING record_id', (record_id,))
        deleted = cur.fetchone()
        conn.commit()
        return deleted is not None
    finally:
        cur.close()
        conn.close()


def count_records() -> int:
    """Return the total number of records in the database."""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute('SELECT COUNT(*) as count FROM records')
        row = cur.fetchone()
        return row['count']
    finally:
        cur.close()
        conn.close()


# =============================================================================
# Template Operations
# =============================================================================

def save_template(name: str, data: dict) -> str:
    """
    Save a form template to the database.

    Args:
        name: Unique template name
        data: Template data (form field values)

    Returns:
        The template name
    """
    if not name or not name.strip():
        raise ValueError("Template name is required")

    name = name.strip()

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute('''
            INSERT INTO templates (name, data)
            VALUES (%s, %s)
            ON CONFLICT (name) DO UPDATE SET data = EXCLUDED.data
            RETURNING name
        ''', (name, json.dumps(data)))

        result = cur.fetchone()
        conn.commit()
        return result['name']
    finally:
        cur.close()
        conn.close()


def get_template(name: str) -> dict:
    """
    Retrieve a template by name.

    Args:
        name: Template name

    Returns:
        Template data dict with 'name', 'data', 'created_at', 'updated_at'
        or None if not found
    """
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            'SELECT name, data, created_at, updated_at FROM templates WHERE name = %s',
            (name,)
        )
        row = cur.fetchone()

        if not row:
            return None

        return {
            'name': row['name'],
            'data': row['data'],
            'created_at': row['created_at'].isoformat() if row['created_at'] else None,
            'updated_at': row['updated_at'].isoformat() if row['updated_at'] else None
        }
    finally:
        cur.close()
        conn.close()


def list_templates() -> list:
    """
    List all templates.

    Returns:
        List of template summaries (name, created_at, updated_at)
    """
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute('SELECT name, created_at, updated_at FROM templates ORDER BY name')
        rows = cur.fetchall()

        return [{
            'name': row['name'],
            'created_at': row['created_at'].isoformat() if row['created_at'] else None,
            'updated_at': row['updated_at'].isoformat() if row['updated_at'] else None
        } for row in rows]
    finally:
        cur.close()
        conn.close()


def delete_template(name: str) -> bool:
    """
    Delete a template by name.

    Args:
        name: Template name to delete

    Returns:
        True if deleted, False if not found
    """
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute('DELETE FROM templates WHERE name = %s RETURNING name', (name,))
        deleted = cur.fetchone()
        conn.commit()
        return deleted is not None
    finally:
        cur.close()
        conn.close()
