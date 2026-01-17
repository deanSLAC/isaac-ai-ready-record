"""
ISAAC AI-Ready Record - Database Connection Module
PostgreSQL connection for vocabulary and record storage
"""

import os
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
