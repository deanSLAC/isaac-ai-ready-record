"""
ISAAC AI-Ready Record - Ontology/Vocabulary Module
Supports both file-based (local dev) and PostgreSQL (production) storage
"""

import json
import os

# Updated to use relative path for portability
VOCAB_FILE = os.path.join(os.path.dirname(__file__), "vocabulary.json")

# Database imports (optional, for PostgreSQL support)
try:
    from database import get_db_connection, is_db_configured, test_db_connection
    DB_AVAILABLE = True
except ImportError:
    DB_AVAILABLE = False


def _use_database():
    """Determine if we should use database or file storage"""
    if not DB_AVAILABLE:
        return False
    if not is_db_configured():
        return False
    return test_db_connection()


# =============================================================================
# File-based operations (local development fallback)
# =============================================================================

def _load_vocabulary_from_file():
    """Loads the vocabulary JSON file."""
    if not os.path.exists(VOCAB_FILE):
        return {}
    with open(VOCAB_FILE, 'r') as f:
        return json.load(f)


def _save_vocabulary_to_file(vocab):
    """Saves the vocabulary to JSON."""
    with open(VOCAB_FILE, 'w') as f:
        json.dump(vocab, f, indent=4)


# =============================================================================
# PostgreSQL operations (production)
# =============================================================================

def _load_vocabulary_from_db():
    """Loads vocabulary from PostgreSQL database."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT section, category, description, terms FROM vocabulary ORDER BY section, category')
    rows = cur.fetchall()
    cur.close()
    conn.close()

    # Convert flat rows to nested structure matching file format
    vocab = {}
    for row in rows:
        section = row['section']
        category = row['category']
        if section not in vocab:
            vocab[section] = {}
        vocab[section][category] = {
            'description': row['description'] or '',
            'values': row['terms'] if isinstance(row['terms'], list) else json.loads(row['terms'])
        }
    return vocab


def _save_vocabulary_to_db(vocab):
    """Saves entire vocabulary to PostgreSQL database."""
    conn = get_db_connection()
    cur = conn.cursor()

    # Clear existing and re-insert (simple approach for full save)
    cur.execute('DELETE FROM vocabulary')

    for section, categories in vocab.items():
        for category, data in categories.items():
            cur.execute('''
                INSERT INTO vocabulary (section, category, description, terms)
                VALUES (%s, %s, %s, %s)
            ''', (section, category, data.get('description', ''), json.dumps(data.get('values', []))))

    conn.commit()
    cur.close()
    conn.close()


def _add_term_to_db(section, category, term):
    """Adds a term to a category in the database."""
    conn = get_db_connection()
    cur = conn.cursor()

    # Get current terms
    cur.execute('SELECT terms FROM vocabulary WHERE section = %s AND category = %s', (section, category))
    row = cur.fetchone()

    if not row:
        cur.close()
        conn.close()
        return False, f"Category '{category}' not found in '{section}'."

    terms = row['terms'] if isinstance(row['terms'], list) else json.loads(row['terms'])

    if term in terms:
        cur.close()
        conn.close()
        return False, f"'{term}' already exists in '{category}'."

    terms.append(term)
    cur.execute('UPDATE vocabulary SET terms = %s WHERE section = %s AND category = %s',
                (json.dumps(terms), section, category))
    conn.commit()
    cur.close()
    conn.close()
    return True, f"Added '{term}' to '{category}'."


def _add_category_to_db(section, category, description=""):
    """Adds a new category to a section in the database."""
    conn = get_db_connection()
    cur = conn.cursor()

    # Check if category exists
    cur.execute('SELECT 1 FROM vocabulary WHERE section = %s AND category = %s', (section, category))
    if cur.fetchone():
        cur.close()
        conn.close()
        return False, "Category already exists."

    cur.execute('''
        INSERT INTO vocabulary (section, category, description, terms)
        VALUES (%s, %s, %s, %s)
    ''', (section, category, description, json.dumps([])))
    conn.commit()
    cur.close()
    conn.close()
    return True, f"Created category '{category}' in '{section}'."


# =============================================================================
# Public API - automatically selects file or database backend
# =============================================================================

def load_vocabulary():
    """Loads the vocabulary from database or file."""
    if _use_database():
        try:
            return _load_vocabulary_from_db()
        except Exception:
            # Fall back to file on database error
            pass
    return _load_vocabulary_from_file()


def save_vocabulary(vocab):
    """Saves the vocabulary to database or file."""
    if _use_database():
        try:
            _save_vocabulary_to_db(vocab)
            return
        except Exception:
            # Fall back to file on database error
            pass
    _save_vocabulary_to_file(vocab)


def get_sections():
    """Returns list of top-level sections."""
    vocab = load_vocabulary()
    return list(vocab.keys())


def get_categories(section):
    """Returns categories in a section."""
    vocab = load_vocabulary()
    if section in vocab:
        return vocab[section]
    return {}


def add_term(section, category, term):
    """Adds a new term to a category within a section."""
    if _use_database():
        try:
            return _add_term_to_db(section, category, term)
        except Exception:
            pass

    # File-based fallback
    vocab = load_vocabulary()
    if section in vocab and category in vocab[section]:
        if term not in vocab[section][category]['values']:
            vocab[section][category]['values'].append(term)
            save_vocabulary(vocab)
            return True, f"Added '{term}' to '{category}'."
        else:
            return False, f"'{term}' already exists in '{category}'."
    else:
        return False, f"Category '{category}' not found in '{section}'."


def add_category(section, category, description=""):
    """Adds a new category to a section."""
    if _use_database():
        try:
            return _add_category_to_db(section, category, description)
        except Exception:
            pass

    # File-based fallback
    vocab = load_vocabulary()
    if section not in vocab:
         vocab[section] = {}

    if category not in vocab[section]:
        vocab[section][category] = {"description": description, "values": []}
        save_vocabulary(vocab)
        return True, f"Created category '{category}' in '{section}'."
    return False, "Category already exists."


def sync_file_to_db():
    """Utility to sync vocabulary from file to database (for initial setup)."""
    if not _use_database():
        return False, "Database not configured"

    vocab = _load_vocabulary_from_file()
    if not vocab:
        return False, "No vocabulary file found"

    _save_vocabulary_to_db(vocab)
    return True, f"Synced {sum(len(cats) for cats in vocab.values())} categories to database"
