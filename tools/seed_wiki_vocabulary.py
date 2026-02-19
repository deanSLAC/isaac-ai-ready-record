#!/usr/bin/env python3
"""
Seed Wiki Controlled Vocabulary

One-time script to append ## Controlled Vocabulary YAML blocks
to each wiki page in deanSLAC/isaac-ai-ready-record.wiki.

Usage:
    export GITHUB_TOKEN="github_pat_xxxx..."
    python tools/seed_wiki_vocabulary.py

Or pass the token directly:
    python tools/seed_wiki_vocabulary.py --token github_pat_xxxx...
"""

import argparse
import json
import os
import sys
import shutil
import tempfile
import re

# Add parent dir so we can import from portal/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

try:
    import git
except ImportError:
    print("GitPython not installed. Run: pip install GitPython")
    sys.exit(1)

WIKI_REPO = "https://github.com/deanSLAC/isaac-ai-ready-record.wiki.git"
VOCAB_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'vocabulary.json')

# Section name → wiki page filename (without .md)
SECTION_TO_PAGE = {
    "Record Info": "Record-Overview",
    "Sample": "Sample",
    "Context": "Context",
    "System": "System",
    "Measurement": "Measurement",
    "Assets": "Assets",
    "Links": "Links",
    "Descriptors": "Descriptors",
}


def generate_yaml_block(categories: dict) -> str:
    """Generate the YAML code block for a section's vocabulary."""
    lines = []
    for key, data in sorted(categories.items()):
        desc = data.get('description', '')
        values = data.get('values', [])
        lines.append(f'{key}:')
        lines.append(f'  description: "{desc}"')
        values_str = ", ".join(values)
        lines.append(f'  values: [{values_str}]')
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Seed wiki pages with Controlled Vocabulary YAML")
    parser.add_argument("--token", help="GitHub PAT (or set GITHUB_TOKEN env var)")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without pushing")
    args = parser.parse_args()

    token = args.token or os.environ.get("GITHUB_TOKEN", "")
    if not token and not args.dry_run:
        print("Error: Provide --token or set GITHUB_TOKEN env var")
        sys.exit(1)

    # Load vocabulary
    with open(VOCAB_FILE, 'r') as f:
        vocab = json.load(f)

    print(f"Loaded vocabulary: {sum(len(cats) for cats in vocab.values())} categories across {len(vocab)} sections")

    # Clone wiki
    tmp_dir = tempfile.mkdtemp(prefix="isaac_wiki_seed_")
    wiki_url = WIKI_REPO
    if token:
        wiki_url = wiki_url.replace("https://github.com/", f"https://{token}@github.com/")

    print(f"Cloning wiki to {tmp_dir}...")
    try:
        repo = git.Repo.clone_from(wiki_url, os.path.join(tmp_dir, "wiki"))
    except Exception as e:
        print(f"Failed to clone wiki: {e}")
        print("\nMake sure:")
        print("  1. The wiki exists (visit https://github.com/deanSLAC/isaac-ai-ready-record/wiki)")
        print("  2. Create at least one page via the GitHub UI first")
        print("  3. Your token has Contents read/write permission")
        shutil.rmtree(tmp_dir, ignore_errors=True)
        sys.exit(1)

    repo_path = os.path.join(tmp_dir, "wiki")
    modified_pages = []

    for section_name, wiki_page in SECTION_TO_PAGE.items():
        if section_name not in vocab:
            print(f"  SKIP {wiki_page}.md — no vocabulary data for '{section_name}'")
            continue

        md_file = os.path.join(repo_path, f"{wiki_page}.md")
        yaml_block = generate_yaml_block(vocab[section_name])
        new_section = f"\n\n## Controlled Vocabulary\n\n```yaml\n{yaml_block}\n```\n"

        if os.path.exists(md_file):
            with open(md_file, 'r') as f:
                content = f.read()

            # Check if already has a Controlled Vocabulary section
            pattern = r'##\s*Controlled\s+Vocabulary\s*\n+```yaml\s*\n.*?```'
            if re.search(pattern, content, re.DOTALL | re.IGNORECASE):
                # Replace existing
                new_content = re.sub(pattern, new_section.strip(), content, flags=re.DOTALL | re.IGNORECASE)
                print(f"  UPDATE {wiki_page}.md — replacing existing Controlled Vocabulary")
            else:
                # Append
                new_content = content.rstrip() + new_section
                print(f"  APPEND {wiki_page}.md — adding Controlled Vocabulary section")
        else:
            # Create the page
            new_content = f"# {section_name}\n{new_section}"
            print(f"  CREATE {wiki_page}.md — new page with Controlled Vocabulary")

        with open(md_file, 'w') as f:
            f.write(new_content)
        modified_pages.append(f"{wiki_page}.md")

    if not modified_pages:
        print("\nNo pages modified.")
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return

    print(f"\nModified {len(modified_pages)} pages: {', '.join(modified_pages)}")

    if args.dry_run:
        print("\n--- DRY RUN — showing diffs ---")
        print(repo.git.diff())
        print("\nDry run complete. No changes pushed.")
    else:
        # Commit and push
        repo.index.add(modified_pages)
        if repo.is_dirty() or repo.untracked_files:
            repo.index.commit("Add Controlled Vocabulary YAML blocks (ISAAC Portal seed)")
            print("Pushing to wiki...")
            repo.remotes.origin.push()
            print("Done! Wiki pages updated successfully.")
        else:
            print("No changes to commit.")

    shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
