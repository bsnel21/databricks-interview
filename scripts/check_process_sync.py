"""Check that the shared working process is identical in this repo and its sibling repo.

The baseball and databricks-interview repos follow one process, written in
docs/databricks-project-playbook.md. This script is byte-identical in both repos; copy it, don't edit one copy.

Checks (exit code 1 if any fails):
  1. Every shared file is byte-identical in both repos (skipped with a note if the sibling repo isn't there).
  2. Every shared block in CLAUDE.md (between `<!-- shared-process:<name> BEGIN -->` and `... END -->`) is
     identical to the same block in the playbook.
  3. The installed Databricks skills (.databricks/aitools/skills/.state.json, project scope) match the
     skill list and version pinned in the shared skills block, in every repo that is present.

Usage:
  python3 scripts/check_process_sync.py                    # sibling repos are looked up next to this one
  python3 scripts/check_process_sync.py --other <path>     # explicit path to the other repo
"""
import json
import re
import sys
from pathlib import Path

REPOS = ("baseball", "databricks-interview")
SHARED_FILES = ("docs/databricks-project-playbook.md", "scripts/check_process_sync.py")
PLAYBOOK = "docs/databricks-project-playbook.md"
BLOCK_RE = re.compile(r"<!-- shared-process:(?P<name>[\w-]+) BEGIN -->\n(?P<body>.*?)<!-- shared-process:(?P=name) END -->",
                      re.S)

here = Path(__file__).resolve().parents[1]
failures = []


def fail(msg):
    failures.append(msg)
    print("FAIL  " + msg)


def ok(msg):
    print("ok    " + msg)


def blocks(path):
    return {m["name"]: m["body"] for m in BLOCK_RE.finditer(path.read_text())} if path.exists() else {}


def other_repo():
    if "--other" in sys.argv:
        return Path(sys.argv[sys.argv.index("--other") + 1]).expanduser().resolve()
    names = [n for n in REPOS if n != here.name]
    return here.parent / names[0] if len(names) == 1 else None


repos = [here]
other = other_repo()
if other and other.exists():
    repos.append(other)
else:
    print(f"note  sibling repo not found ({other}); checking this repo only")

# 1. shared files identical across repos
if len(repos) == 2:
    for rel in SHARED_FILES:
        a, b = repos[0] / rel, repos[1] / rel
        if not a.exists() or not b.exists():
            fail(f"{rel} missing in {repos[0].name if not a.exists() else repos[1].name}")
        elif a.read_bytes() != b.read_bytes():
            fail(f"{rel} differs between {repos[0].name} and {repos[1].name}")
        else:
            ok(f"{rel} identical in both repos")

for repo in repos:
    # 2. CLAUDE.md shared blocks == playbook blocks
    pb, cm = blocks(repo / PLAYBOOK), blocks(repo / "CLAUDE.md")
    if not pb:
        fail(f"{repo.name}: no shared blocks found in {PLAYBOOK}")
    for name, body in pb.items():
        if name not in cm:
            fail(f"{repo.name}: CLAUDE.md is missing shared block '{name}'")
        elif cm[name] != body:
            fail(f"{repo.name}: CLAUDE.md block '{name}' differs from the playbook")
        else:
            ok(f"{repo.name}: CLAUDE.md block '{name}' matches the playbook")

    # 3. installed skills == pinned skills
    skills_block = pb.get("databricks-skills", "")
    m_ver = re.search(r"skills version \*\*(\S+?)\*\*", skills_block)
    m_list = re.search(r"Installed skills: (.+?)\.\n", skills_block)
    if not (m_ver and m_list):
        fail(f"{repo.name}: could not read the pinned skills version and list from the playbook")
        continue
    want_ver = m_ver.group(1)
    want = sorted(re.findall(r"`([\w-]+)`", m_list.group(1)))
    state = repo / ".databricks/aitools/skills/.state.json"
    if not state.exists():
        fail(f"{repo.name}: Databricks skills not installed (run the install command in playbook section 2.8)")
        continue
    s = json.loads(state.read_text())
    have_ver, have = s.get("release", "").lstrip("v"), sorted(s.get("skills", {}))
    if s.get("scope") != "project":
        fail(f"{repo.name}: skills installed with scope '{s.get('scope')}', expected 'project'")
    if have_ver != want_ver:
        fail(f"{repo.name}: skills version {have_ver}, pinned {want_ver}")
    if have != want:
        fail(f"{repo.name}: installed skills {have} != pinned {want}")
    if have_ver == want_ver and have == want:
        ok(f"{repo.name}: {len(have)} Databricks skills at version {have_ver}")

print("\nFAILED: %d check(s)" % len(failures) if failures else "\nAll process checks passed.")
sys.exit(1 if failures else 0)
