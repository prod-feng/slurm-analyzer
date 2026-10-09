"""Read Slurm account hierarchy and associations using sacctmgr."""
from __future__ import annotations

import os
import subprocess


def _run(args, timeout=300):
    env = os.environ.copy()
    env["TZ"] = "UTC"
    result = subprocess.run(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        env=env,
        timeout=timeout,
    )
    if result.returncode:
        raise RuntimeError("sacctmgr failed: {}\n{}".format(" ".join(args), result.stderr))
    return result.stdout


def _parse_association_tree(output):
    """Parse ``sacctmgr -p list associations tree`` output.

    ``-p`` (parsable) is important here.  The normal ``sacctmgr`` output is
    fixed-width and the tree indentation changes the apparent column widths;
    slicing character positions can therefore turn trailing characters from
    the User column into part of Account.  Parsable output uses ``|`` as the
    delimiter and is stable regardless of indentation or name length.
    """
    rows = []
    for line in output.splitlines():
        if not line.strip():
            continue
        parts = line.split("|")
        if len(parts) < 3:
            # Be tolerant of a header or separator if a site/version emits it.
            continue
        account = parts[0].strip()
        user = parts[1].strip()
        parent = parts[2].strip()
        if not account:
            continue
        if account.lower() == "account":
            continue
        if set(account) <= {"-", " "}:
            continue
        if user in {"(null)", "None", "NULL", "-"}:
            user = ""
        if parent in {"(null)", "None", "NULL", "-"}:
            parent = ""
        rows.append((account, user or None, parent or None))
    return rows


def fetch_accounts(timeout=300):
    """Return accounts and hierarchy from Slurm's association tree.

    This intentionally uses the same command an administrator can run by
    hand::

        sacctmgr list associations tree format=Account,user,ParentName

    ``ParentName`` is therefore taken from the actual association tree rather
    than inferred from ``show account`` output.
    """
    output = _run([
        "sacctmgr", "-p", "list", "associations", "tree",
        "format=Account,user,ParentName",
    ], timeout)
    tree_rows = _parse_association_tree(output)

    # Get descriptions separately.  They are metadata only; hierarchy comes
    # from the association tree above.
    account_output = _run([
        "sacctmgr", "-n", "-P", "show", "account",
        "format=Account,Description",
    ], timeout)
    descriptions = {}
    for line in account_output.splitlines():
        parts = line.split("|")
        if not parts or not parts[0].strip():
            continue
        name = parts[0].strip()
        description = parts[1].strip() if len(parts) > 1 else ""
        descriptions[name] = description or None

    # One account can occur more than once in the association tree because of
    # users/clusters. Keep the first parent and prefer a non-empty parent if a
    # later association provides one.
    accounts = {}
    for name, _user, parent in tree_rows:
        if name not in accounts:
            accounts[name] = parent
        elif accounts[name] is None and parent:
            accounts[name] = parent

    # Include accounts that exist in the account table but are absent from the
    # association output.
    for name in descriptions:
        accounts.setdefault(name, None)

    return [
        (name, accounts[name], descriptions.get(name))
        for name in accounts
    ]


def fetch_associations(timeout=300):
    output = _run([
        "sacctmgr", "-n", "-P", "show", "assoc",
        "format=Cluster,Account,User",
    ], timeout)
    rows = []
    for line in output.splitlines():
        parts = line.split("|")
        if len(parts) < 2:
            continue
        cluster = parts[0].strip()
        account = parts[1].strip()
        user = parts[2].strip() if len(parts) > 2 else ""
        if user in {"(null)", "None", "NULL", "-"}:
            user = ""
        if account:
            # An empty User is a valid sacctmgr account-level association.
            rows.append((cluster, account, user or None))
    return rows


def add_hierarchy(rows):
    """Calculate hierarchy depth and a slash-separated canonical path."""
    parents = {name: parent for name, parent, _ in rows}
    result = []
    for name, parent, description in rows:
        chain = []
        current = name
        seen = set()
        while current and current not in seen:
            seen.add(current)
            chain.append(current)
            current = parents.get(current)
        chain.reverse()
        result.append((
            name,
            parent,
            description,
            max(0, len(chain) - 1),
            "/".join(chain),
        ))
    return result
