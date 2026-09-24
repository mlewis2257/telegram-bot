"""
qsim_audit_lookahead.py — which replay policies can see the future, mechanically.

WHY THIS EXISTS
---------------
Two bugs in one week, both the same shape, neither caught by any test:

  * bor_* banked at 1.4x on positions qsim had already hard-stopped at -35%,
    because its call site never passed held_until. It was the only profitable
    family in the book, and it was profitable for that reason.
  * bank_*, obs_*, floor_* and confirm_* take a bare list of prices with no
    timestamps at all, so under --include-post-exit they bank on quotes from
    after the sale. bank_2x looked like the best policy in the table.

Both were found by reading code after the numbers looked wrong. That is the
wrong order, and it is not a habit that scales: there are forty-odd policies
here and a human reading them one at a time will miss one.

A policy can only be trusted under --include-post-exit if its ENTRY leg is
bounded by the real exit time. Whether it is, is a mechanical property of the
function signature and its call site, so it can be checked mechanically.

WHAT IT CHECKS
--------------
  1. Every `_*_return` function in the replay: does it accept `held_until`?
  2. Every call site: is `held_until` actually passed?
  3. Functions taking `mults` (bare floats, no timestamps) CANNOT be bounded
     without a signature change, and are reported as structurally unbounded.

Exit code is non-zero when a policy COULD be bounded and is not — either it
accepts held_until and a caller omits it, or it receives `points` and ignores
the timestamps sitting in them. bor_ was the second kind, and an earlier
version of this audit graded it a warning and exited 0, which is to say it
failed the one bug it was written to catch. It was fixed by testing it against
the commit where that bug still existed, which is the only way to know a
checker works.

The structurally-unbounded list is NOT a failure. Those policies are fine
without --include-post-exit; the output is there so the reader knows which
blocks to ignore when that flag is on.

    python3 qsim_audit_lookahead.py
    python3 qsim_audit_lookahead.py --json
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import sys

TARGET = os.path.join(os.path.dirname(__file__), "qsim_quote_capture_replay.py")


def _arg_names(fn: ast.FunctionDef) -> set[str]:
    a = fn.args
    return {x.arg for x in (a.posonlyargs + a.args + a.kwonlyargs)} | {
        x.arg for x in ([a.vararg] if a.vararg else []) + ([a.kwarg] if a.kwarg else [])
    }


def audit(path: str) -> dict:
    tree = ast.parse(open(path).read())

    defs: dict[str, dict] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name.endswith("_return"):
            names = _arg_names(node)
            defs[node.name] = {
                "accepts_held_until": "held_until" in names,
                # A function given only bare floats has no timestamps to compare
                # against an exit time, so it cannot be bounded without a
                # signature change. That is a design fact, not an oversight.
                "takes_timestamps": "points" in names,
                "line": node.lineno,
                "calls": [],
            }

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        fn = node.func
        name = fn.id if isinstance(fn, ast.Name) else getattr(fn, "attr", None)
        if name in defs:
            passed = {kw.arg for kw in node.keywords if kw.arg}
            defs[name]["calls"].append({
                "line": node.lineno,
                "passes_held_until": "held_until" in passed,
            })

    violations, unbounded, ok = [], [], []
    for name, info in sorted(defs.items()):
        if not info["calls"]:
            continue
        if info["accepts_held_until"]:
            bad = [c["line"] for c in info["calls"] if not c["passes_held_until"]]
            (violations if bad else ok).append(
                {"fn": name, "def_line": info["line"], "bad_call_lines": bad,
                 "why": "accepts held_until but a caller omits it"})
        elif info["takes_timestamps"]:
            # It receives `points`, so the timestamps are RIGHT THERE and it
            # simply does not use them. That is the bor_ bug in its original
            # form — fixable, therefore a violation, not a fact of life.
            violations.append(
                {"fn": name, "def_line": info["line"], "bad_call_lines": [],
                 "why": "takes points (timestamps available) but has no held_until parameter"})
        else:
            unbounded.append({"fn": name, "def_line": info["line"],
                              "reason": "takes bare mults — no timestamps to bound with"})
    return {"violations": violations, "unbounded": unbounded, "bounded_ok": ok}


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--path", default=TARGET)
    args = ap.parse_args()

    res = audit(args.path)
    if args.json:
        print(json.dumps(res, indent=2))
        return 1 if res["violations"] else 0

    print(f"LOOK-AHEAD AUDIT  {os.path.basename(args.path)}")
    print()
    print(f"BOUNDED — safe under --include-post-exit ({len(res['bounded_ok'])})")
    for r in res["bounded_ok"]:
        print(f"  ok    {r['fn']:<42} line {r['def_line']}")
    print()
    print(f"STRUCTURALLY UNBOUNDED — do NOT read these with --include-post-exit "
          f"({len(res['unbounded'])})")
    for r in res["unbounded"]:
        print(f"  warn  {r['fn']:<42} line {r['def_line']}  ({r['reason']})")
    print()
    if res["violations"]:
        print(f"VIOLATIONS — could be bounded, is not ({len(res['violations'])})")
        for r in res["violations"]:
            where = f", unbounded call(s) at {r['bad_call_lines']}" if r["bad_call_lines"] else ""
            print(f"  FAIL  {r['fn']:<42} def line {r['def_line']}{where}")
            print(f"        {r['why']}")
        print()
        print("  Under --include-post-exit these families book entries on positions")
        print("  that were already closed. bor_ was exactly this and it was the only")
        print("  profitable family in the book until it was bounded.")
        return 1

    print("VIOLATIONS  none — every function that can bound itself is called that way.")
    print()
    print("Reminder: 'no violations' does NOT mean every number is post-exit safe.")
    print("The structurally unbounded list above still must be read WITHOUT the flag.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
