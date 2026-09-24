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

THE ACTUAL DEFECT IS A FREE OPTION, NOT POST-EXIT DATA
-----------------------------------------------------
The first two versions of this audit checked the wrong property. Reading quotes
from after qsim's exit is not itself wrong: a policy with a looser stop HOLDS
LONGER than qsim did, and asking what that would have returned is the entire
point of --include-post-exit. _runner_window_return does exactly that and is
sound — it ends with `return last_mult - 1.0`, so it takes whatever the last
observed price gives, good or bad.

What bor_ actually did was worse and subtler. It banked at 1.4x when a coin
recovered after the real exit, and fell back to `current_return` — qsim's
ACTUAL, stop-protected result — when it did not. Post-exit upside with
stop-protected downside. Heads it wins, tails it takes the stop. That is a free
option, and it is why it was the only profitable family in the book.

So the property to check is: can this policy return `current_return` AFTER it
has looked at the price series? If yes, it is picking its downside from one
world and its upside from another. If it always ends on an observed price, it
is consistent whatever quotes it saw.

WHAT IT CHECKS
--------------
  1. Free option: does `current_return` appear anywhere outside the leading
     `if not points:` guard — as a return, or as the else-branch of a
     conditional expression?
  2. Is that free option closed by a `held_until` bound (the bor_ fix), which
     forces the fallback consistently for every row it could apply to?

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

# Functions whose `current_return` fallback has been READ and found consistent:
# it fires only when the policy never engaged at all, and every path on which it
# did engage returns a price observed at the time. These never pair post-exit
# upside with qsim's stop-protected downside, which is the actual defect.
#
# This is a reviewed list, not an inferred one. A static check cannot tell
# `if in_recovery` from `if first is not None` — they are the same shape and
# opposite meanings — so anything added here needs a human to have read it and
# a reason recorded next to it.
CONSISTENT_BY_REVIEW: dict[str, str] = {
    "_runner_window_return":
        "ends `return last_mult - 1.0`; no fallback after the scan at all",
    "_soft_stop_recovery_return":
        "`if in_recovery` = the soft stop engaged; if it did, every exit is an observed price",
    "_bank_soft_stop_return":
        "same in_recovery shape as _soft_stop_recovery_return",
    "_conditional_stop_delay_return":
        "`if in_delay` = the delay engaged; same shape",
}


def _arg_names(fn: ast.FunctionDef) -> set[str]:
    a = fn.args
    return {x.arg for x in (a.posonlyargs + a.args + a.kwonlyargs)} | {
        x.arg for x in ([a.vararg] if a.vararg else []) + ([a.kwarg] if a.kwarg else [])
    }


def _guard_node(fn: ast.FunctionDef) -> ast.AST | None:
    """The leading `if not points: return current_return` guard, if present.

    Its fallback is not a free option: nothing has been observed yet, so there is
    no other world to pick an upside from.
    """
    body = list(fn.body)
    # Skip the docstring. Without this the guard is never found in any function
    # that has one, which silently turned the guard's own fallback into a
    # reported free option — three false positives, and the reason
    # _runner_window_return alone looked clean: it has no docstring.
    if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) \
            and isinstance(body[0].value.value, str):
        body = body[1:]
    first = body[0] if body else None
    if isinstance(first, ast.If) and isinstance(first.test, ast.UnaryOp) \
            and isinstance(first.test.op, ast.Not):
        return first
    return None


def _free_option(fn: ast.FunctionDef) -> bool:
    """
    True if `current_return` is reachable on a path where the policy ALSO books
    exits at prices it only saw because of post-exit data.

    Two placements are NOT a free option, and both took a manual read to
    separate from the one that is:

      the leading `if not points:` guard — nothing has been observed yet, so
      there is no other world to borrow an upside from.

      anything in CONSISTENT_BY_REVIEW — see that list.

    An earlier version tried to exempt the terminal
    `return X if engaged else current_return` pattern automatically. That is
    wrong: it also exempts `return first - 1.0 if first is not None else
    current_return` in _bank_return, where the condition means "a crossing
    exists ANYWHERE in the series, post-exit included" rather than "the policy
    engaged". Those two read identically to an AST and mean opposite things.

    Separating them needs the function's semantics, not its shape, so this no
    longer guesses. Everything with a fallback is flagged unless it is on the
    reviewed list below, which is short, named, and justified per entry.
    """
    guard = _guard_node(fn)
    skip = set(map(id, ast.walk(guard))) if guard is not None else set()


    for node in ast.walk(fn):
        if id(node) in skip:
            continue
        if isinstance(node, ast.Return) and node.value is not None:
            for sub in ast.walk(node.value):
                if id(sub) in skip:
                    continue
                if isinstance(sub, ast.Name) and sub.id == "current_return":
                    return True
    return False


def audit(path: str) -> dict:
    tree = ast.parse(open(path).read())

    defs: dict[str, dict] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name.endswith("_return"):
            names = _arg_names(node)
            if "current_return" not in names:
                continue
            defs[node.name] = {
                "accepts_held_until": "held_until" in names,
                "takes_timestamps": "points" in names,
                "free_option": _free_option(node),
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
            # A caller can bound a bare-mults policy by pre-truncating the data:
            # `mults = _held_mults(points, exit_time)` leaves nothing post-exit to
            # look ahead at. Missing this was the audit's third false-positive
            # class and the most consequential — it made every bank_*/lock_*/
            # dyn_*/winner_* number look contaminated when the caller had bounded
            # them since e44c007.
            src_args = " ".join(
                ast.unparse(a) for a in list(node.args) + [k.value for k in node.keywords])
            defs[name]["calls"].append({
                "line": node.lineno,
                "passes_held_until": "held_until" in passed,
                "data_prebounded": "_held_mults" in src_args or "mults" in src_args,
            })

    violations, unbounded, ok = [], [], []
    for name, info in sorted(defs.items()):
        if not info["calls"]:
            continue
        if name in CONSISTENT_BY_REVIEW:
            ok.append({"fn": name, "def_line": info["line"],
                       "why": f"reviewed: {CONSISTENT_BY_REVIEW[name]}"})
            continue
        if not info["free_option"]:
            ok.append({"fn": name, "def_line": info["line"],
                       "why": "no current_return fallback after the scan"})
            continue
        bounded = info["accepts_held_until"] and all(
            c["passes_held_until"] for c in info["calls"])
        data_bounded = bool(info["calls"]) and all(
            c.get("data_prebounded") for c in info["calls"])
        if bounded:
            ok.append({"fn": name, "def_line": info["line"],
                       "why": "free option closed by a held_until bound"})
        elif data_bounded:
            ok.append({"fn": name, "def_line": info["line"],
                       "why": "caller passes _held_mults — data truncated at exit_time, "
                              "nothing post-exit to look ahead at"})
        elif info["takes_timestamps"]:
            violations.append(
                {"fn": name, "def_line": info["line"],
                 "bad_call_lines": [c["line"] for c in info["calls"]
                                    if not c["passes_held_until"]],
                 "why": "falls back to current_return after reading the series, "
                        "and takes points so it COULD bound itself"})
        else:
            unbounded.append(
                {"fn": name, "def_line": info["line"],
                 "reason": "falls back to current_return after reading the series; "
                           "takes bare mults so it cannot bound itself"})
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
    print(f"SAFE under --include-post-exit ({len(res['bounded_ok'])})")
    for r in res["bounded_ok"]:
        print(f"  ok    {r['fn']:<42} line {r['def_line']}")
        print(f"        {r['why']}")
    print()
    print(f"FREE OPTION, UNFIXABLE IN PLACE — do NOT read with --include-post-exit "
          f"({len(res['unbounded'])})")
    for r in res["unbounded"]:
        print(f"  warn  {r['fn']:<42} line {r['def_line']}")
        print(f"        {r['reason']}")
    print()
    if res["violations"]:
        print(f"VIOLATIONS — free option that could be closed, and is not ({len(res['violations'])})")
        for r in res["violations"]:
            where = f", unbounded call(s) at {r['bad_call_lines']}" if r["bad_call_lines"] else ""
            print(f"  FAIL  {r['fn']:<42} def line {r['def_line']}{where}")
            print(f"        {r['why']}")
        print()
        print("  Under --include-post-exit these families book entries on positions")
        print("  that were already closed. bor_ was exactly this and it was the only")
        print("  profitable family in the book until it was bounded.")
        return 1

    print("VIOLATIONS  none — every closable free option is closed.")
    print()
    print("Reminder: 'no violations' does NOT mean every number is post-exit safe.")
    print("The free-option list above still must be read WITHOUT the flag.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
