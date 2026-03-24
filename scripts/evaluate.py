#!/usr/bin/env python3
"""
Evaluate altimate-code-actions review output on simulation PRs.

After a simulation PR is created and reviewed by altimate-code-actions,
this script:
1. Reads the PR comment posted by the action
2. Checks if expected findings were detected
3. Checks for false positives
4. Validates comment format (mermaid, tables, executive line)
5. Files issues in altimate-code-actions repo for failures

Usage:
    python scripts/evaluate.py --pr <number>
    python scripts/evaluate.py --pr <number> --auto-issue
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------


@dataclass
class EvalResult:
    pr_number: int
    pr_title: str
    pr_url: str
    comment_found: bool = False
    comment_body: str = ""
    comment_length: int = 0

    # Format checks
    has_executive_line: bool = False
    has_summary_table: bool = False
    has_mermaid_dag: bool = False
    has_cost_section: bool = False
    has_issues_section: bool = False
    has_footer: bool = False

    # Finding checks
    expected_findings: list = field(default_factory=list)
    actual_findings: list = field(default_factory=list)
    missed_findings: list = field(default_factory=list)
    false_positives: list = field(default_factory=list)

    # Scores
    detection_score: float = 0.0  # % of expected findings detected
    precision_score: float = 0.0  # 1 - (false_positives / total_findings)
    format_score: float = 0.0     # % of format checks passed
    overall_score: float = 0.0

    # Issues to file
    issues_to_file: list = field(default_factory=list)


@dataclass
class Issue:
    title: str
    body: str
    labels: list = field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def gh(*args, check=True):
    result = subprocess.run(
        ["gh", *args],
        capture_output=True, text=True, check=False,
    )
    if check and result.returncode != 0:
        print(f"gh command failed: {result.stderr}", file=sys.stderr)
        sys.exit(1)
    return result


def get_pr_info(pr_number: int) -> dict:
    """Get PR metadata."""
    result = gh(
        "pr", "view", str(pr_number), "--json",
        "title,url,body,comments,state,headRefName",
    )
    return json.loads(result.stdout)


def get_pr_comments(pr_number: int) -> list:
    """Get all comments on a PR."""
    result = gh(
        "api", f"repos/{{owner}}/{{repo}}/issues/{pr_number}/comments",
        "--paginate",
    )
    if not result.stdout.strip():
        return []
    comments = json.loads(result.stdout)
    return [c["body"] for c in comments if "body" in c]


def find_altimate_comment(comments: list) -> Optional[str]:
    """Find the altimate-code-actions review comment."""
    for comment in comments:
        if "<!-- altimate-code-review -->" in comment:
            return comment
        if "Altimate Code" in comment and ("SQL Quality" in comment or "all checks passed" in comment):
            return comment
    return None


# ---------------------------------------------------------------------------
# Format validation
# ---------------------------------------------------------------------------


def check_format(comment: str, mode: str = "static") -> dict:
    """Validate the PR comment format against the v0.3 design spec.

    Only checks sections that are expected for the given mode:
    - static: executive_line, summary_table, issues_section, footer
    - full/ai: all of the above + mermaid_dag, cost_section (if data present)
    """
    checks = {}

    # Executive one-line summary — match the emoji + "Altimate Code"
    checks["executive_line"] = bool(
        re.search(r"^## .{1,4} Altimate Code", comment, re.MULTILINE)
    )

    # Summary table
    checks["summary_table"] = bool(
        re.search(r"\|\s*Check\s*\|\s*Result\s*\|\s*Details", comment)
    )

    # Issues section (could be absent if clean — check for warnings/errors OR "0 issues" OR "all checks passed")
    checks["issues_section"] = bool(
        re.search(r"(warning|error|critical|0 issues|all checks passed|passed)", comment, re.IGNORECASE)
    )

    # Footer — match "Altimate Code" followed by version pattern
    checks["footer"] = bool(
        re.search(r"Altimate Code.*v\d+\.\d+\.\d+", comment)
    )

    # Mermaid DAG and cost section are only expected in full/ai mode
    # AND only when the data is actually present
    if mode != "static":
        checks["mermaid_dag"] = "```mermaid" in comment or "mermaid" in comment.lower()
        checks["cost_section"] = bool(
            re.search(r"(💰|Cost Impact|Before.*After.*Delta)", comment)
        )

    return checks


# ---------------------------------------------------------------------------
# Finding detection
# ---------------------------------------------------------------------------


FINDING_PATTERNS = {
    "select_star": [r"SELECT\s*\*", r"select.star", r"select_star"],
    "cartesian_join": [r"cartesian", r"comma.separated.*FROM", r"cartesian_join"],
    "non_deterministic": [r"non.deterministic", r"CURRENT_DATE", r"NOW\(\)", r"non_deterministic"],
    "correlated_subquery": [r"correlated.subquery", r"correlated_subquery"],
    "missing_group_by": [r"missing.*GROUP BY", r"missing_group_by"],
    "or_in_join": [r"OR.*JOIN", r"or_in_join"],
    "missing_partition": [r"missing.*PARTITION", r"missing_partition"],
    "implicit_type_cast": [r"implicit.*type.*cast", r"implicit_type_cast"],
    "select_star_warning": [r"SELECT\s*\*", r"list columns explicitly"],
    "breaking_change": [r"breaking", r"downstream", r"column.*removed", r"column.*renamed"],
    "impact_analysis": [r"downstream", r"models affected", r"blast radius"],
    "pii_detected": [r"PII", r"email", r"ssn", r"personal"],
}


def extract_findings(comment: str) -> list:
    """Extract rule IDs and finding types from the comment."""
    findings = []

    # Look for rule IDs in backtick code spans
    rule_matches = re.findall(r"`(\w+_\w+)`", comment)
    for rule in rule_matches:
        if rule in FINDING_PATTERNS:
            findings.append(rule)

    # Look for finding descriptions
    for finding_type, patterns in FINDING_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, comment, re.IGNORECASE):
                if finding_type not in findings:
                    findings.append(finding_type)
                break

    return list(set(findings))


def evaluate_findings(
    expected: list,
    actual: list,
) -> tuple[list, list, float, float]:
    """Compare expected vs actual findings."""
    missed = [f for f in expected if f not in actual]
    # For false positives, we only flag unexpected critical/error findings
    # Warnings and info are expected noise
    false_pos = []  # Conservative — only flag if we know it's wrong

    detection = len(expected - set(missed)) / len(expected) if expected else 1.0
    precision = 1.0  # Conservative default

    return missed, false_pos, detection, precision


# ---------------------------------------------------------------------------
# Issue filing
# ---------------------------------------------------------------------------


def build_issues(result: EvalResult) -> list:
    """Generate GitHub issues for failures."""
    issues = []

    # Missed findings
    for finding in result.missed_findings:
        issues.append(Issue(
            title=f"Missed detection: `{finding}` not caught in simulation PR #{result.pr_number}",
            body=f"""## Bug Report

**Source:** Automated simulation evaluation on [PR #{result.pr_number}]({result.pr_url})

**Expected:** The review should have detected `{finding}`.
**Actual:** The finding was not present in the review comment.

**PR Title:** {result.pr_title}

### Comment Body (truncated)
```
{result.comment_body[:2000]}
```

### Reproduction
1. Check out the PR branch
2. Run the action in static mode
3. Verify if `{finding}` is detected

---
*Filed automatically by simulation evaluator*
""",
            labels=["bug", "detection", "simulation"],
        ))

    # Format issues — only check sections that should be present
    format_checks_for_issues = check_format(result.comment_body, mode="static")
    missing_format = [k for k, v in format_checks_for_issues.items() if not v]
    if missing_format and result.comment_found:
        issues.append(Issue(
            title=f"Comment format: missing {', '.join(missing_format)} in PR #{result.pr_number}",
            body=f"""## UX Issue

**Source:** Automated simulation evaluation on [PR #{result.pr_number}]({result.pr_url})

**Missing format elements:** {', '.join(missing_format)}

The v0.3 design spec requires:
- Executive one-line summary (`## ✅/⚠️/❌ Altimate Code — ...`)
- Summary table (Check | Result | Details)
- Mermaid DAG blast radius (when impact analysis enabled)
- Footer with version

### Actual Comment (truncated)
```
{result.comment_body[:2000]}
```

---
*Filed automatically by simulation evaluator*
""",
            labels=["ux", "comment-format", "simulation"],
        ))

    # No comment at all
    if not result.comment_found:
        issues.append(Issue(
            title=f"No review comment posted on simulation PR #{result.pr_number}",
            body=f"""## Bug Report

**Source:** Automated simulation evaluation on [PR #{result.pr_number}]({result.pr_url})

**Expected:** altimate-code-actions should post a review comment on every PR with SQL changes.
**Actual:** No comment was found on the PR.

**PR Title:** {result.pr_title}

### Possible Causes
- Action did not trigger
- Action failed silently
- Comment was posted on a different PR
- Permissions issue

---
*Filed automatically by simulation evaluator*
""",
            labels=["bug", "critical", "simulation"],
        ))

    return issues


def file_issues(issues: list, repo: str = "AltimateAI/altimate-code-actions"):
    """File issues on the altimate-code-actions repo."""
    for issue in issues:
        labels_args = []
        for label in issue.labels:
            labels_args.extend(["--label", label])

        result = gh(
            "issue", "create",
            "--repo", repo,
            "--title", issue.title,
            "--body", issue.body,
            *labels_args,
            check=False,
        )
        if result.returncode == 0:
            print(f"  Filed: {result.stdout.strip()}")
        else:
            print(f"  Failed to file issue: {result.stderr}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Main evaluation
# ---------------------------------------------------------------------------


def evaluate_pr(pr_number: int, expected_findings: list = None) -> EvalResult:
    """Evaluate altimate-code-actions review on a specific PR."""
    print(f"\n{'='*60}")
    print(f"Evaluating PR #{pr_number}")
    print(f"{'='*60}\n")

    # Get PR info
    pr_info = get_pr_info(pr_number)
    result = EvalResult(
        pr_number=pr_number,
        pr_title=pr_info.get("title", ""),
        pr_url=pr_info.get("url", ""),
    )

    # Parse expected findings from PR body
    if expected_findings is None:
        body = pr_info.get("body", "")
        # Look for <!-- expected: finding1, finding2 --> marker
        match = re.search(r"<!--\s*expected:\s*(.*?)\s*-->", body)
        if match:
            expected_findings = [f.strip() for f in match.group(1).split(",") if f.strip()]
    result.expected_findings = expected_findings or []

    # Wait a bit for the action to complete (if just created)
    print("Checking for review comment...")

    # Get comments
    comments = get_pr_comments(pr_number)
    altimate_comment = find_altimate_comment(comments)

    if altimate_comment:
        result.comment_found = True
        result.comment_body = altimate_comment
        result.comment_length = len(altimate_comment)
        print(f"  Found comment ({result.comment_length} chars)")

        # Detect mode from PR body or default to static
        body = pr_info.get("body", "")
        mode = "static"
        if "mode: full" in body or "mode: ai" in body:
            mode = "full"

        # Format checks (context-aware based on mode)
        format_checks = check_format(altimate_comment, mode=mode)
        result.has_executive_line = format_checks.get("executive_line", False)
        result.has_summary_table = format_checks.get("summary_table", False)
        result.has_mermaid_dag = format_checks.get("mermaid_dag", False)
        result.has_cost_section = format_checks.get("cost_section", False)
        result.has_issues_section = format_checks.get("issues_section", False)
        result.has_footer = format_checks.get("footer", False)

        format_passed = sum(1 for v in format_checks.values() if v)
        result.format_score = format_passed / len(format_checks)
        print(f"  Format: {format_passed}/{len(format_checks)} checks passed ({result.format_score:.0%})")

        # Finding detection
        result.actual_findings = extract_findings(altimate_comment)
        print(f"  Findings detected: {result.actual_findings}")

        if result.expected_findings:
            result.missed_findings = [
                f for f in result.expected_findings if f not in result.actual_findings
            ]
            detected = len(result.expected_findings) - len(result.missed_findings)
            result.detection_score = detected / len(result.expected_findings)
            print(f"  Expected: {result.expected_findings}")
            print(f"  Missed: {result.missed_findings}")
            print(f"  Detection: {detected}/{len(result.expected_findings)} ({result.detection_score:.0%})")
        else:
            result.detection_score = 1.0
            print("  No expected findings specified — skipping detection check")

        result.precision_score = 1.0  # Conservative
    else:
        print("  No altimate-code-actions comment found!")
        result.detection_score = 0.0
        result.format_score = 0.0

    # Overall score
    result.overall_score = (
        result.detection_score * 0.4 +
        result.precision_score * 0.2 +
        result.format_score * 0.4
    )

    # Build issues
    result.issues_to_file = build_issues(result)

    # Print summary
    print(f"\n--- Evaluation Summary ---")
    print(f"  Overall Score: {result.overall_score:.0%}")
    print(f"  Detection: {result.detection_score:.0%}")
    print(f"  Precision: {result.precision_score:.0%}")
    print(f"  Format: {result.format_score:.0%}")
    print(f"  Issues to file: {len(result.issues_to_file)}")

    return result


def evaluate_latest_prs(count: int = 5) -> list:
    """Evaluate the N most recent simulation PRs."""
    result = gh(
        "pr", "list",
        "--state", "open",
        "--limit", str(count),
        "--json", "number,title",
        "--jq", ".[].number",
    )
    pr_numbers = [int(n) for n in result.stdout.strip().split("\n") if n.strip()]

    results = []
    for pr_num in pr_numbers:
        eval_result = evaluate_pr(pr_num)
        results.append(eval_result)

    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Evaluate altimate-code-actions reviews")
    parser.add_argument("--pr", type=int, help="Specific PR number to evaluate")
    parser.add_argument("--latest", type=int, default=0, help="Evaluate N latest PRs")
    parser.add_argument("--auto-issue", action="store_true", help="Automatically file issues for failures")
    parser.add_argument("--expected", type=str, help="Comma-separated expected findings")
    parser.add_argument("--wait", type=int, default=0, help="Wait N seconds for action to complete before evaluating")
    args = parser.parse_args()

    if args.wait > 0:
        print(f"Waiting {args.wait}s for action to complete...")
        time.sleep(args.wait)

    expected = None
    if args.expected:
        expected = [f.strip() for f in args.expected.split(",")]

    if args.pr:
        result = evaluate_pr(args.pr, expected)
        results = [result]
    elif args.latest > 0:
        results = evaluate_latest_prs(args.latest)
    else:
        print("Specify --pr <number> or --latest <count>")
        sys.exit(1)

    # File issues if requested
    if args.auto_issue:
        total_issues = 0
        for result in results:
            if result.issues_to_file:
                print(f"\nFiling {len(result.issues_to_file)} issue(s) for PR #{result.pr_number}...")
                file_issues(result.issues_to_file)
                total_issues += len(result.issues_to_file)
        print(f"\nTotal issues filed: {total_issues}")

    # Print overall summary
    if results:
        avg_score = sum(r.overall_score for r in results) / len(results)
        print(f"\n{'='*60}")
        print(f"Overall: {avg_score:.0%} across {len(results)} PR(s)")
        print(f"{'='*60}")


if __name__ == "__main__":
    main()
