#!/usr/bin/env python3
"""
Simulation engine for altimate-bigquery-demo.

Creates realistic PRs with dbt model changes to test altimate-code-actions.
Each scenario produces different changes every time via random model selection,
random column names, random parameters, etc.
"""

import argparse
import os
import random
import re
import string
import subprocess
import sys
import textwrap
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

STAGING_DIR = PROJECT_ROOT / "models" / "staging"
INTERMEDIATE_DIR = PROJECT_ROOT / "models" / "intermediate"
MARTS_DIR = PROJECT_ROOT / "models" / "marts"
REPORTING_DIR = PROJECT_ROOT / "models" / "reporting"
MACROS_DIR = PROJECT_ROOT / "macros"

TIMESTAMP = datetime.now().strftime("%Y%m%d%H%M")

# All available scenarios grouped by category
SCENARIOS = {
    "refactor": [
        "refactor_cte_to_subquery",
        "refactor_subquery_to_cte",
        "rename_column",
        "change_materialization",
        "extract_staging_model",
    ],
    "bugfix": [
        "fix_null_handling",
        "fix_join_condition",
        "fix_duplicate_rows",
        "fix_date_filter",
        "fix_type_cast",
    ],
    "feature": [
        "add_new_metric",
        "add_new_model",
        "add_new_source",
        "add_incremental",
        "add_snapshot",
    ],
    "optimization": [
        "add_partition",
        "remove_select_star",
        "optimize_join",
        "add_where_clause",
        "convert_to_incremental",
    ],
    "anti_pattern": [
        "introduce_select_star",
        "introduce_cartesian_join",
        "introduce_non_deterministic",
        "introduce_or_in_join",
        "remove_tests",
    ],
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sql_files(directory: Path) -> list[Path]:
    """Return SQL files in a directory, excluding partials."""
    return sorted(f for f in directory.glob("*.sql") if not f.name.startswith("_"))


def _yml_files(directory: Path) -> list[Path]:
    """Return YAML files in a directory."""
    return sorted(directory.glob("*.yml"))


def _rand_suffix() -> str:
    return "".join(random.choices(string.ascii_lowercase, k=4))


def _pick_random_model(directory: Path) -> Path:
    """Pick a random SQL model from a directory."""
    models = _sql_files(directory)
    if not models:
        raise RuntimeError(f"No SQL models found in {directory}")
    return random.choice(models)


def _find_cte_names(content: str) -> list[str]:
    """Extract CTE names from SQL content."""
    return re.findall(r"(\w+)\s+as\s*\(", content, re.IGNORECASE)


def _find_columns_in_select(content: str) -> list[str]:
    """Extract column names/expressions from the final SELECT."""
    # Find the last select statement
    selects = list(re.finditer(r"\bselect\b", content, re.IGNORECASE))
    if not selects:
        return []
    last_select = selects[-1]
    remainder = content[last_select.end() :]
    # Find the FROM
    from_match = re.search(r"\bfrom\b", remainder, re.IGNORECASE)
    if not from_match:
        return []
    cols_text = remainder[: from_match.start()]
    # Split by comma and extract column aliases or names
    cols = []
    for part in cols_text.split(","):
        part = part.strip()
        if not part or part == "*":
            continue
        # Look for alias (AS keyword or last word)
        alias_match = re.search(r"\bas\s+(\w+)\s*$", part, re.IGNORECASE)
        if alias_match:
            cols.append(alias_match.group(1))
        else:
            # Last word-like token
            tokens = re.findall(r"\w+", part)
            if tokens:
                cols.append(tokens[-1])
    return cols


# ---------------------------------------------------------------------------
# Refactor scenarios
# ---------------------------------------------------------------------------


def refactor_cte_to_subquery():
    """Convert a CTE to an inline subquery (anti-pattern for readability)."""
    model = _pick_random_model(random.choice([INTERMEDIATE_DIR, MARTS_DIR]))
    content = model.read_text()
    ctes = _find_cte_names(content)

    if len(ctes) < 2:
        # Fallback: just add a redundant subquery wrapper
        content = content.rstrip()
        new_content = (
            f"-- Refactored: wrapped final query in subquery\n"
            f"select * from (\n{content}\n) as subq_{_rand_suffix()}\n"
        )
        model.write_text(new_content)
    else:
        # Pick the first CTE and convert it to a subquery
        target_cte = ctes[0]
        # Find the CTE definition
        pattern = rf"(\b{target_cte}\b)\s+as\s*\("
        match = re.search(pattern, content, re.IGNORECASE)
        if match:
            # Find matching closing paren
            start = match.end()
            depth = 1
            pos = start
            while pos < len(content) and depth > 0:
                if content[pos] == "(":
                    depth += 1
                elif content[pos] == ")":
                    depth -= 1
                pos += 1
            cte_body = content[start : pos - 1].strip()
            # Replace usages of the CTE name with the subquery
            new_content = content.replace(
                f"from {target_cte}",
                f"from ({cte_body}) as {target_cte}",
            )
            # Remove the CTE definition from the WITH clause
            # This is a simplified approach
            new_content = re.sub(
                rf"\b{target_cte}\b\s+as\s*\([^)]*\)\s*,?\s*",
                "",
                new_content,
                count=1,
                flags=re.IGNORECASE | re.DOTALL,
            )
            model.write_text(new_content)
        else:
            model.write_text(content + "\n-- refactor: inline CTE attempted\n")

    return {
        "branch": f"sim/refactor-cte-{model.stem}-{TIMESTAMP}",
        "title": f"refactor: inline CTE in {model.stem}",
        "body": (
            f"Converted a CTE to an inline subquery in `{model.name}` "
            f"for experimentation.\n\n"
            f"This is a simulated PR for testing altimate-code-actions."
        ),
        "files": [str(model)],
    }


def refactor_subquery_to_cte():
    """Wrap a FROM clause table reference in a new CTE."""
    model = _pick_random_model(random.choice([MARTS_DIR, REPORTING_DIR]))
    content = model.read_text()

    # Find ref() calls and wrap one in a new CTE
    refs = re.findall(r"\{\{\s*ref\('(\w+)'\)\s*\}\}", content)
    if not refs:
        model.write_text(content + "\n-- refactor: no refs to extract\n")
    else:
        target_ref = random.choice(refs)
        cte_name = f"base_{target_ref}"
        # Add a new CTE at the top
        new_cte = f"{cte_name} as (\n\n    select * from {{{{ ref('{target_ref}') }}}}\n\n)"
        if content.strip().lower().startswith("with"):
            # Add after WITH
            content = content.replace("with ", f"with {new_cte},\n\n", 1)
        else:
            content = f"with {new_cte}\n\n{content}"
        # Replace the ref in the original location
        content = content.replace(
            f"{{{{ ref('{target_ref}') }}}}",
            cte_name,
            1,  # Only replace the second occurrence (first is in our new CTE)
        )
        model.write_text(content)

    return {
        "branch": f"sim/refactor-cte-extract-{model.stem}-{TIMESTAMP}",
        "title": f"refactor: extract CTE in {model.stem}",
        "body": (
            f"Extracted a ref into a dedicated CTE in `{model.name}` "
            f"for clarity.\n\n"
            f"This is a simulated PR for testing altimate-code-actions."
        ),
        "files": [str(model)],
    }


def rename_column():
    """Rename a column alias (introduces a breaking change downstream)."""
    model = _pick_random_model(random.choice([STAGING_DIR, INTERMEDIATE_DIR]))
    content = model.read_text()

    # Find 'as <alias>' patterns
    aliases = re.findall(r"\bas\s+(\w+)", content, re.IGNORECASE)
    # Filter out CTE aliases and SQL keywords
    sql_keywords = {
        "select", "from", "where", "group", "order", "by", "and", "or",
        "on", "join", "left", "inner", "right", "case", "when", "then",
        "else", "end", "with", "true", "false", "null", "not", "in",
        "between", "like", "is", "date", "timestamp", "numeric", "int64",
    }
    column_aliases = [a for a in aliases if a.lower() not in sql_keywords and not a.startswith("(")]
    if not column_aliases:
        model.write_text(content + f"\n-- rename: no aliases found\n")
    else:
        target = random.choice(column_aliases)
        new_name = f"{target}_v2"
        content = content.replace(f"as {target}", f"as {new_name}", 1)
        model.write_text(content)

    return {
        "branch": f"sim/rename-col-{model.stem}-{TIMESTAMP}",
        "title": f"refactor: rename column in {model.stem}",
        "body": (
            f"Renamed a column in `{model.name}`. This may be a breaking change "
            f"for downstream models.\n\n"
            f"This is a simulated PR for testing altimate-code-actions."
        ),
        "files": [str(model)],
    }


def change_materialization():
    """Change a model's materialization config."""
    model = _pick_random_model(random.choice([INTERMEDIATE_DIR, MARTS_DIR]))
    content = model.read_text()

    materializations = ["view", "table", "ephemeral"]
    chosen = random.choice(materializations)
    config_line = f"{{{{ config(materialized='{chosen}') }}}}\n\n"

    if "config(" in content:
        content = re.sub(
            r"\{\{.*?config\(.*?\).*?\}\}\s*\n*",
            config_line,
            content,
            count=1,
        )
    else:
        content = config_line + content

    model.write_text(content)

    return {
        "branch": f"sim/materialization-{model.stem}-{TIMESTAMP}",
        "title": f"refactor: change {model.stem} to {chosen}",
        "body": (
            f"Changed materialization of `{model.name}` to `{chosen}`.\n\n"
            f"This is a simulated PR for testing altimate-code-actions."
        ),
        "files": [str(model)],
    }


def extract_staging_model():
    """Create a new staging model by splitting logic from an existing one."""
    source_model = _pick_random_model(STAGING_DIR)
    suffix = _rand_suffix()
    new_model_name = f"stg_{suffix}_extract"
    new_model_path = STAGING_DIR / f"{new_model_name}.sql"

    new_content = textwrap.dedent(f"""\
        -- Extracted from {source_model.stem}
        with base as (

            select * from {{{{ ref('{source_model.stem}') }}}}

        ),

        filtered as (

            select
                *
            from base
            where created_at >= '2023-01-01'

        )

        select * from filtered
    """)

    new_model_path.write_text(new_content)

    return {
        "branch": f"sim/extract-staging-{suffix}-{TIMESTAMP}",
        "title": f"refactor: extract staging model from {source_model.stem}",
        "body": (
            f"Created `{new_model_name}.sql` by extracting filtered logic "
            f"from `{source_model.name}`.\n\n"
            f"This is a simulated PR for testing altimate-code-actions."
        ),
        "files": [str(new_model_path)],
    }


# ---------------------------------------------------------------------------
# Bugfix scenarios
# ---------------------------------------------------------------------------


def fix_null_handling():
    """Add COALESCE wrappers to handle NULL values."""
    model = _pick_random_model(random.choice([INTERMEDIATE_DIR, MARTS_DIR]))
    content = model.read_text()

    # Find columns that could benefit from COALESCE
    # Look for patterns like "sum(x) as y" or "count(x) as y"
    agg_pattern = r"(sum|avg|count|min|max)\(([^)]+)\)\s+as\s+(\w+)"
    matches = list(re.finditer(agg_pattern, content, re.IGNORECASE))

    if matches:
        target = random.choice(matches)
        func, expr, alias = target.group(1), target.group(2), target.group(3)
        old = target.group(0)
        new = f"coalesce({func}({expr}), 0) as {alias}"
        content = content.replace(old, new, 1)
    else:
        # Add a coalesce to any column reference
        cols = _find_columns_in_select(content)
        if cols:
            target_col = random.choice(cols)
            content = content.replace(
                target_col,
                f"coalesce({target_col}, 0) as {target_col}",
                1,
            )

    model.write_text(content)

    return {
        "branch": f"sim/fix-null-{model.stem}-{TIMESTAMP}",
        "title": f"fix: handle null values in {model.stem}",
        "body": (
            f"Added COALESCE to handle potential NULL values in `{model.name}`.\n\n"
            f"This is a simulated PR for testing altimate-code-actions."
        ),
        "files": [str(model)],
    }


def fix_join_condition():
    """Fix or add a join condition."""
    model = _pick_random_model(random.choice([INTERMEDIATE_DIR, MARTS_DIR]))
    content = model.read_text()

    # Find existing joins and add a redundant condition
    join_match = re.search(
        r"((?:left|inner|right|full)\s+join\s+\w+\s+\w+)\s+on\s+([^\n]+)",
        content,
        re.IGNORECASE,
    )
    if join_match:
        old_join = join_match.group(0)
        condition = join_match.group(2).strip()
        # Add a redundant IS NOT NULL check
        alias_match = re.search(r"(\w+)\.\w+\s*=", condition)
        if alias_match:
            alias = alias_match.group(1)
            new_join = f"{old_join}\n        and {alias}.{random.choice(['customer_id', 'order_id', 'product_id'])} is not null"
            content = content.replace(old_join, new_join, 1)

    model.write_text(content)

    return {
        "branch": f"sim/fix-join-{model.stem}-{TIMESTAMP}",
        "title": f"fix: tighten join condition in {model.stem}",
        "body": (
            f"Added an additional join filter in `{model.name}` to prevent "
            f"NULL key matches.\n\n"
            f"This is a simulated PR for testing altimate-code-actions."
        ),
        "files": [str(model)],
    }


def fix_duplicate_rows():
    """Add DISTINCT or qualify to prevent duplicates."""
    model = _pick_random_model(random.choice([MARTS_DIR, REPORTING_DIR]))
    content = model.read_text()

    # Find the final select and add DISTINCT
    content = re.sub(
        r"\bselect\b(?!\s+distinct)(?!\s+\*\s+from\s*\()",
        "select distinct",
        content,
        count=1,
        flags=re.IGNORECASE,
    )

    model.write_text(content)

    return {
        "branch": f"sim/fix-dupes-{model.stem}-{TIMESTAMP}",
        "title": f"fix: prevent duplicate rows in {model.stem}",
        "body": (
            f"Added DISTINCT to the final select in `{model.name}` "
            f"to prevent duplicate rows.\n\n"
            f"This is a simulated PR for testing altimate-code-actions."
        ),
        "files": [str(model)],
    }


def fix_date_filter():
    """Add or fix a date range filter."""
    model = _pick_random_model(random.choice([INTERMEDIATE_DIR, REPORTING_DIR]))
    content = model.read_text()

    # Pick a random date column and add a filter
    date_cols = ["ordered_at", "created_at", "session_start", "order_date", "revenue_date"]
    date_col = random.choice(date_cols)
    days_back = random.choice([30, 60, 90, 180, 365])

    # Add WHERE clause before the final GROUP BY or at end
    filter_clause = (
        f"\n    where cast({date_col} as date) >= "
        f"date_sub(current_date(), interval {days_back} day)"
    )

    if "where" in content.lower():
        # Add as AND condition
        content = re.sub(
            r"(\bwhere\b\s+)",
            f"where cast({date_col} as date) >= "
            f"date_sub(current_date(), interval {days_back} day)\n        and ",
            content,
            count=1,
            flags=re.IGNORECASE,
        )
    else:
        # Add before GROUP BY or at end of last CTE
        group_match = re.search(r"\n(\s*group\s+by)", content, re.IGNORECASE)
        if group_match:
            content = content[: group_match.start()] + filter_clause + content[group_match.start() :]
        else:
            # Before final select
            content = content.rstrip() + filter_clause + "\n"

    model.write_text(content)

    return {
        "branch": f"sim/fix-date-{model.stem}-{TIMESTAMP}",
        "title": f"fix: add {days_back}-day date filter to {model.stem}",
        "body": (
            f"Added a date range filter to `{model.name}` to limit data "
            f"to the last {days_back} days.\n\n"
            f"This is a simulated PR for testing altimate-code-actions."
        ),
        "files": [str(model)],
    }


def fix_type_cast():
    """Add explicit type casts to prevent implicit conversion issues."""
    model = _pick_random_model(STAGING_DIR)
    content = model.read_text()

    # Find columns without casts and add one
    lines = content.split("\n")
    new_lines = []
    modified = False
    for line in lines:
        if not modified and re.match(r"\s+\w+\s+as\s+\w+", line) and "cast(" not in line.lower():
            # Wrap in a safe cast
            match = re.match(r"(\s+)(\w+)(\s+as\s+\w+.*)", line)
            if match:
                indent, col, rest = match.groups()
                line = f"{indent}safe_cast({col} as string){rest}"
                modified = True
        new_lines.append(line)

    model.write_text("\n".join(new_lines))

    return {
        "branch": f"sim/fix-cast-{model.stem}-{TIMESTAMP}",
        "title": f"fix: add explicit type cast in {model.stem}",
        "body": (
            f"Added explicit SAFE_CAST to `{model.name}` to prevent "
            f"implicit type conversion issues.\n\n"
            f"This is a simulated PR for testing altimate-code-actions."
        ),
        "files": [str(model)],
    }


# ---------------------------------------------------------------------------
# Feature scenarios
# ---------------------------------------------------------------------------


def add_new_metric():
    """Add a new calculated column to a mart model."""
    model = _pick_random_model(random.choice([MARTS_DIR, REPORTING_DIR]))
    content = model.read_text()

    metric_options = [
        ("days_since_last_order", "date_diff(current_date(), cast(last_order_at as date), day)"),
        ("revenue_per_session", "{{ safe_divide('total_revenue', 'total_sessions') }}"),
        ("items_per_order", "{{ safe_divide('total_items_purchased', 'total_orders') }}"),
        ("is_high_value", "case when total_revenue > 500 then true else false end"),
        ("order_frequency_score", "case when total_orders > 10 then 'high' when total_orders > 3 then 'medium' else 'low' end"),
        ("margin_pct", "{{ safe_divide('margin', 'retail_price') }}"),
        ("conversion_rate", "{{ safe_divide('1', 'total_sessions') }}"),
    ]

    metric_name, metric_expr = random.choice(metric_options)

    # Add before the final "from" in the last CTE
    lines = content.split("\n")
    insert_idx = None
    for i in range(len(lines) - 1, -1, -1):
        if re.match(r"\s+from\s+", lines[i], re.IGNORECASE):
            insert_idx = i
            break

    if insert_idx:
        indent = "        "
        lines.insert(insert_idx, f"{indent}{metric_expr} as {metric_name},\n")

    model.write_text("\n".join(lines))

    return {
        "branch": f"sim/add-metric-{metric_name}-{TIMESTAMP}",
        "title": f"feat: add {metric_name} metric to {model.stem}",
        "body": (
            f"Added `{metric_name}` calculated column to `{model.name}`.\n\n"
            f"**Formula:** `{metric_expr}`\n\n"
            f"This is a simulated PR for testing altimate-code-actions."
        ),
        "files": [str(model)],
    }


def add_new_model():
    """Create a new reporting/mart model."""
    layer = random.choice(["reporting", "marts"])
    directory = REPORTING_DIR if layer == "reporting" else MARTS_DIR

    model_templates = [
        (
            "rpt_order_status_summary",
            textwrap.dedent("""\
                with orders as (

                    select * from {{ ref('fct_orders') }}

                ),

                summary as (

                    select
                        order_status,
                        count(*) as order_count,
                        sum(order_revenue) as total_revenue,
                        avg(order_revenue) as avg_revenue,
                        count(distinct customer_id) as unique_customers
                    from orders
                    group by 1

                )

                select * from summary
            """),
        ),
        (
            "rpt_traffic_source_analysis",
            textwrap.dedent("""\
                with sessions as (

                    select * from {{ ref('fct_sessions') }}

                ),

                summary as (

                    select
                        traffic_source,
                        count(*) as session_count,
                        count(distinct customer_id) as unique_visitors,
                        sum(case when is_converted then 1 else 0 end) as conversions,
                        avg(page_views) as avg_page_views
                    from sessions
                    group by 1

                )

                select * from summary
            """),
        ),
        (
            "rpt_category_performance",
            textwrap.dedent("""\
                with items as (

                    select * from {{ ref('int_order_items_enriched') }}

                ),

                summary as (

                    select
                        category,
                        brand,
                        count(*) as items_sold,
                        sum(sale_price) as total_revenue,
                        sum(item_profit) as total_profit,
                        count(distinct order_id) as order_count
                    from items
                    group by 1, 2

                )

                select * from summary
            """),
        ),
        (
            "rpt_customer_segments",
            textwrap.dedent("""\
                with customers as (

                    select * from {{ ref('dim_customers') }}

                ),

                segments as (

                    select
                        customer_tier,
                        activity_status,
                        country,
                        count(*) as customer_count,
                        avg(lifetime_value) as avg_ltv,
                        avg(total_orders) as avg_orders
                    from customers
                    group by 1, 2, 3

                )

                select * from segments
            """),
        ),
    ]

    model_name, model_content = random.choice(model_templates)
    suffix = _rand_suffix()
    full_name = f"{model_name}_{suffix}"
    model_path = directory / f"{full_name}.sql"
    model_path.write_text(model_content)

    return {
        "branch": f"sim/new-model-{full_name}-{TIMESTAMP}",
        "title": f"feat: add {full_name} {layer} model",
        "body": (
            f"Added new {layer} model `{full_name}.sql`.\n\n"
            f"This is a simulated PR for testing altimate-code-actions."
        ),
        "files": [str(model_path)],
    }


def add_new_source():
    """Add a new source table definition."""
    source_file = STAGING_DIR / "_stg_sources.yml"
    content = source_file.read_text()

    new_tables = [
        ("distribution_centers", "Distribution center locations and details."),
        ("inventory_items", "Individual inventory items with cost and timestamps."),
    ]

    table_name, description = random.choice(new_tables)
    suffix = _rand_suffix()
    table_name = f"{table_name}"

    # Add to the end of the sources YAML
    new_source_entry = textwrap.dedent(f"""\

      - name: {table_name}
        description: "{description}"
        columns:
          - name: id
            description: Primary key.
            tests:
              - unique
              - not_null
    """)

    content = content.rstrip() + new_source_entry

    source_file.write_text(content)

    # Also create a staging model for it
    stg_model_path = STAGING_DIR / f"stg_{table_name}.sql"
    stg_content = textwrap.dedent(f"""\
        with source as (

            select * from {{{{ source('thelook_ecommerce', '{table_name}') }}}}

        ),

        renamed as (

            select
                id as {table_name.rstrip('s')}_id,
                *
            from source

        )

        select * from renamed
    """)
    stg_model_path.write_text(stg_content)

    return {
        "branch": f"sim/new-source-{table_name}-{TIMESTAMP}",
        "title": f"feat: add {table_name} source and staging model",
        "body": (
            f"Added `{table_name}` source definition and corresponding "
            f"`stg_{table_name}.sql` staging model.\n\n"
            f"This is a simulated PR for testing altimate-code-actions."
        ),
        "files": [str(source_file), str(stg_model_path)],
    }


def add_incremental():
    """Convert a model to incremental materialization."""
    model = _pick_random_model(random.choice([MARTS_DIR, REPORTING_DIR]))
    content = model.read_text()

    config_block = textwrap.dedent("""\
        {{
            config(
                materialized='incremental',
                unique_key='order_id',
                incremental_strategy='merge'
            )
        }}

    """)

    # Check for timestamp columns to use for incremental filter
    ts_cols = ["ordered_at", "created_at", "session_start", "revenue_date"]
    chosen_ts = random.choice(ts_cols)

    incremental_filter = textwrap.dedent(f"""\

        {{% if is_incremental() %}}
        where {chosen_ts} > (select max({chosen_ts}) from {{{{ this }}}})
        {{% endif %}}
    """)

    # Add config at top
    if "config(" in content:
        content = re.sub(r"\{\{.*?config\(.*?\).*?\}\}\s*\n*", config_block, content, count=1)
    else:
        content = config_block + content

    # Add incremental filter before the final select or at the end
    content = content.rstrip() + "\n" + incremental_filter

    model.write_text(content)

    return {
        "branch": f"sim/incremental-{model.stem}-{TIMESTAMP}",
        "title": f"feat: convert {model.stem} to incremental",
        "body": (
            f"Converted `{model.name}` to incremental materialization "
            f"using merge strategy on `{chosen_ts}`.\n\n"
            f"This is a simulated PR for testing altimate-code-actions."
        ),
        "files": [str(model)],
    }


def add_snapshot():
    """Create a new snapshot model."""
    snapshot_dir = PROJECT_ROOT / "snapshots"
    snapshot_dir.mkdir(exist_ok=True)

    targets = [
        ("orders", "stg_orders", "order_id", "ordered_at"),
        ("customers", "stg_customers", "customer_id", "created_at"),
        ("products", "stg_products", "product_id", "retail_price"),
    ]

    target_name, ref_model, unique_key, check_col = random.choice(targets)
    suffix = _rand_suffix()
    snapshot_name = f"snap_{target_name}_{suffix}"
    snapshot_path = snapshot_dir / f"{snapshot_name}.sql"

    strategy = random.choice(["timestamp", "check"])

    if strategy == "timestamp":
        strategy_config = f"strategy='timestamp',\n        updated_at='{check_col}',"
    else:
        strategy_config = f"strategy='check',\n        check_cols=['{check_col}'],"

    content = textwrap.dedent(f"""\
        {{% snapshot {snapshot_name} %}}

        {{{{
            config(
                target_schema='snapshots',
                unique_key='{unique_key}',
                {strategy_config}
            )
        }}}}

        select * from {{{{ ref('{ref_model}') }}}}

        {{% endsnapshot %}}
    """)

    snapshot_path.write_text(content)

    return {
        "branch": f"sim/snapshot-{snapshot_name}-{TIMESTAMP}",
        "title": f"feat: add {snapshot_name} snapshot",
        "body": (
            f"Added snapshot `{snapshot_name}` tracking `{ref_model}` "
            f"using {strategy} strategy.\n\n"
            f"This is a simulated PR for testing altimate-code-actions."
        ),
        "files": [str(snapshot_path)],
    }


# ---------------------------------------------------------------------------
# Optimization scenarios
# ---------------------------------------------------------------------------


def add_partition():
    """Add BigQuery partition/cluster config to a model."""
    model = _pick_random_model(random.choice([MARTS_DIR, REPORTING_DIR]))
    content = model.read_text()

    partition_cols = {
        "fct_orders": ("ordered_at", ["customer_id", "order_status"]),
        "fct_revenue": ("revenue_date", ["order_count"]),
        "fct_sessions": ("session_start", ["traffic_source", "browser"]),
        "dim_customers": ("account_created_at", ["country", "customer_tier"]),
        "dim_products": ("category", ["brand", "department"]),
    }

    stem = model.stem
    if stem in partition_cols:
        part_col, cluster_cols = partition_cols[stem]
    else:
        part_col = "created_at"
        cluster_cols = ["customer_id"]

    cluster_str = ", ".join(f"'{c}'" for c in cluster_cols)
    config_block = textwrap.dedent(f"""\
        {{{{
            config(
                materialized='table',
                partition_by={{{{
                    "field": "{part_col}",
                    "data_type": "timestamp",
                    "granularity": "day"
                }}}},
                cluster_by=[{cluster_str}]
            )
        }}}}

    """)

    if "config(" in content:
        content = re.sub(r"\{\{.*?config\(.*?\).*?\}\}\s*\n*", config_block, content, count=1)
    else:
        content = config_block + content

    model.write_text(content)

    return {
        "branch": f"sim/partition-{model.stem}-{TIMESTAMP}",
        "title": f"perf: add partition/cluster to {model.stem}",
        "body": (
            f"Added BigQuery partitioning on `{part_col}` and clustering "
            f"on `{', '.join(cluster_cols)}` to `{model.name}`.\n\n"
            f"This is a simulated PR for testing altimate-code-actions."
        ),
        "files": [str(model)],
    }


def remove_select_star():
    """Replace SELECT * with explicit column list (improvement)."""
    model = _pick_random_model(STAGING_DIR)
    content = model.read_text()

    # Find "select * from renamed" pattern and replace with explicit columns
    if "select * from" in content.lower():
        # Get columns from the CTE above
        cols = _find_columns_in_select(content)
        if cols:
            col_list = ",\n        ".join(cols[:8])  # Limit to 8 for readability
            content = re.sub(
                r"select\s+\*\s+from\s+(\w+)",
                f"select\n        {col_list}\n    from \\1",
                content,
                count=1,
                flags=re.IGNORECASE,
            )
            model.write_text(content)

    return {
        "branch": f"sim/no-select-star-{model.stem}-{TIMESTAMP}",
        "title": f"perf: replace SELECT * in {model.stem}",
        "body": (
            f"Replaced `SELECT *` with explicit column list in `{model.name}` "
            f"for better query performance and clarity.\n\n"
            f"This is a simulated PR for testing altimate-code-actions."
        ),
        "files": [str(model)],
    }


def optimize_join():
    """Add filtering before a join to reduce data scanned."""
    model = _pick_random_model(random.choice([INTERMEDIATE_DIR, MARTS_DIR]))
    content = model.read_text()

    # Add a pre-filter CTE
    refs = re.findall(r"\{\{\s*ref\('(\w+)'\)\s*\}\}", content)
    if refs:
        target_ref = random.choice(refs)
        filter_cte_name = f"filtered_{target_ref}"
        filter_cte = (
            f"\n{filter_cte_name} as (\n\n"
            f"    select * from {{{{ ref('{target_ref}') }}}}\n"
            f"    where created_at >= '2023-01-01'\n\n"
            f"),\n"
        )
        # Replace the ref with the filtered version
        content = content.replace(
            f"{{{{ ref('{target_ref}') }}}}",
            filter_cte_name,
            1,
        )
        # Add the filter CTE
        if "with " in content.lower():
            # Add after the first CTE definition
            first_close = content.find("),")
            if first_close != -1:
                content = content[: first_close + 2] + filter_cte + content[first_close + 2 :]
        model.write_text(content)

    return {
        "branch": f"sim/optimize-join-{model.stem}-{TIMESTAMP}",
        "title": f"perf: pre-filter data before join in {model.stem}",
        "body": (
            f"Added a pre-filtering CTE in `{model.name}` to reduce "
            f"the amount of data scanned during joins.\n\n"
            f"This is a simulated PR for testing altimate-code-actions."
        ),
        "files": [str(model)],
    }


def add_where_clause():
    """Add a WHERE clause filter to reduce scan size."""
    model = _pick_random_model(random.choice([MARTS_DIR, REPORTING_DIR]))
    content = model.read_text()

    filters = [
        "where order_revenue > 0",
        "where total_orders > 0",
        "where customer_id is not null",
        "where total_revenue > 0",
        "where page_views > 0",
    ]

    chosen_filter = random.choice(filters)

    # Add before the final closing paren of the last CTE
    last_from = content.rfind("\n    from ")
    if last_from != -1:
        # Find the end of the FROM line
        end_of_from = content.find("\n", last_from + 1)
        if end_of_from != -1:
            next_content = content[end_of_from:]
            if "where" not in next_content[:100].lower():
                content = content[:end_of_from] + f"\n    {chosen_filter}" + content[end_of_from:]

    model.write_text(content)

    return {
        "branch": f"sim/add-filter-{model.stem}-{TIMESTAMP}",
        "title": f"perf: add filter clause to {model.stem}",
        "body": (
            f"Added `{chosen_filter}` to `{model.name}` to reduce "
            f"unnecessary data processing.\n\n"
            f"This is a simulated PR for testing altimate-code-actions."
        ),
        "files": [str(model)],
    }


def convert_to_incremental():
    """Convert a full-refresh table to incremental."""
    model = _pick_random_model(MARTS_DIR)
    content = model.read_text()

    config = textwrap.dedent("""\
        {{
            config(
                materialized='incremental',
                unique_key='order_id',
                on_schema_change='append_new_columns'
            )
        }}

    """)

    if "config(" in content:
        content = re.sub(r"\{\{.*?config\(.*?\).*?\}\}\s*\n*", config, content, count=1)
    else:
        content = config + content

    # Add is_incremental filter
    content += textwrap.dedent("""
        {% if is_incremental() %}
        where ordered_at > (select max(ordered_at) from {{ this }})
        {% endif %}
    """)

    model.write_text(content)

    return {
        "branch": f"sim/to-incremental-{model.stem}-{TIMESTAMP}",
        "title": f"perf: convert {model.stem} to incremental",
        "body": (
            f"Converted `{model.name}` from full-refresh to incremental "
            f"materialization to reduce processing time.\n\n"
            f"This is a simulated PR for testing altimate-code-actions."
        ),
        "files": [str(model)],
    }


# ---------------------------------------------------------------------------
# Anti-pattern scenarios (should be caught by altimate-code-actions)
# ---------------------------------------------------------------------------


def introduce_select_star():
    """Introduce SELECT * (should be caught by review)."""
    models = _sql_files(STAGING_DIR)
    model = random.choice(models)
    content = model.read_text()

    # Replace explicit column list with SELECT *
    # Find the renamed CTE and replace its select list
    renamed_match = re.search(
        r"(renamed\s+as\s*\(\s*\n\s*select)\s+([\s\S]*?)(\s*from\s+source)",
        content,
        re.IGNORECASE,
    )
    if renamed_match:
        new_content = (
            content[: renamed_match.start(2)]
            + "\n        *\n    "
            + content[renamed_match.start(3) :]
        )
        model.write_text(new_content)
    else:
        # Simpler approach: replace any "select <columns>" with "select *"
        content = re.sub(
            r"(select)\s+\w+.*?(\n\s+from\s)",
            r"\1 *\2",
            content,
            count=1,
            flags=re.IGNORECASE | re.DOTALL,
        )
        model.write_text(content)

    return {
        "branch": f"sim/select-star-{model.stem}-{TIMESTAMP}",
        "title": f"refactor: simplify {model.stem} query",
        "body": (
            f"Simplified the SELECT clause in `{model.name}` for readability.\n\n"
            f"This is a simulated PR for testing altimate-code-actions.\n\n"
            f"<!-- expected: select_star -->"
        ),
        "files": [str(model)],
    }


def introduce_cartesian_join():
    """Introduce a CROSS JOIN / cartesian product (should be caught)."""
    model = _pick_random_model(random.choice([MARTS_DIR, REPORTING_DIR]))
    content = model.read_text()

    refs = re.findall(r"\{\{\s*ref\('(\w+)'\)\s*\}\}", content)
    cross_target = random.choice(
        ["stg_products", "stg_customers", "stg_orders"]
    )

    # Add a cross join at the end
    cross_cte = f"cross_data as (\n\n    select * from {{{{ ref('{cross_target}') }}}}\n\n)"

    if content.strip().lower().startswith("with"):
        # Add as additional CTE
        # Find last CTE closing
        last_paren = content.rfind("),")
        if last_paren != -1:
            content = content[: last_paren + 2] + f"\n\n{cross_cte},\n" + content[last_paren + 2 :]
    else:
        content = f"with {cross_cte}\n\n{content}"

    # Add cross join to final select
    content = content.replace(
        "select * from final",
        "select f.*, c.* from final f cross join cross_data c",
    )
    if "select * from final" not in content:
        # Different final pattern
        content = content.rstrip() + "\n-- cross join cross_data added for testing\n"

    model.write_text(content)

    return {
        "branch": f"sim/cartesian-{model.stem}-{TIMESTAMP}",
        "title": f"feat: enrich {model.stem} with {cross_target} data",
        "body": (
            f"Added cross-reference data from `{cross_target}` to `{model.name}`.\n\n"
            f"This is a simulated PR for testing altimate-code-actions.\n\n"
            f"<!-- expected: cartesian_join -->"
        ),
        "files": [str(model)],
    }


def introduce_non_deterministic():
    """Add CURRENT_DATE() or NOW() usage (should be caught)."""
    model = _pick_random_model(random.choice([INTERMEDIATE_DIR, MARTS_DIR, REPORTING_DIR]))
    content = model.read_text()

    non_det_options = [
        ("current_timestamp()", "query_run_at"),
        ("current_date()", "report_date"),
        ("generate_uuid()", "run_id"),
    ]

    expr, alias = random.choice(non_det_options)

    # Add to the final select
    lines = content.split("\n")
    for i in range(len(lines) - 1, -1, -1):
        if re.match(r"\s+from\s+", lines[i], re.IGNORECASE):
            lines.insert(i, f"        {expr} as {alias},")
            break

    model.write_text("\n".join(lines))

    return {
        "branch": f"sim/non-det-{model.stem}-{TIMESTAMP}",
        "title": f"feat: add {alias} to {model.stem}",
        "body": (
            f"Added `{alias}` column using `{expr}` to `{model.name}` "
            f"for audit tracking.\n\n"
            f"This is a simulated PR for testing altimate-code-actions.\n\n"
            f"<!-- expected: non_deterministic -->"
        ),
        "files": [str(model)],
    }


def introduce_or_in_join():
    """Add OR condition in a JOIN (should be caught as anti-pattern)."""
    model = _pick_random_model(random.choice([INTERMEDIATE_DIR, MARTS_DIR]))
    content = model.read_text()

    # Find an existing join and add OR condition
    join_match = re.search(
        r"(\bon\b\s+\w+\.\w+\s*=\s*\w+\.\w+)",
        content,
        re.IGNORECASE,
    )
    if join_match:
        old = join_match.group(0)
        # Add a problematic OR condition
        new = f"{old}\n        or 1 = 1  -- fallback match"
        content = content.replace(old, new, 1)
    else:
        content += "\n-- OR in join: no join found to modify\n"

    model.write_text(content)

    return {
        "branch": f"sim/or-join-{model.stem}-{TIMESTAMP}",
        "title": f"fix: broaden join match in {model.stem}",
        "body": (
            f"Added fallback join condition in `{model.name}` to handle "
            f"edge cases with missing keys.\n\n"
            f"This is a simulated PR for testing altimate-code-actions.\n\n"
            f"<!-- expected: or_in_join -->"
        ),
        "files": [str(model)],
    }


def remove_tests():
    """Remove dbt tests from a schema file (should be flagged)."""
    yml_files = _yml_files(STAGING_DIR) + _yml_files(INTERMEDIATE_DIR) + _yml_files(MARTS_DIR)
    yml_files = [f for f in yml_files if f.name.startswith("_")]
    if not yml_files:
        return {
            "branch": f"sim/remove-tests-noop-{TIMESTAMP}",
            "title": "chore: clean up test configuration",
            "body": "No YAML files found to modify.\n\nThis is a simulated PR.",
            "files": [],
        }

    yml_file = random.choice(yml_files)
    content = yml_file.read_text()

    # Remove test blocks
    content = re.sub(r"\n\s+tests:\n(\s+-\s+\S+\n?)+", "", content)

    yml_file.write_text(content)

    return {
        "branch": f"sim/remove-tests-{yml_file.stem}-{TIMESTAMP}",
        "title": f"chore: remove tests from {yml_file.name}",
        "body": (
            f"Removed test definitions from `{yml_file.name}` to simplify "
            f"the schema configuration.\n\n"
            f"This is a simulated PR for testing altimate-code-actions."
        ),
        "files": [str(yml_file)],
    }


# ---------------------------------------------------------------------------
# Main execution
# ---------------------------------------------------------------------------


def _resolve_scenario(scenario_name: str):
    """Resolve a scenario name to its function."""
    if scenario_name == "random":
        category = random.choice(list(SCENARIOS.keys()))
        fn_name = random.choice(SCENARIOS[category])
        print(f"Randomly selected: {category}/{fn_name}")
        return fn_name

    # Check if it is a category name
    if scenario_name in SCENARIOS:
        fn_name = random.choice(SCENARIOS[scenario_name])
        print(f"Randomly selected from {scenario_name}: {fn_name}")
        return fn_name

    # Check if it is a specific scenario function name
    for category, fns in SCENARIOS.items():
        if scenario_name in fns:
            return scenario_name

    print(f"Unknown scenario: {scenario_name}")
    print("Available categories:", ", ".join(SCENARIOS.keys()))
    print("Available scenarios:")
    for cat, fns in SCENARIOS.items():
        for fn in fns:
            print(f"  {cat}/{fn}")
    sys.exit(1)


def _git(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    """Run a git command."""
    cmd = ["git"] + list(args)
    print(f"$ {' '.join(cmd)}")
    return subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True, check=check)


def _gh(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    """Run a gh CLI command."""
    cmd = ["gh"] + list(args)
    print(f"$ {' '.join(cmd)}")
    return subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True, check=check)


def main():
    parser = argparse.ArgumentParser(description="Simulate dbt workload for altimate-code-actions testing")
    parser.add_argument(
        "--scenario",
        default="random",
        help="Scenario to run (random, category name, or specific scenario function name)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would happen without creating branches or PRs",
    )
    args = parser.parse_args()

    fn_name = _resolve_scenario(args.scenario)
    fn = globals().get(fn_name)
    if fn is None or not callable(fn):
        print(f"Scenario function {fn_name} not found")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"Running scenario: {fn_name}")
    print(f"{'='*60}\n")

    # Ensure we start from a clean main branch
    _git("checkout", "main", check=False)
    _git("pull", "origin", "main", check=False)

    # Run the scenario
    result = fn()
    branch = result["branch"]
    title = result["title"]
    body = result["body"]
    files = result["files"]

    print(f"\nBranch: {branch}")
    print(f"Title:  {title}")
    print(f"Files:  {files}")
    print(f"Body:\n{body}\n")

    if args.dry_run:
        print("DRY RUN -- not creating branch or PR")
        # Restore files
        _git("checkout", ".", check=False)
        return

    if not files:
        print("No files modified -- skipping PR creation")
        _git("checkout", ".", check=False)
        return

    # Create branch, commit, push, and open PR
    _git("checkout", "-b", branch)
    _git("add", *files)

    # Commit
    commit_result = _git("commit", "-m", title, check=False)
    if commit_result.returncode != 0:
        print(f"Commit failed: {commit_result.stderr}")
        _git("checkout", "main", check=False)
        _git("branch", "-D", branch, check=False)
        sys.exit(1)

    # Push
    push_result = _git("push", "origin", branch, check=False)
    if push_result.returncode != 0:
        print(f"Push failed: {push_result.stderr}")
        _git("checkout", "main", check=False)
        _git("branch", "-D", branch, check=False)
        sys.exit(1)

    # Create PR
    pr_result = _gh(
        "pr", "create",
        "--title", title,
        "--body", body,
        "--base", "main",
        "--head", branch,
        check=False,
    )
    if pr_result.returncode != 0:
        print(f"PR creation failed: {pr_result.stderr}")
    else:
        print(f"\nPR created: {pr_result.stdout.strip()}")

    # Return to main
    _git("checkout", "main", check=False)

    print(f"\n{'='*60}")
    print("Simulation complete!")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
