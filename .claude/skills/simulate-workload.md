---
name: simulate-workload
description: Run a simulation that creates a realistic PR against the BigQuery demo project, triggering altimate-code-actions review
---

# Simulate Workload

Run the simulation script to create a realistic dbt PR:

1. Pick a random scenario category (refactor, bugfix, feature, optimization, anti_pattern)
2. Execute the scenario to modify dbt models
3. Create a branch, commit, push, and open a PR
4. The PR will automatically trigger altimate-code-actions review
5. After review, evaluate whether the review adds value

## Usage

```bash
cd /Users/anandgupta/codebase/altimate-bigquery-demo
python scripts/simulate.py --scenario random
```

### Specific categories

```bash
python scripts/simulate.py --scenario refactor
python scripts/simulate.py --scenario bugfix
python scripts/simulate.py --scenario feature
python scripts/simulate.py --scenario optimization
python scripts/simulate.py --scenario anti_pattern
```

### Specific scenarios

```bash
python scripts/simulate.py --scenario introduce_select_star
python scripts/simulate.py --scenario add_new_model
python scripts/simulate.py --scenario fix_null_handling
```

### Dry run (preview without creating PR)

```bash
python scripts/simulate.py --scenario random --dry-run
```

## Available Scenarios (25 total)

**Refactor (5):** refactor_cte_to_subquery, refactor_subquery_to_cte, rename_column, change_materialization, extract_staging_model

**Bugfix (5):** fix_null_handling, fix_join_condition, fix_duplicate_rows, fix_date_filter, fix_type_cast

**Feature (5):** add_new_metric, add_new_model, add_new_source, add_incremental, add_snapshot

**Optimization (5):** add_partition, remove_select_star, optimize_join, add_where_clause, convert_to_incremental

**Anti-pattern (5):** introduce_select_star, introduce_cartesian_join, introduce_non_deterministic, introduce_or_in_join, remove_tests

## Evaluating Results

After the PR is reviewed by altimate-code-actions:

1. Check the altimate-code-actions comment on the PR
2. If the review missed an issue that should have been caught -> file a bug in AltimateAI/altimate-code-actions
3. If the review flagged a false positive -> file a bug
4. If the review format is unclear -> file a UX issue
5. If the review adds genuine value -> note it as a success

## Architecture

- **Workflows:** `.github/workflows/altimate-review.yml` triggers on PR, `.github/workflows/simulate.yml` runs on schedule
- **Script:** `scripts/simulate.py` is the core engine with 25 scenario functions
- **Models:** 5 staging, 5 intermediate, 5 marts, 3 reporting models based on BigQuery public dataset `thelook_ecommerce`
