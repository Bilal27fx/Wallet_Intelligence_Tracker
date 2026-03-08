"""Task definitions for the WIT AI team improvement workflow."""
from crewai import Task
from ai_team.agents.lead_engineer import lead_engineer
from ai_team.agents.quant_researcher import quant_researcher
from ai_team.agents.quant_developer import quant_developer
from ai_team.agents.db_engineer import db_engineer

_RULES = (
    "MANDATORY RULES — violation = task failure:\n"
    "- NEVER invent code, column names, function names or SQL. Only cite what you READ.\n"
    "- Every claim must be backed by a direct quote from a file or a real query result.\n"
    "- If a tool call returns an error, report it honestly — do not guess the content.\n"
    "- SQLite does NOT support ALTER COLUMN, RENAME COLUMN (pre-3.35), or SET NOT NULL — never propose these.\n"
)

_MODULE_MAP = (
    "Module → directory mapping:\n"
    "  score_engine          → smart_wallet_analysis/score_engine/\n"
    "  tracking_live         → smart_wallet_analysis/tracking_live/\n"
    "  consensus_live        → smart_wallet_analysis/consensus_live/\n"
    "  token_discovery       → smart_wallet_analysis/token_discovery_manual/\n"
    "  wallet_tracker        → smart_wallet_analysis/wallet_tracker/\n"
    "  telegram              → smart_wallet_analysis/Telegram/\n"
    "  config                → smart_wallet_analysis/config.py\n"
    "  db tables             → wallets, tokens, transaction_history, token_analytics,\n"
    "                          wallet_tier_performance, wallet_qualified, smart_wallets,\n"
    "                          wallet_brute, wallet_position_changes, consensus_live\n"
)


def build_tasks(topic: str) -> list[Task]:
    """Build the ordered task list for a given improvement topic."""

    research_task = Task(
        description=(
            f"You are the Quantitative Researcher. Analyze WIT V1 for: '{topic}'.\n\n"
            + _MODULE_MAP
            + "\nMANDATORY WORKFLOW — execute in order, do not skip:\n"
            "1. Use codebase_list to list files in the relevant module directory.\n"
            "2. Use codebase_reader to read EVERY file listed — do not skip any.\n"
            "3. Use config_reader to read the config section for that module.\n"
            "4. Use db_inspector to run these queries and include the FULL results:\n"
            "   a. SELECT COUNT(*) FROM <relevant_table>\n"
            "   b. SELECT * FROM <relevant_table> LIMIT 5  (to see real data)\n"
            "   c. At least 2 analytical queries relevant to the topic "
            "      (distributions, NULLs, outliers, duplicates…)\n"
            "5. Based ONLY on what you read and queried, identify 3-5 real issues.\n\n"
            + _RULES
            + "\nFor each issue, you MUST provide:\n"
            "- The exact file and line(s) where the problem is in the code\n"
            "- The actual query result that shows the data problem (if data-related)\n"
            "- Why it matters for wallet tracking quality\n"
        ),
        expected_output=(
            "A research report with ONLY evidence-backed findings:\n\n"
            "For each issue (3-5 total):\n"
            "### Issue N: <title>\n"
            "**File**: `path/to/file.py` lines X-Y\n"
            "**Code cited**: ```python\n<exact snippet from file>```\n"
            "**DB evidence**: (paste real query + result, or N/A if code-only issue)\n"
            "**Impact**: High / Medium / Low — explain why\n"
            "**Direction**: one concrete sentence on how to fix it\n"
        ),
        agent=quant_researcher,
    )

    db_audit_task = Task(
        description=(
            f"You are the DB Engineer. Audit wit_database.db for topic: '{topic}'.\n\n"
            + _MODULE_MAP
            + "\nMANDATORY WORKFLOW — execute in order, do not skip:\n"
            "1. Use db_schema_inspector on ALL tables relevant to the topic.\n"
            "   Paste the full CREATE TABLE statement in your report.\n"
            "2. Run PRAGMA index_list(<table>) for each relevant table.\n"
            "3. Run EXPLAIN QUERY PLAN for the 2-3 most frequent SELECT queries "
            "   used in the module (read the module code first with codebase_reader to find them).\n"
            "4. Run these data quality checks and include real results:\n"
            "   a. NULL check: SELECT COUNT(*) FROM <table> WHERE <critical_col> IS NULL\n"
            "   b. Orphan check if applicable (e.g. tokens without parent wallet)\n"
            "   c. Duplicate check if applicable\n"
            "   d. At least one distribution query (e.g. GROUP BY to see value spread)\n"
            "5. Propose indexes or schema changes — ONLY if justified by query plans or NULL counts.\n\n"
            + _RULES
            + "\nSQLite-safe migrations only: CREATE INDEX, CREATE TABLE, INSERT, UPDATE, DELETE.\n"
            "No ALTER COLUMN, no SET NOT NULL, no RENAME COLUMN on SQLite < 3.35.\n"
        ),
        expected_output=(
            "A DB audit report with real evidence:\n\n"
            "### Schema: <table_name>\n"
            "```sql\n<full CREATE TABLE from db_schema_inspector>\n```\n"
            "Indexes found: (paste PRAGMA index_list result)\n\n"
            "### Query Plan Analysis\n"
            "For each query: paste EXPLAIN QUERY PLAN result and assessment.\n\n"
            "### Data Quality Results\n"
            "Paste each query + its real result. Flag any anomaly.\n\n"
            "### Proposed Migrations\n"
            "Only SQLite-compatible SQL. Justify each with the evidence above.\n"
        ),
        agent=db_engineer,
    )

    dev_proposal_task = Task(
        description=(
            f"You are the Quant Developer. Propose code changes for: '{topic}'.\n"
            "You have access to the Quant Researcher's findings in context.\n\n"
            + _MODULE_MAP
            + "\nMANDATORY WORKFLOW — execute in order, do not skip:\n"
            "1. Use codebase_reader to read the FULL content of each file to be modified.\n"
            "   Do not propose changes to files you have not fully read.\n"
            "2. Use config_full_reader to read the entire config.py.\n"
            "3. For each issue from the researcher's report that requires a code change:\n"
            "   a. Quote the CURRENT code (exact lines from the file you read)\n"
            "   b. Write the PROPOSED replacement code\n"
            "   c. List any new config.py keys needed (with their section and default value)\n"
            "   d. List all other files that reference this function (use codebase_reader to check)\n"
            "4. Do NOT propose changes to code you haven't read.\n"
            "5. Do NOT invent function signatures, class structures or column names.\n\n"
            + _RULES
            + "\nCLAUDE.md rules (non-negotiable):\n"
            "- Full import paths: from smart_wallet_analysis.module import func\n"
            "- All new parameters in config.py, never hardcoded\n"
            "- Functions ≤ 30 lines, one responsibility\n"
            "- No code duplication\n"
        ),
        expected_output=(
            "For each proposed change:\n\n"
            "### Change N: <description>\n"
            "**File**: `smart_wallet_analysis/module/file.py`\n"
            "**Function**: `function_name` (lines X-Y)\n\n"
            "**Current code** (cited from file):\n"
            "```python\n<exact current code>\n```\n\n"
            "**Proposed code**:\n"
            "```python\n<new implementation>\n```\n\n"
            "**config.py addition** (section + key + default):\n"
            "```python\n# in SECTION_NAME\n'KEY': value\n```\n\n"
            "**Other files impacted**: list with reason\n"
            "**Risk**: breaking change? data migration needed? backward compatible?\n"
        ),
        agent=quant_developer,
        context=[research_task],
    )

    synthesis_task = Task(
        description=(
            f"You are the Lead Engineer. Synthesize the team's work on: '{topic}' "
            "into a final report for Bilal to review and validate.\n\n"
            "You have full context from: Quant Researcher, DB Engineer, Quant Developer.\n\n"
            "MANDATORY WORKFLOW:\n"
            "1. Read all three team outputs from context.\n"
            "2. Cross-check: if a code proposal references a function that the researcher "
            "   didn't actually cite from the file, flag it as 'UNVERIFIED — needs review'.\n"
            "3. Cross-check: if a SQL migration is not SQLite-compatible, flag it as 'INVALID SQL'.\n"
            "4. Rank improvements by: (impact × confidence in evidence) / effort.\n"
            "5. Separate what can be done immediately from what needs more analysis.\n"
            "6. List explicitly what Bilal needs to decide before anything is implemented.\n\n"
            "The report is READ by Bilal — it must be actionable, honest, and short.\n"
            "If the team produced weak evidence for a proposal, say so explicitly.\n"
        ),
        expected_output=(
            "## Summary\n"
            "2-3 sentences. What the team found, confidence level, recommended next step.\n\n"
            "## Prioritized Improvements\n"
            "Ranked table: | # | Title | Impact | Effort | Confidence | Status |\n"
            "Status = READY TO IMPLEMENT / NEEDS MORE ANALYSIS / UNVERIFIED\n\n"
            "## Implementation Plan (READY items only)\n"
            "For each: file, function, change summary, config.py keys to add.\n"
            "Include the exact code snippets from the developer's proposals.\n\n"
            "## Database Changes\n"
            "Only SQLite-valid migrations. Flag any INVALID SQL from the DB Engineer.\n\n"
            "## Decisions Required from Bilal\n"
            "Numbered list. Each item must be a binary choice or explicit approval.\n\n"
            "## What NOT to Change\n"
            "What was reviewed and explicitly decided to leave as-is, and why.\n\n"
            "## Team Confidence Assessment\n"
            "Honest rating: did each agent use their tools properly? "
            "Flag any finding that looks invented rather than evidence-based."
        ),
        agent=lead_engineer,
        context=[research_task, db_audit_task, dev_proposal_task],
    )

    return [research_task, db_audit_task, dev_proposal_task, synthesis_task]
