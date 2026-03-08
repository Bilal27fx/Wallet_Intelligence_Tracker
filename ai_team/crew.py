"""WIT AI Team crew orchestrator."""
import os
from pathlib import Path
from datetime import datetime
from crewai import Crew, Process
from ai_team.agents import lead_engineer, quant_researcher, quant_developer, db_engineer
from ai_team.tasks.improvement_tasks import build_tasks
from smart_wallet_analysis.logger import get_logger

log = get_logger("ai_team.crew")

OUTPUTS_DIR = Path(__file__).parent / "outputs"
OUTPUTS_DIR.mkdir(exist_ok=True)


def run_improvement_session(topic: str) -> str:
    """Run a full improvement analysis session on the given topic."""
    log.info(f"Starting improvement session — topic: '{topic}'")
    tasks = build_tasks(topic)

    crew = Crew(
        agents=[quant_researcher, db_engineer, quant_developer, lead_engineer],
        tasks=tasks,
        process=Process.sequential,
        verbose=True,
        memory=False,
    )

    result = crew.kickoff()
    report = str(result)

    slug = topic.lower().replace(" ", "_")[:40]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    output_path = OUTPUTS_DIR / f"report_{slug}_{timestamp}.md"
    output_path.write_text(f"# WIT AI Team Report — {topic}\n\n{report}", encoding="utf-8")

    log.info(f"Report saved to: {output_path}")
    return report
