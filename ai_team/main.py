"""Entry point for the WIT AI Team — interactive CLI."""
import sys
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from smart_wallet_analysis.logger import get_logger
from ai_team.crew import run_improvement_session

log = get_logger("ai_team.main")

TOPICS = [
    "Score Engine — scoring model, FIFO accuracy, thresholds",
    "Consensus Signal — signal quality and detection logic",
    "Token Discovery — explosive token detection and early wallet extraction",
    "Tracking Live — real-time monitoring performance and reliability",
    "Database — schema optimization, indexes, data quality",
    "Global pipeline audit — full system review",
]


def choose_topic() -> str:
    """Interactive topic selector."""
    lines = ["\nAvailable improvement topics:"]
    for i, t in enumerate(TOPICS, 1):
        lines.append(f"  {i}. {t}")
    lines.append(f"  {len(TOPICS) + 1}. Custom topic")
    sys.stdout.write("\n".join(lines) + "\n")

    while True:
        choice = input(f"\nChoose a topic [1-{len(TOPICS) + 1}]: ").strip()
        if choice.isdigit():
            idx = int(choice)
            if 1 <= idx <= len(TOPICS):
                return TOPICS[idx - 1]
            if idx == len(TOPICS) + 1:
                return input("Enter your custom topic: ").strip()
        sys.stdout.write("Invalid choice, try again.\n")


def main():
    if not os.getenv("OPENAI_API_KEY"):
        log.error("OPENAI_API_KEY not found in .env — add it before running the team")
        sys.exit(1)

    topic = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else choose_topic()

    log.info(f"WIT AI Team session starting — topic: '{topic}'")
    log.info("Report will be saved to ai_team/outputs/ after completion")

    run_improvement_session(topic)


if __name__ == "__main__":
    main()
