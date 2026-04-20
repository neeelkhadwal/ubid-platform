#!/usr/bin/env python3
"""
End-to-end UBID pipeline runner.
Usage:
  python scripts/run_pipeline.py             # full run
  python scripts/run_pipeline.py --setup-only
  python scripts/run_pipeline.py --generate-only
  python scripts/run_pipeline.py --er-only
"""
import argparse
import sys
from pathlib import Path

# Ensure project root is on the path
sys.path.insert(0, str(Path(__file__).parent.parent))

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from sqlalchemy.orm import Session

from src.config import settings
from src.database.models import SourceRecord
from src.database.session import SessionLocal, init_db
from src.ingestion.standardiser import standardise_record
from src.entity_resolution.pipeline import run_entity_resolution
from src.activity.classifier import classify_all, ingest_events
from src.registry.ubid_registry import UBIDRegistry
from synthetic_data.generator import generate_all

console = Console()
registry = UBIDRegistry()


def setup_db():
    console.print("[bold green]▶ Initialising database…[/]")
    init_db()
    console.print("[green]✓ Database ready[/]")


def generate_and_ingest(db: Session) -> tuple[list[dict], list[dict]]:
    console.print("[bold green]▶ Generating synthetic data…[/]")
    source_records, activity_events = generate_all(n_businesses=120)
    console.print(f"  Generated {len(source_records)} source records, {len(activity_events)} events")

    console.print("[bold green]▶ Standardising and ingesting source records…[/]")
    existing_keys = {
        (r.source_system, r.source_record_id)
        for r in db.query(SourceRecord).all()
    }

    new_recs = []
    for raw in source_records:
        key = (raw["source_system"], raw["source_record_id"])
        if key in existing_keys:
            continue
        std = standardise_record(raw)
        from datetime import date as _date
        reg_date = None
        if std.registration_date:
            try:
                reg_date = _date.fromisoformat(std.registration_date)
            except (ValueError, TypeError):
                reg_date = None

        rec = SourceRecord(
            source_system=std.source_system,
            source_record_id=std.source_record_id,
            business_name_raw=std.name.raw,
            address_raw=std.address.raw,
            business_name_std=std.name.cleaned,
            phonetic_key=std.name.phonetic_key,
            name_tokens=" ".join(std.name.tokens),
            pin_code=std.address.pin_code,
            locality_std=std.address.locality,
            door_number=std.address.door_number,
            street_std=std.address.street,
            pan=std.pan,
            gstin=std.gstin,
            pan_valid=std.pan_valid,
            gstin_valid=std.gstin_valid,
            entity_type=std.entity_type,
            nic_code=std.nic_code,
            registration_date=reg_date,
            raw_data=raw,
        )
        new_recs.append(rec)
        existing_keys.add(key)

    db.bulk_save_objects(new_recs)
    db.commit()
    console.print(f"[green]✓ {len(new_recs)} new source records ingested[/]")
    return source_records, activity_events


def run_er(db: Session):
    console.print("[bold green]▶ Running entity resolution…[/]")
    stats = run_entity_resolution(db, registry)
    return stats


def ingest_and_classify(db: Session, events: list[dict]):
    console.print("[bold green]▶ Ingesting activity events…[/]")
    ingest_stats = ingest_events(db, events)
    console.print(
        f"  Joined: {ingest_stats['joined']}, "
        f"Unmatched known: {ingest_stats['unmatched_known']}, "
        f"Unmatched unknown: {ingest_stats['unmatched_unknown']}"
    )

    console.print("[bold green]▶ Classifying business activity status…[/]")
    status_counts = classify_all(db)
    return ingest_stats, status_counts


def print_summary(db: Session, er_stats: dict, status_counts: dict):
    from src.database.models import MatchCandidate, UBIDRecord

    table = Table(title="UBID Pipeline Summary", show_header=True)
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="bold white")

    table.add_row("Source records", str(db.query(SourceRecord).count()))
    table.add_row("Total UBIDs created", str(db.query(UBIDRecord).count()))
    table.add_row("  → PAN-anchored", str(db.query(UBIDRecord).filter_by(anchor_type="PAN").count()))
    table.add_row("  → GST-anchored", str(db.query(UBIDRecord).filter_by(anchor_type="GST").count()))
    table.add_row("  → Internal", str(db.query(UBIDRecord).filter_by(anchor_type="INT").count()))
    table.add_row("Auto-linked pairs", str(db.query(MatchCandidate).filter_by(status="AUTO_LINKED").count()))
    table.add_row("Review queue (pending)", str(db.query(MatchCandidate).filter_by(status="PENDING").count()))
    table.add_row("Rejected pairs", str(db.query(MatchCandidate).filter_by(status="REJECTED").count()))
    for status, count in sorted(status_counts.items()):
        table.add_row(f"Status: {status}", str(count))

    console.print(table)
    console.print()
    console.print(Panel(
        "[bold]API:[/] [link]http://localhost:8000[/link]\n"
        "[bold]Docs:[/] [link]http://localhost:8000/docs[/link]\n\n"
        "Run [bold cyan]make serve[/] to start the API server.",
        title="Next Steps",
        expand=False,
    ))


def main():
    parser = argparse.ArgumentParser(description="UBID Pipeline Runner")
    parser.add_argument("--setup-only", action="store_true")
    parser.add_argument("--generate-only", action="store_true")
    parser.add_argument("--er-only", action="store_true")
    args = parser.parse_args()

    setup_db()

    if args.setup_only:
        console.print("[green]Setup complete.[/]")
        return

    db: Session = SessionLocal()
    try:
        source_records, activity_events = generate_and_ingest(db)

        if args.generate_only:
            console.print("[green]Data generation complete.[/]")
            return

        er_stats = run_er(db)
        ingest_stats, status_counts = ingest_and_classify(db, activity_events)
        print_summary(db, er_stats, status_counts)
    finally:
        db.close()


if __name__ == "__main__":
    main()
