"""Job state (v0: JSON file in the job directory; M1 moves this to a Spanner metadata DB)."""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

PHASES = [
    "CREATED", "ASSESSED", "CONVERTED", "SCHEMA_APPLIED", "SNAPSHOT_TAKEN", "LOADED",
    "REPLICATING", "INDEXES_APPLIED", "VALIDATED", "CUTOVER_DONE",
]


@dataclass
class JobState:
    job_id: str
    phase: str = "CREATED"
    t0: str | None = None
    changefeeds: dict[str, int] = field(default_factory=dict)  # table -> CRDB job id
    history: list[dict] = field(default_factory=list)

    @classmethod
    def load(cls, job_dir: Path, job_id: str) -> "JobState":
        p = job_dir / "state.json"
        if p.exists():
            return cls(**json.loads(p.read_text()))
        return cls(job_id=job_id)

    def advance(self, job_dir: Path, phase: str, **info) -> None:
        if phase not in PHASES:
            raise ValueError(phase)
        if PHASES.index(phase) > PHASES.index(self.phase):
            self.phase = phase
        self.history.append({"at": dt.datetime.now(dt.timezone.utc).isoformat(), "phase": phase, **info})
        self.save(job_dir)

    def save(self, job_dir: Path) -> None:
        (job_dir / "state.json").write_text(json.dumps(asdict(self), indent=2))
