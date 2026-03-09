from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass
class LogEntry:
    timestamp: str
    level: str
    code: str
    message: str
    context: dict[str, Any]


class RunLogger:
    def __init__(self) -> None:
        self.entries: list[LogEntry] = []

    def _add(self, level: str, code: str, message: str, context: dict[str, Any] | None = None) -> None:
        self.entries.append(
            LogEntry(
                timestamp=datetime.now().isoformat(timespec="seconds"),
                level=level,
                code=code,
                message=message,
                context=context or {},
            )
        )

    def info(self, code: str, message: str, context: dict[str, Any] | None = None) -> None:
        self._add("INFO", code, message, context)

    def warn(self, code: str, message: str, context: dict[str, Any] | None = None) -> None:
        self._add("WARN", code, message, context)

    def error(self, code: str, message: str, context: dict[str, Any] | None = None) -> None:
        self._add("ERROR", code, message, context)

    def to_text(self, metadata: dict[str, Any] | None = None) -> str:
        lines: list[str] = []
        if metadata:
            lines.append("RUN_METADATA: " + json.dumps(metadata, ensure_ascii=False))
        for entry in self.entries:
            context_str = json.dumps(entry.context, ensure_ascii=False) if entry.context else "{}"
            lines.append(f"[{entry.timestamp}] {entry.level} {entry.code} - {entry.message} | {context_str}")
        return "\n".join(lines) + "\n"

    def to_json(self, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        return {
            "metadata": metadata or {},
            "entries": [asdict(entry) for entry in self.entries],
        }

    def save(self, output_dir: Path, timestamp: str, metadata: dict[str, Any] | None = None) -> dict[str, str]:
        output_dir.mkdir(parents=True, exist_ok=True)
        txt_path = output_dir / f"log_{timestamp}.txt"
        json_path = output_dir / f"log_{timestamp}.json"

        txt_path.write_text(self.to_text(metadata), encoding="utf-8")
        json_path.write_text(json.dumps(self.to_json(metadata), ensure_ascii=False, indent=2), encoding="utf-8")
        return {"txt": str(txt_path), "json": str(json_path)}

