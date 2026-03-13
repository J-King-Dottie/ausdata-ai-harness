from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
from typing import Any, Dict, List

from ..config import get_settings


@dataclass
class ConversationState:
    conversation_id: str
    messages: List[Dict[str, str]] = field(default_factory=list)
    loop_history: List[Dict[str, Any]] = field(default_factory=list)
    artifacts: List[Dict[str, Any]] = field(default_factory=list)
    pending_plan: Dict[str, Any] | None = None


class ConversationStore:
    """Conversation storage backed by memory and runtime JSON files."""

    def __init__(self) -> None:
        self._states: Dict[str, ConversationState] = {}
        self._lock = RLock()
        settings = get_settings()
        self._base_dir = settings.runtime_dir / "conversation_store"
        self._base_dir.mkdir(parents=True, exist_ok=True)

    def _state_path(self, conversation_id: str) -> Path:
        safe_id = "".join(ch for ch in conversation_id if ch.isalnum() or ch in {"-", "_"})
        if not safe_id:
            safe_id = "conversation"
        return self._base_dir / f"{safe_id}.json"

    def _load_from_disk(self, conversation_id: str) -> ConversationState | None:
        path = self._state_path(conversation_id)
        if not path.exists():
            return None
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None
        if not isinstance(raw, dict):
            return None
        return ConversationState(
            conversation_id=conversation_id,
            messages=list(raw.get("messages") or []),
            loop_history=list(raw.get("loop_history") or []),
            artifacts=list(raw.get("artifacts") or []),
            pending_plan=raw.get("pending_plan") if isinstance(raw.get("pending_plan"), dict) else None,
        )

    def _save_to_disk(self, state: ConversationState) -> None:
        path = self._state_path(state.conversation_id)
        payload = {
            "conversation_id": state.conversation_id,
            "messages": state.messages,
            "loop_history": state.loop_history,
            "artifacts": state.artifacts,
            "pending_plan": state.pending_plan,
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def load(self, conversation_id: str) -> ConversationState:
        with self._lock:
            state = self._states.get(conversation_id)
            if state is None:
                state = self._load_from_disk(conversation_id) or ConversationState(conversation_id=conversation_id)
                self._states[conversation_id] = state
            return state

    def save(self, state: ConversationState) -> None:
        with self._lock:
            self._states[state.conversation_id] = state
            self._save_to_disk(state)

    def clear(self, conversation_id: str) -> None:
        with self._lock:
            self._states.pop(conversation_id, None)
            self._state_path(conversation_id).unlink(missing_ok=True)

    def clear_all(self) -> None:
        with self._lock:
            self._states.clear()
            for path in self._base_dir.glob("*.json"):
                path.unlink(missing_ok=True)
