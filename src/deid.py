"""De-identification: apply config/deid_policy.yaml to a row from the EHR.

Pseudonyms and date shifts are derived from a secret key with HMAC-SHA256. Without the key,
tokens cannot be reversed or recomputed from known identifiers; with the same key, the same
patient always gets the same token and the same date shift, so joins and intervals survive.
"""

from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

ACTIONS = {"token", "shift_date", "birth_year", "zip3", "keep", "drop"}

# 3-digit ZIP areas with 20,000 or fewer people (HHS Safe Harbor guidance, 2000 Census).
# Their ZIP3 must be replaced with 000.
RESTRICTED_ZIP3 = frozenset({"036", "059", "063", "102", "203", "556", "692", "790", "821",
                             "823", "830", "831", "878", "879", "884", "890", "893"})  # fmt: skip


class PolicyError(ValueError):
    pass


def load_policy(path: str | Path) -> dict[str, dict[str, str]]:
    import yaml

    tables = (yaml.safe_load(Path(path).read_text()) or {}).get("tables") or {}
    for table, columns in tables.items():
        for column, action in columns.items():
            if action not in ACTIONS:
                raise PolicyError(f"{table}.{column}: unknown action {action!r}")
        if "patient_id" not in columns:
            raise PolicyError(f"{table}: every table needs patient_id (tokens and date shifts)")
    return tables


@dataclass
class Deidentifier:
    key: bytes
    policy: dict[str, dict[str, str]]
    max_shift_days: int = 365
    today: date = field(default_factory=date.today)

    def __post_init__(self):
        if len(self.key) < 32:
            raise ValueError("de-identification key must be at least 32 bytes")

    # --------------------------------------------------------------- primitives
    def _mac(self, purpose: str, value) -> bytes:
        return hmac.new(self.key, f"{purpose}:{value}".encode(), hashlib.sha256).digest()

    def token(self, kind: str, value) -> str | None:
        """Per-kind namespace: a patient token can never be matched to an encounter token."""
        if value is None:
            return None
        return f"{kind[:3]}_{self._mac(f'token:{kind}', value).hex()[:24]}"

    def shift_days(self, patient_id) -> int:
        """Stable per-patient offset: -max..-1 or 1..max days, never 0."""
        n = int.from_bytes(self._mac("shift", patient_id)[:8], "big")
        days = n % (2 * self.max_shift_days) - self.max_shift_days  # -max .. max-1
        return days + 1 if days >= 0 else days

    def shift(self, value, patient_id):
        if value is None:
            return None
        if patient_id is None:
            raise PolicyError("cannot shift a date without the row's patient_id")
        return value + timedelta(days=self.shift_days(patient_id))

    def birth_year(self, dob: date | None) -> dict:
        if dob is None:
            return {"birth_year": None, "age_90_plus": None}
        age = (
            self.today.year - dob.year - ((self.today.month, self.today.day) < (dob.month, dob.day))
        )
        if age >= 90:
            return {"birth_year": None, "age_90_plus": True}
        return {"birth_year": dob.year, "age_90_plus": False}

    @staticmethod
    def zip3(zip_code: str | None) -> str | None:
        if not zip_code or len(zip_code.strip()) < 3:
            return None
        prefix = zip_code.strip()[:3]
        return "000" if prefix in RESTRICTED_ZIP3 else prefix

    # --------------------------------------------------------------- rows
    def apply(self, table: str, row: dict) -> tuple[dict, list[str]]:
        """De-identified row, plus the names of unclassified columns that were dropped."""
        if table not in self.policy:
            raise PolicyError(f"table {table!r} is not in the de-identification policy")
        rules = self.policy[table]
        patient_id = row.get("patient_id")
        out, unclassified = {}, []
        for column, value in row.items():
            action = rules.get(column)
            if action is None:
                unclassified.append(column)
            elif action == "keep":
                out[column] = value
            elif action == "token":
                kind = column.removesuffix("_id")
                out[f"{kind}_token"] = self.token(kind, value)
            elif action == "shift_date":
                out[column] = self.shift(value, patient_id)
            elif action == "birth_year":
                out.update(self.birth_year(value))
            elif action == "zip3":
                out["zip3"] = self.zip3(value)
        return out, sorted(unclassified)
