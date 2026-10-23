"""Write a queued cron delivery's terminal outcome back onto the run that queued it.

An agent run hands its send to ``cron.delivery_queue`` and books ``last_status =
"delivery_queued"`` / ledger ``delivery_outcome = "queued"``. Once a gateway drain sends,
refuses, suppresses or fences the send ``unknown``, the job record and the execution
ledger must carry the real result (main's inline ``ok``/``delivered`` and
``delivery_failed``/``failed`` + ``last_delivery_error``); otherwise ``hermes cron list``
says "delivery still in progress" forever and refused sends never surface.

Both sides call :func:`settle_queued_delivery`: the drain right after it terminalizes a
row, and the run right after its own bookkeeping (a drain can finish before the run's
``mark_job_run`` lands). Every write is fenced on the execution id, so the call is
idempotent and never rewrites a later run's status.
"""
from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Queue terminal status -> ledger delivery_outcome. ``unknown`` (owner died mid-send, never
# retried) is surfaced as a failure: the user cannot be told the result arrived.
_LEDGER_OUTCOME = {
    "delivered": "delivered", "suppressed": "suppressed", "failed": "failed", "unknown": "failed",
}


def _settle_job_record(job_id: str, execution_id: str, delivery_error: Optional[str]) -> bool:
    """Stamp the outcome onto the job only while *execution_id* is still its current run:
    the ledger's latest attempt for the job AND (agent jobs only) the job's latest recorded
    completion. ``no_agent`` jobs never populate ``canonical_completions`` —
    ``scheduler_bookkeeping.finish_completed_run`` only passes ``execution_id`` into
    ``mark_job_run`` ``if not job.get('no_agent')`` — so for them the ledger fence above
    (the job's single latest attempt) is the only fence available; requiring
    ``canonical_completions`` too would make a ``no_agent`` job's ``delivery_queued`` status
    permanently unsettleable."""
    from cron.executions import latest_execution
    from cron.jobs import _with_job, save_jobs

    latest = latest_execution(job_id)
    if latest is None or latest.get("id") != execution_id:
        return False

    def apply(jobs, _i, job):
        completions = job.get("canonical_completions") or []
        if not job.get("no_agent") and (not completions or completions[-1] != execution_id):
            return False
        status = job.get("last_status")
        if status == "delivery_queued":
            status = "delivery_failed" if delivery_error else "ok"
        if (status, delivery_error) == (job.get("last_status"), job.get("last_delivery_error")):
            return True
        job["last_status"] = status
        job["last_delivery_error"] = delivery_error
        save_jobs(jobs)
        return True

    return bool(_with_job(job_id, apply, missing=False))


def _terminal(execution_id: str) -> Optional[tuple[str, Optional[str]]]:
    """``(ledger_outcome, delivery_error)`` once the queue row is terminal, else None."""
    from cron.delivery_queue import get_status

    row = get_status(str(execution_id))
    if not row or row["status"] not in _LEDGER_OUTCOME:
        return None
    delivery_error = None
    if row["status"] in ("failed", "unknown"):
        delivery_error = str(row.get("error") or f"delivery {row['status']}")
    return _LEDGER_OUTCOME[row["status"]], delivery_error


def settled_outcome(execution_id: str) -> Optional[str]:
    """The ledger outcome a drain already reached for *execution_id* (None while queued)."""
    terminal = _terminal(execution_id)
    return terminal[0] if terminal else None


def settle_queued_delivery(job_id: Optional[str], execution_id: Optional[str]) -> bool:
    """Apply the queue's terminal outcome for *execution_id*; False while still in flight."""
    if not job_id or not execution_id:
        return False
    terminal = _terminal(str(execution_id))
    if terminal is None:
        return False
    from cron.executions import settle_delivery_outcome

    settle_delivery_outcome(str(execution_id), terminal[0])
    _settle_job_record(str(job_id), str(execution_id), terminal[1])
    return True


def settle_quietly(job_id: Optional[str], execution_id: Optional[str]) -> None:
    """Bookkeeping failure must not undo a terminal at-most-once send or fail its run."""
    try:
        settle_queued_delivery(job_id, execution_id)
    except Exception:
        logger.warning("Cron delivery %s: outcome write-back failed", execution_id, exc_info=True)


def bot_chat_settled_outcome(job_id: str, execution_id: str) -> Optional[str]:
    """``"delivered"`` once a Bot Chat admission for *execution_id* has settled, else None.

    Bot Chat deliveries (``deliver=bot-chat[:<profile>]``) never go through
    ``cron.delivery_queue`` — they are admitted directly to a profile's live owner
    (``tools.bot_live_delivery``) and their receipt lives under
    ``<profile_home>/runtime/bot_live_delivery/<delivery_id>.json``. ``_terminal`` above only
    ever reads the queue, so a run whose firing tick observed the admission still ``queued``
    (``scheduler_delivery._deliver_to_bot_chat``) never learns about a receipt that settles a
    moment later — nothing previously re-checked it. The receipt id is derived
    deterministically by the producer: ``sha256([source_home, job_id, execution_id,
    target_home])`` (see ``scheduler_delivery._deliver_to_bot_chat``), so it can be
    recomputed here without storing anything new. Checks this profile's own home and every
    local profile (named ``bot-chat:<profile>`` targets).
    """
    import hashlib
    import json

    from hermes_cli.profiles import get_profile_dir, list_profile_names
    from hermes_constants import get_hermes_home

    source_home = get_hermes_home().resolve()
    for profile_name in list_profile_names():
        try:
            target_home = get_profile_dir(profile_name).resolve()
        except Exception:
            continue
        key = hashlib.sha256(json.dumps(
            [str(source_home), str(job_id), str(execution_id), str(target_home)],
            ensure_ascii=False, separators=(",", ":")).encode("utf-8")).hexdigest()
        try:
            receipt = json.loads(
                (target_home / "runtime" / "bot_live_delivery" / f"{key}.json")
                .read_text(encoding="utf-8-sig"))
        except (OSError, ValueError):
            continue
        if isinstance(receipt, dict) and receipt.get("status") == "settled":
            return "delivered"
    return None


def reconcile_stalled_bot_chat_deliveries() -> int:
    """Settle every job stuck at ``last_status="delivery_queued"`` whose Bot Chat admission has
    since reached ``"settled"``. Runs once per cron tick (``cron.scheduler_tick``), alongside
    the legacy Bot Chat drain — the only periodic re-check this lane gets, since
    ``settle_queued_delivery``/``settle_quietly`` above only ever fire once, at the firing run's
    own bookkeeping. Writes through the same ``_settle_job_record`` + ``settle_delivery_outcome``
    pair ``settle_queued_delivery`` uses (never a direct ``jobs.json``/``executions.db`` write).

    Reads the execution id from ``latest_execution(job_id)``, not ``canonical_completions``:
    the latter is never populated for ``no_agent`` jobs (see ``_settle_job_record``), and a
    stalled ``no_agent`` job is exactly the case this sweep exists for.
    Returns the number of jobs settled.
    """
    from cron.executions import latest_execution, settle_delivery_outcome
    from cron.jobs import list_jobs

    settled = 0
    for job in list_jobs(include_disabled=True):
        if job.get("last_status") != "delivery_queued":
            continue
        job_id = job.get("id")
        if not job_id:
            continue
        latest = latest_execution(job_id)
        if latest is None:
            continue
        execution_id = str(latest["id"])
        if bot_chat_settled_outcome(job_id, execution_id) != "delivered":
            continue
        try:
            settle_delivery_outcome(execution_id, "delivered")
            if _settle_job_record(job_id, execution_id, None):
                settled += 1
        except Exception:
            logger.warning(
                "Stalled bot-chat delivery reconcile failed for job %s execution %s",
                job_id, execution_id, exc_info=True)
    return settled
