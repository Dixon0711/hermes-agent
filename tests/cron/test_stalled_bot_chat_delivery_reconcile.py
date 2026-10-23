"""A job stuck at ``last_status="delivery_queued"`` whose Bot Chat admission settles AFTER its
own firing run's bookkeeping must still be reconciled — not left at ``delivery_queued`` forever.

Bot Chat deliveries never touch ``cron.delivery_queue`` (that's platform sends only), so the
existing ``settle_queued_delivery``/``settle_quietly`` write-back (which only ever reads the
queue) never sees a Bot Chat receipt. ``bot_chat_settled_outcome`` recomputes the producer's
deterministic receipt id and checks ``runtime/bot_live_delivery/<id>.json`` directly;
``reconcile_stalled_bot_chat_deliveries`` sweeps every stuck job once per tick.
"""
import hashlib
import json

import pytest

from cron import delivery_outcome, executions, jobs


def _receipt_key(source_home, job_id, execution_id, target_home):
    return hashlib.sha256(json.dumps(
        [str(source_home), str(job_id), str(execution_id), str(target_home)],
        ensure_ascii=False, separators=(",", ":")).encode("utf-8")).hexdigest()


def _stalled_job(tmp_path, monkeypatch):
    """A no_agent job whose firing run booked ``delivery_queued`` and is the job's current
    execution. ``no_agent=True`` is deliberate: these jobs never populate
    ``canonical_completions`` (``scheduler_bookkeeping.finish_completed_run`` only passes
    ``execution_id`` into ``mark_job_run`` for agent-driven jobs), which is exactly the real
    case (``wissen-sync-01``) this reconcile exists for — the fixture must not fake that field
    in or it would mask the gap the fix closes."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    job = jobs.create_job(prompt=None, schedule="every 1h", deliver="bot-chat", no_agent=True,
                          script="true")
    execution = executions.create_execution(job["id"], source="builtin")
    executions.finish_execution(execution["id"], success=True, delivery_outcome="queued")
    jobs.update_job(job["id"], {"last_status": "delivery_queued"})
    return job["id"], execution["id"]


def _write_settled_receipt(tmp_path, job_id, execution_id):
    key = _receipt_key(tmp_path, job_id, execution_id, tmp_path)
    mailbox = tmp_path / "runtime" / "bot_live_delivery"
    mailbox.mkdir(parents=True)
    (mailbox / f"{key}.json").write_text(json.dumps({
        "status": "settled", "delivery_id": key, "reply": "ok",
    }))


def test_settled_receipt_is_reconciled_to_delivered(tmp_path, monkeypatch):
    job_id, execution_id = _stalled_job(tmp_path, monkeypatch)
    _write_settled_receipt(tmp_path, job_id, execution_id)
    # The real case this fix targets: no_agent jobs never populate canonical_completions, so
    # the execution id must come from latest_execution(), and _settle_job_record's fence must
    # not require it either.
    assert not jobs.get_job(job_id).get("canonical_completions")

    assert delivery_outcome.bot_chat_settled_outcome(job_id, execution_id) == "delivered"
    settled = delivery_outcome.reconcile_stalled_bot_chat_deliveries()

    assert settled == 1
    saved = jobs.get_job(job_id)
    assert saved["last_status"] == "ok"
    row = executions.get_execution(execution_id)
    assert row["delivery_outcome"] == "delivered"


@pytest.mark.parametrize("receipt_status", ["queued", "claimed", None])
def test_unsettled_or_missing_receipt_stays_queued(tmp_path, monkeypatch, receipt_status):
    job_id, execution_id = _stalled_job(tmp_path, monkeypatch)
    if receipt_status is not None:
        key = _receipt_key(tmp_path, job_id, execution_id, tmp_path)
        mailbox = tmp_path / "runtime" / "bot_live_delivery"
        mailbox.mkdir(parents=True)
        (mailbox / f"{key}.json").write_text(json.dumps({
            "status": receipt_status, "delivery_id": key,
        }))

    assert delivery_outcome.bot_chat_settled_outcome(job_id, execution_id) is None
    settled = delivery_outcome.reconcile_stalled_bot_chat_deliveries()

    assert settled == 0
    saved = jobs.get_job(job_id)
    assert saved["last_status"] == "delivery_queued"
    row = executions.get_execution(execution_id)
    assert row["delivery_outcome"] == "queued"


def test_tick_runs_the_reconcile_without_blocking_due_jobs(tmp_path, monkeypatch):
    """The reconcile sweep rides ``_pre_dispatch_housekeeping`` and never stops a due job."""
    import cron.jobs as J
    import cron.scheduler as S
    from cron import scheduler_authority

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setattr(J, "HERMES_DIR", tmp_path)
    monkeypatch.setattr(J, "CRON_DIR", tmp_path / "cron")
    monkeypatch.setattr(J, "JOBS_FILE", tmp_path / "cron" / "jobs.json")
    monkeypatch.setattr(J, "OUTPUT_DIR", tmp_path / "cron" / "output")
    monkeypatch.setattr(S, "_hermes_home", tmp_path)
    monkeypatch.setattr(S, "_sweep_mcp_orphans", lambda: None)
    monkeypatch.setattr(scheduler_authority, "reconcile_pending", lambda *, allow_connect=True: None)
    (tmp_path / "state.db").write_text("")

    job_id, execution_id = _stalled_job(tmp_path, monkeypatch)
    _write_settled_receipt(tmp_path, job_id, execution_id)

    S.tick(verbose=False)

    saved = jobs.get_job(job_id)
    assert saved["last_status"] == "ok"
