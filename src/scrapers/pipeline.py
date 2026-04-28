"""High-level scrape pipeline orchestrator.

Wraps the lower-level ``ScrapeRunner`` + ``BronzeSink`` plumbing into a
single Protocol-based entry point so that source-specific scrapers only
need to supply three thin objects (Fetcher / Parser / Normalizer) plus a
``BronzeWriterGroup`` and ``Checkpoint``.

Layered responsibilities:

    Fetcher     — async ID → raw payload (HTML/JSON bytes, dict, …)
    Parser      — pure raw → typed Record (dataclass / Pydantic model)
    Normalizer  — Record → dict[table_name, list[row_dict]] for BRONZE

The pipeline preserves the existing ``BronzeSink`` row-stamping (hash +
fetched_at) and ``ScrapeRunner`` checkpoint/flush/progress behaviour;
only the public surface and the per-source coupling change.

Usage::

    from src.scrapers.pipeline import run_pipeline
    from src.scrapers.bronze_writer import BronzeWriterGroup
    from src.scrapers.checkpoint import Checkpoint

    fetcher    = MySourceFetcher(client)        # implements Fetcher
    parser     = MySourceParser()               # implements Parser
    normalizer = MySourceNormalizer()           # implements Normalizer

    with BronzeWriterGroup("mysource", tables=["anime", "credits"]) as g:
        cp = Checkpoint.load(checkpoint_path)
        stats = await run_pipeline(
            ids, fetcher, parser, normalizer, g, cp,
            label="mysource",
        )
"""

from __future__ import annotations

import dataclasses
from typing import Protocol, TypeVar, runtime_checkable

from src.scrapers.bronze_writer import BronzeWriterGroup
from src.scrapers.checkpoint import Checkpoint
from src.scrapers.runner import ScrapeRunner
from src.scrapers.sinks import BronzeSink

RawT = TypeVar("RawT")
RecT = TypeVar("RecT")


@runtime_checkable
class Fetcher(Protocol[RawT]):
    """Async ID → raw payload. Return ``None`` to skip (404, parse-guard, …).

    Exceptions raised here are caught by ``ScrapeRunner`` and counted as errors.
    """

    async def fetch(self, id: str) -> RawT | None: ...


@runtime_checkable
class Parser(Protocol[RawT, RecT]):
    """Pure (raw, id) → Record. Return ``None`` to skip (not-anime, missing data).

    Exceptions raised here are caught by ``ScrapeRunner`` and counted as errors.
    """

    def parse(self, raw: RawT, id: str) -> RecT | None: ...


@runtime_checkable
class Normalizer(Protocol[RecT]):
    """Record → ``{table_name: [row_dict, ...]}`` for BRONZE writer group.

    Returned keys must match the tables registered on the ``BronzeWriterGroup``.
    Return ``{}`` to write no rows for this record.
    """

    def normalize(self, rec: RecT) -> dict[str, list[dict]]: ...


@dataclasses.dataclass
class PipelineStats:
    """Outcome counts for a ``run_pipeline`` invocation.

    Attributes:
        fetched:  IDs for which the fetcher returned a non-None payload.
        parsed:   Records produced by the parser (parser returned non-None).
        written:  Total BRONZE rows written (sum across tables).
        failed:   Exceptions raised in fetch / parse / sink steps.
        skipped:  IDs the pipeline did not advance past fetch or parse
                  (fetcher → None, or parser → None).
    """

    fetched: int = 0
    parsed: int = 0
    written: int = 0
    failed: int = 0
    skipped: int = 0


async def run_pipeline(
    ids: list[str],
    fetcher: Fetcher[RawT],
    parser: Parser[RawT, RecT],
    normalizer: Normalizer[RecT],
    writer: BronzeWriterGroup,
    checkpoint: Checkpoint,
    *,
    limit: int = 0,
    progress_override: bool | None = None,
    label: str = "",
) -> PipelineStats:
    """Run fetch → parse → normalize → BRONZE write loop with checkpointing.

    Internally adapts the Protocol triple to ``ScrapeRunner`` + ``BronzeSink``
    so existing checkpoint/flush/progress behaviour is preserved.

    Args:
        ids:               Candidate ID list (already-completed IDs are skipped).
        fetcher:           ``Fetcher[RawT]`` implementation.
        parser:            ``Parser[RawT, RecT]`` implementation.
        normalizer:        ``Normalizer[RecT]`` implementation.
        writer:            Open ``BronzeWriterGroup`` (caller manages lifecycle).
        checkpoint:        Loaded ``Checkpoint``; updated in-place.
        limit:             Cap to first N pending IDs; 0 = no cap.
        progress_override: True/False to force progress mode; None = auto.
        label:             Short tag for log/progress events.

    Returns:
        ``PipelineStats`` summarising the run.
    """
    fetched_count = [0]
    parsed_count = [0]

    async def _fetch(id_: str) -> RawT | None:
        raw = await fetcher.fetch(id_)
        if raw is not None:
            fetched_count[0] += 1
        return raw

    def _parse(raw: RawT, id_: str) -> RecT | None:
        rec = parser.parse(raw, id_)
        if rec is not None:
            parsed_count[0] += 1
        return rec

    sink = BronzeSink(group=writer, mapper=normalizer.normalize)

    runner = ScrapeRunner(
        fetcher=_fetch,
        parser=_parse,
        sink=sink,
        checkpoint=checkpoint,
        label=label or "pipeline",
        flush=writer.flush_all,
    )
    stats = await runner.run(ids, limit=limit, progress_override=progress_override)

    return PipelineStats(
        fetched=fetched_count[0],
        parsed=parsed_count[0],
        written=stats.written,
        failed=stats.errors,
        skipped=stats.skipped,
    )
