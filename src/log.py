"""structlog configuration."""

import sys

import structlog


def setup_logging(json_output: bool = False) -> None:
    """Configure structlog."""
    processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="iso"),
    ]

    if json_output:
        processors.append(structlog.processors.JSONRenderer(ensure_ascii=False))
    else:
        processors.append(
            structlog.dev.ConsoleRenderer(
                colors=sys.stderr.isatty(),
            )
        )

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(0),  # DEBUG以上
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )
