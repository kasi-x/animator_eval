"""pytest 共通設定."""

import structlog


def pytest_configure(config):
    """structlog をテスト用に設定する.

    テスト時はログ出力を抑制し、pytest の出力キャプチャとの衝突を回避する。
    """
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(colors=False),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(0),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )
    config.addinivalue_line(
        "markers",
        "requires_meta_tables: skip unless the meta_* tables are populated",
    )
