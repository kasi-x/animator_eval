"""スクレイパー共通の例外階層."""


class ScraperError(Exception):
    """スクレイパーの基底例外.

    Attributes:
        source: データソース名 (e.g. "anilist", "mal", "mediaarts", "wikidata")
        url: リクエスト先URL
        metadata: 追加情報の辞書
    """

    def __init__(
        self,
        message: str,
        *,
        source: str = "",
        url: str = "",
        metadata: dict | None = None,
    ) -> None:
        super().__init__(message)
        self.source = source
        self.url = url
        self.metadata = metadata or {}


class RateLimitError(ScraperError):
    """レート制限に到達した場合の例外.

    Attributes:
        retry_after: リトライまでの秒数 (Retry-After ヘッダー由来)
    """

    def __init__(
        self,
        message: str = "Rate limited",
        *,
        source: str = "",
        url: str = "",
        retry_after: float = 60.0,
        metadata: dict | None = None,
    ) -> None:
        super().__init__(message, source=source, url=url, metadata=metadata)
        self.retry_after = retry_after


class AuthenticationError(ScraperError):
    """認証失敗の例外 (無効なトークン等)."""


class DataParseError(ScraperError):
    """レスポンスのパース失敗."""


class EndpointUnreachableError(ScraperError):
    """エンドポイントに到達不能 (リトライ上限超過)."""


class ContentValidationError(ScraperError):
    """ダウンロードしたコンテンツの検証失敗 (不正なContent-Type, サイズ不足等)."""
