"""Download queue management for background image processing."""

import json
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import List
import structlog

log = structlog.get_logger()


@dataclass
class DownloadItem:
    """Single download queue item."""

    item_id: str  # person_id or anime_id
    item_type: str  # "person" or "anime"
    url_large: str = None  # large image URL
    url_medium: str = None  # medium image URL
    url_banner: str = None  # banner URL (anime only)
    added_at: float = None  # timestamp


class DownloadQueue:
    """Manages image download queue with JSON persistence."""

    def __init__(self, queue_file: Path = None):
        """Initialize queue.

        Args:
            queue_file: Path to queue JSON file. If None, uses default location.
        """
        if queue_file is None:
            queue_file = (
                Path(__file__).parent.parent.parent / "data" / "download_queue.json"
            )

        self.queue_file = queue_file
        self.queue_file.parent.mkdir(parents=True, exist_ok=True)
        self.items: List[DownloadItem] = []

        # Load existing queue from file
        self._load_from_file()

    def _load_from_file(self) -> None:
        """Load queue from JSON file."""
        if not self.queue_file.exists():
            return

        try:
            with open(self.queue_file) as f:
                data = json.load(f)
                self.items = [DownloadItem(**item) for item in data.get("items", [])]
            log.info("download_queue_loaded", count=len(self.items))
        except Exception as e:
            log.warning("download_queue_load_failed", error=str(e))
            self.items = []

    def _save_to_file(self) -> None:
        """Save queue to JSON file."""
        try:
            data = {
                "count": len(self.items),
                "items": [asdict(item) for item in self.items],
            }
            with open(self.queue_file, "w") as f:
                json.dump(data, f, indent=2, default=str)
        except Exception as e:
            log.warning("download_queue_save_failed", error=str(e))

    def add_person(
        self, person_id: str, url_large: str = None, url_medium: str = None
    ) -> None:
        """Add person image to download queue.

        Args:
            person_id: Person ID (e.g., "anilist:p12345")
            url_large: Large image URL
            url_medium: Medium image URL
        """
        import time

        if url_large or url_medium:
            item = DownloadItem(
                item_id=person_id,
                item_type="person",
                url_large=url_large,
                url_medium=url_medium,
                added_at=time.time(),
            )
            self.items.append(item)
            self._save_to_file()

    def add_anime(
        self,
        anime_id: str,
        url_large: str = None,
        url_extra_large: str = None,
        url_banner: str = None,
    ) -> None:
        """Add anime image to download queue.

        Args:
            anime_id: Anime ID (e.g., "anilist:12345")
            url_large: Large cover image URL
            url_extra_large: Extra large cover image URL
            url_banner: Banner image URL
        """
        import time

        if url_large or url_extra_large or url_banner:
            item = DownloadItem(
                item_id=anime_id,
                item_type="anime",
                url_large=url_extra_large or url_large,  # Use extra_large if available
                url_medium=url_large,  # Store large as medium
                url_banner=url_banner,
                added_at=time.time(),
            )
            self.items.append(item)
            self._save_to_file()

    def remove_item(self, item_id: str) -> None:
        """Remove item from queue by ID.

        Args:
            item_id: Item ID to remove
        """
        self.items = [item for item in self.items if item.item_id != item_id]
        self._save_to_file()

    def get_all_items(self) -> List[DownloadItem]:
        """Get all queued items.

        Returns:
            List of DownloadItem objects
        """
        return self.items.copy()

    def clear(self) -> None:
        """Clear all items from queue."""
        self.items = []
        self._save_to_file()

    def count(self) -> int:
        """Get queue size.

        Returns:
            Number of items in queue
        """
        return len(self.items)

    def is_empty(self) -> bool:
        """Check if queue is empty.

        Returns:
            True if queue is empty
        """
        return len(self.items) == 0

    def get_persons(self) -> List[DownloadItem]:
        """Get all person download items.

        Returns:
            List of person DownloadItem objects
        """
        return [item for item in self.items if item.item_type == "person"]

    def get_anime(self) -> List[DownloadItem]:
        """Get all anime download items.

        Returns:
            List of anime DownloadItem objects
        """
        return [item for item in self.items if item.item_type == "anime"]
