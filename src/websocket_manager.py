"""WebSocket マネージャー — リアルタイム進捗配信.

パイプライン実行中の進捗、スコア更新などをWebSocket経由で配信。
複数クライアントへのブロードキャスト対応。
"""

import asyncio
from typing import Any

import structlog
from fastapi import WebSocket

logger = structlog.get_logger()


class WebSocketManager:
    """WebSocket接続管理とブロードキャスト配信クラス.

    複数のWebSocketクライアントを管理し、メッセージを一斉配信。
    """

    def __init__(self):
        """Initialize WebSocket manager."""
        self.active_connections: list[WebSocket] = []
        self.message_queue: asyncio.Queue = asyncio.Queue()

    async def connect(self, websocket: WebSocket):
        """Accept new WebSocket connection.

        Args:
            websocket: FastAPI WebSocket instance
        """
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info("websocket_connected", total_connections=len(self.active_connections))

    def disconnect(self, websocket: WebSocket):
        """Remove WebSocket connection.

        Args:
            websocket: FastAPI WebSocket instance
        """
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info("websocket_disconnected", total_connections=len(self.active_connections))

    async def send_personal_message(self, message: dict, websocket: WebSocket):
        """Send message to specific WebSocket client.

        Args:
            message: Message dict (will be JSON serialized)
            websocket: Target WebSocket connection
        """
        await websocket.send_json(message)

    async def broadcast(self, message: dict):
        """Broadcast message to all connected WebSocket clients.

        Args:
            message: Message dict (will be JSON serialized)
        """
        if not self.active_connections:
            return

        # Remove disconnected clients
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.warning("websocket_send_failed", error=str(e))
                disconnected.append(connection)

        # Clean up disconnected clients
        for conn in disconnected:
            self.disconnect(conn)

    def broadcast_sync(self, message: dict):
        """Synchronous wrapper for broadcast (for non-async contexts).

        Args:
            message: Message dict (will be JSON serialized)
        """
        # Queue message for async processing
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If event loop is running, create task
                asyncio.create_task(self.broadcast(message))
            else:
                # If no loop, run in new event loop
                asyncio.run(self.broadcast(message))
        except RuntimeError:
            # No event loop available, skip broadcast
            logger.debug("websocket_broadcast_skipped_no_loop", message_type=message.get("type"))


# Global WebSocket manager instance
manager = WebSocketManager()


def get_websocket_manager() -> WebSocketManager:
    """Get global WebSocket manager instance.

    Returns:
        WebSocketManager singleton instance
    """
    return manager


class PipelineProgressBroadcaster:
    """Pipeline進捗をWebSocket配信するヘルパークラス.

    パイプライン実行中に使用し、各フェーズの進捗をリアルタイム配信。
    """

    def __init__(self, manager: WebSocketManager | None = None):
        """Initialize progress broadcaster.

        Args:
            manager: WebSocket manager (defaults to global instance)
        """
        self.manager = manager or get_websocket_manager()
        self.current_phase = 0
        self.total_phases = 10

    def start_pipeline(self, total_phases: int = 10):
        """Pipeline開始を通知.

        Args:
            total_phases: Total number of pipeline phases
        """
        self.total_phases = total_phases
        self.current_phase = 0

        self.manager.broadcast_sync({
            "type": "pipeline_start",
            "total_phases": total_phases,
            "timestamp": self._get_timestamp(),
        })

    def update_phase(self, phase: int, phase_name: str, status: str = "running"):
        """フェーズ進捗を更新.

        Args:
            phase: Phase number (1-indexed)
            phase_name: Phase name
            status: Status ("running", "complete", "error")
        """
        self.current_phase = phase

        self.manager.broadcast_sync({
            "type": "phase_update",
            "phase": phase,
            "phase_name": phase_name,
            "status": status,
            "progress": round(phase / self.total_phases * 100, 1),
            "timestamp": self._get_timestamp(),
        })

    def complete_phase(self, phase: int, phase_name: str, duration_ms: float | None = None):
        """フェーズ完了を通知.

        Args:
            phase: Phase number (1-indexed)
            phase_name: Phase name
            duration_ms: Optional duration in milliseconds
        """
        message = {
            "type": "phase_complete",
            "phase": phase,
            "phase_name": phase_name,
            "progress": round(phase / self.total_phases * 100, 1),
            "timestamp": self._get_timestamp(),
        }

        if duration_ms is not None:
            message["duration_ms"] = round(duration_ms, 2)

        self.manager.broadcast_sync(message)

    def error_phase(self, phase: int, phase_name: str, error: str):
        """フェーズエラーを通知.

        Args:
            phase: Phase number (1-indexed)
            phase_name: Phase name
            error: Error message
        """
        self.manager.broadcast_sync({
            "type": "phase_error",
            "phase": phase,
            "phase_name": phase_name,
            "error": error,
            "timestamp": self._get_timestamp(),
        })

    def complete_pipeline(self, total_persons: int, duration_seconds: float):
        """Pipeline完了を通知.

        Args:
            total_persons: Total persons scored
            duration_seconds: Total pipeline duration
        """
        self.manager.broadcast_sync({
            "type": "pipeline_complete",
            "total_persons": total_persons,
            "duration_seconds": round(duration_seconds, 2),
            "timestamp": self._get_timestamp(),
        })

    def send_custom_message(self, message_type: str, data: dict[str, Any]):
        """カスタムメッセージを配信.

        Args:
            message_type: Message type identifier
            data: Message data dict
        """
        message = {
            "type": message_type,
            "timestamp": self._get_timestamp(),
            **data,
        }

        self.manager.broadcast_sync(message)

    def _get_timestamp(self) -> str:
        """Get current timestamp in ISO format.

        Returns:
            ISO 8601 timestamp string
        """
        from datetime import datetime
        return datetime.now().isoformat()
