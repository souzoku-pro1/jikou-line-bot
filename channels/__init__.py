"""チャネルアダプタのレジストリ（発送/受領ハブ）

設計: docs/architecture/03-common-components.md §5.3-5.4
チャネル実装（M1〜M5）は各タスクで register() により追加される。
"""

from channels.base import ChannelAdapter

CHANNEL_REGISTRY: dict[str, ChannelAdapter] = {}


def register(adapter: ChannelAdapter) -> None:
    CHANNEL_REGISTRY[adapter.channel_name] = adapter


def get_adapter(channel_name: str) -> ChannelAdapter | None:
    return CHANNEL_REGISTRY.get(channel_name)


# ── 実装済みチャネルの登録（T2-2〜） ─────────────────────────────────────────
from channels.soufu_annai import SoufuAnnaiAdapter  # noqa: E402

register(SoufuAnnaiAdapter())
