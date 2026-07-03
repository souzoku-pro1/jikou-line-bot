"""チャネルアダプタの共通インターフェース

設計: docs/architecture/03-common-components.md §5.4
- アダプタは発送管理レコード（App 30）だけを入力とする（ユニット非依存）
- アダプタから LINE を直接呼ばない（通知は hub/notify 経由のみ）
- 承認判断を行わない（状態遷移は hub/approval・ディスパッチャの責務）
"""

from dataclasses import dataclass, field

DOCX_MIME = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
PDF_MIME = "application/pdf"


@dataclass
class Artifact:
    """prepare が生成する成果物（App 30 の 成果物 フィールドに添付される）"""
    filename: str
    content: bytes
    mime: str = PDF_MIME


@dataclass
class PrepareResult:
    artifacts: list[Artifact] = field(default_factory=list)
    fields: dict = field(default_factory=dict)   # App 30 へ追記するフィールド（宛先の自動解決等）


@dataclass
class DispatchResult:
    manual_mailing: bool = False   # True = 物理郵送（印刷・投函は人。発送処理中で停止）
    fields: dict = field(default_factory=dict)   # 書き戻し（外部APIの job_id 等）


class ChannelAdapter:
    """チャネルアダプタの基底クラス。M1〜M5 が継承して実装する。"""

    channel_name: str = ""      # App 30 チャネル欄の値と完全一致させる
    needs_return: bool = False  # 発送済後に返送待ちへ遷移するか（既定）

    async def prepare(self, record: dict) -> PrepareResult:
        """下書き→承認待ちの間に成果物（docx/PDF/CSV）を生成する"""
        raise NotImplementedError

    async def dispatch(self, record: dict) -> DispatchResult:
        """承認済後の発送実行。外部APIなしのチャネルは DispatchResult(manual_mailing=True)"""
        raise NotImplementedError

    async def reprocess(self, record: dict) -> None:
        """要確認レコードの再処理（M5 用・任意実装）"""
        return None
