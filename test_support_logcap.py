"""テスト用のログ捕捉（TEST-FLAKY-1・QUEUE-2026-10-17-H H-2）。

既存テストの一部は root logger に Handler を付けて `hub.*` の INFO を拾っていたが、他のテスト（例: scripts/houki_jukuryo_precount の
import が `logging.getLogger("hub").setLevel(WARNING)` を残す）が **名前付き logger の level／propagate** を変えると、root 側の捕捉には
届かず順序依存で落ちる。本 helper は **当該 logger に直付け**し、その logger の level を一時的に下げて（propagate に依存せず）捕捉する。

- 肯定の検査（「件数ログが 1 行出る」）: 対象 module の logger 名を列挙して attach する。
- 否定の検査（「PII がどのログにも出ない」）: 対象 logger に加えて `include_root=True` で root にも付け、両方の経路を見る。
検査の意図（件数のみを出し値を出さない）は変えない。test_ 接頭辞は test_sink_ast_policy の走査対象外にするため（Handler.emit の
定義が emit_shadow と判定される）。テスト関数は持たない。
"""

import logging


class LogCapture(logging.Handler):
    """record を溜めるだけの Handler（level は DEBUG・formatter なし）。"""

    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.records: list[logging.LogRecord] = []
        self._seen: set[int] = set()

    def emit(self, record: logging.LogRecord) -> None:
        # 同じ Handler を名前付き logger と root の両方に付けたとき、propagate で同じ record が 2 回届くのを 1 回に数える
        if id(record) in self._seen:
            return
        self._seen.add(id(record))
        self.records.append(record)

    def text(self) -> str:
        return "\n".join(r.getMessage() for r in self.records)

    def messages(self) -> list[str]:
        return [r.getMessage() for r in self.records]

    def errors(self) -> list[logging.LogRecord]:
        return [r for r in self.records if r.levelno >= logging.ERROR]


def attach(testcase, logger_names, *, level: int = logging.DEBUG, include_root: bool = False) -> LogCapture:
    """unittest.TestCase の setUp から呼ぶ。各 logger に Handler を直付けし level を下げ、addCleanup で元に戻す。
    同じ record が複数の logger から二重に届かないよう、propagate で親に流れても root 側には同じ Handler を付けない
    （include_root=True のときだけ root に付ける＝名前付き logger からの propagate 分は重複し得るので、否定の検査にだけ使う）。"""
    cap = LogCapture()
    names = list(logger_names)
    if include_root:
        names.append("")
    for name in names:
        lg = logging.getLogger(name) if name else logging.getLogger()
        old_level = lg.level
        lg.addHandler(cap)
        lg.setLevel(level)
        testcase.addCleanup(lg.setLevel, old_level)
        testcase.addCleanup(lg.removeHandler, cap)
    return cap


class attached:
    """with 文版（関数内で一時的に捕捉する既存テスト向け）。"""

    def __init__(self, logger_names, *, level: int = logging.DEBUG, include_root: bool = False) -> None:
        self._names = list(logger_names) + ([""] if include_root else [])
        self._level = level
        self._saved: list[tuple[logging.Logger, int]] = []
        self.cap = LogCapture()

    def __enter__(self) -> LogCapture:
        for name in self._names:
            lg = logging.getLogger(name) if name else logging.getLogger()
            self._saved.append((lg, lg.level))
            lg.addHandler(self.cap)
            lg.setLevel(self._level)
        return self.cap

    def __exit__(self, *exc) -> bool:
        for lg, old in reversed(self._saved):
            lg.removeHandler(self.cap)
            lg.setLevel(old)
        return False
