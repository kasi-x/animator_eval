"""config モジュールの基本テスト."""

from src.utils.config import DAMPING_FACTOR, ROOT_DIR


def test_root_dir_exists():
    assert ROOT_DIR.exists()


def test_damping_factor_range():
    assert 0 < DAMPING_FACTOR < 1
