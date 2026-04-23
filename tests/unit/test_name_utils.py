"""Tests for src/utils/name_utils — script detection, nationality inference, JSON loading."""
from __future__ import annotations

import json

from src.utils.name_utils import (
    assign_native_name_fields,
    assign_native_title_fields,
    detect_name_script,
    infer_nationalities,
    _HOMETOWN_CACHE,
    _TOKENS_JSON_PATH,
    _load_tokens_json,
)


class TestDetectNameScript:
    def test_hiragana(self):
        assert detect_name_script("さとう") == "ja"

    def test_katakana(self):
        assert detect_name_script("ミヤザキ") == "ja"

    def test_hangul(self):
        assert detect_name_script("홍길동") == "ko"

    def test_cjk_only(self):
        assert detect_name_script("宮崎駿") == "zh_or_ja"

    def test_arabic(self):
        assert detect_name_script("محمد") == "ar"

    def test_latin(self):
        assert detect_name_script("John Smith") == "en"

    def test_empty(self):
        assert detect_name_script("") == "en"


class TestInferNationalitiesTokenSets:
    def test_hangul_script_returns_kr(self):
        assert infer_nationalities("홍길동", None) == ["KR"]

    def test_hiragana_returns_jp(self):
        assert infer_nationalities("みやざき", None) == ["JP"]

    def test_thai_returns_th(self):
        assert infer_nationalities("สมชาย", None) == ["TH"]

    def test_cjk_japan_hometown(self):
        assert infer_nationalities("荒木哲郎", "Sayama, Saitama, Japan") == ["JP"]

    def test_cjk_tokyo(self):
        assert infer_nationalities("宮崎駿", "Tokyo, Japan") == ["JP"]

    def test_cjk_chinese_hometown(self):
        assert infer_nationalities("王明", "Beijing, China") == ["CN"]

    def test_cjk_korean_hometown(self):
        assert infer_nationalities("김민준", "Seoul") == ["KR"]

    def test_cjk_unknown_hometown_no_llm(self):
        assert infer_nationalities("张三", "Narnia") == []

    def test_no_hometown_returns_empty(self):
        assert infer_nationalities("宮崎駿", None) == []

    def test_arabic_egypt(self):
        result = infer_nationalities("محمد", "Cairo, Egypt")
        assert result == ["EG"]

    def test_arabic_unknown(self):
        result = infer_nationalities("محمد", "Somewhere unknown")
        assert result == []


class TestInferNationalitiesCache:
    def test_cache_hit_skips_llm(self, monkeypatch):
        monkeypatch.setitem(_HOMETOWN_CACHE, "Testville, Testland", "JP")
        result = infer_nationalities("宮崎", "Testville, Testland", use_llm=True)
        assert result == ["JP"]

    def test_cache_null_returns_empty(self, monkeypatch):
        monkeypatch.setitem(_HOMETOWN_CACHE, "Unknown Planet", None)
        result = infer_nationalities("宮崎", "Unknown Planet", use_llm=True)
        assert result == []

    def test_llm_result_stored_in_cache(self, monkeypatch, tmp_path):
        # Patch JSON path to a temp file so we don't pollute real cache
        fake_json = tmp_path / "hometown_tokens.json"
        fake_json.write_text(json.dumps(
            {"tokens": {"JP": [], "CN": [], "KR": []}, "arabic_tokens": {}, "_cache": {}}
        ), encoding="utf-8")
        monkeypatch.setattr("src.utils.name_utils._TOKENS_JSON_PATH", fake_json)
        # Remove key from in-memory cache to force LLM path
        _HOMETOWN_CACHE.pop("Noplace, Neverland", None)

        def fake_llm(hometown):
            return "JP"

        monkeypatch.setattr("src.utils.name_utils._llm_infer_nationality", fake_llm)
        result = infer_nationalities("宮崎", "Noplace, Neverland", use_llm=True)
        assert result == ["JP"]
        assert _HOMETOWN_CACHE.get("Noplace, Neverland") == "JP"

    def test_llm_none_stored_as_null(self, monkeypatch, tmp_path):
        fake_json = tmp_path / "hometown_tokens.json"
        fake_json.write_text(json.dumps(
            {"tokens": {"JP": [], "CN": [], "KR": []}, "arabic_tokens": {}, "_cache": {}}
        ), encoding="utf-8")
        monkeypatch.setattr("src.utils.name_utils._TOKENS_JSON_PATH", fake_json)
        _HOMETOWN_CACHE.pop("Mystery Island", None)

        monkeypatch.setattr("src.utils.name_utils._llm_infer_nationality", lambda h: None)
        result = infer_nationalities("宮崎", "Mystery Island", use_llm=True)
        assert result == []
        assert _HOMETOWN_CACHE.get("Mystery Island") is None


class TestJsonLoading:
    def test_tokens_json_exists(self):
        assert _TOKENS_JSON_PATH.exists()

    def test_json_has_required_keys(self):
        data = _load_tokens_json()
        assert "tokens" in data
        assert "arabic_tokens" in data
        assert "_cache" in data

    def test_jp_tokens_non_empty(self):
        data = _load_tokens_json()
        assert len(data["tokens"].get("JP", [])) > 10

    def test_corrupt_json_returns_defaults(self, monkeypatch, tmp_path):
        bad_path = tmp_path / "bad.json"
        bad_path.write_text("NOT JSON", encoding="utf-8")
        monkeypatch.setattr("src.utils.name_utils._TOKENS_JSON_PATH", bad_path)
        result = _load_tokens_json()
        assert result == {"tokens": {}, "arabic_tokens": {}, "_cache": {}}


class TestAssignNativeNameFields:
    def test_hiragana_goes_to_name_ja(self):
        ja, ko, zh, alt = assign_native_name_fields("みやざき", [])
        assert ja == "みやざき"

    def test_hangul_goes_to_name_ko(self):
        ja, ko, zh, alt = assign_native_name_fields("홍길동", [])
        assert ko == "홍길동"
        assert ja == ""

    def test_cjk_jp_nationality_goes_to_name_ja(self):
        ja, ko, zh, alt = assign_native_name_fields("宮崎駿", ["JP"])
        assert ja == "宮崎駿"
        assert zh == ""

    def test_cjk_cn_nationality_goes_to_name_zh(self):
        ja, ko, zh, alt = assign_native_name_fields("王明", ["CN"])
        assert zh == "王明"
        assert ja == ""

    def test_cjk_unknown_nationality_returns_empty(self):
        ja, ko, zh, alt = assign_native_name_fields("张三", [])
        assert ja == "" and ko == "" and zh == ""

    def test_thai_goes_to_names_alt(self):
        ja, ko, zh, alt = assign_native_name_fields("สมชาย วงศ์", [])
        assert ja == "" and ko == "" and zh == ""
        assert alt == {"th": "สมชาย วงศ์"}

    def test_arabic_goes_to_names_alt(self):
        ja, ko, zh, alt = assign_native_name_fields("محمد", [])
        assert alt == {"ar": "محمد"}

    def test_empty_string_returns_empty(self):
        assert assign_native_name_fields("", ["JP"]) == ("", "", "", {})


class TestAssignNativeTitleFields:
    def test_jp_country_goes_to_title_ja(self):
        ja, ko, zh, alt = assign_native_title_fields("風の谷のナウシカ", "JP")
        assert ja == "風の谷のナウシカ"
        assert ko == "" and zh == ""
        assert alt == {}

    def test_kr_country_goes_to_title_ko(self):
        ja, ko, zh, alt = assign_native_title_fields("극장판 짱구는 못말려", "KR")
        assert ko == "극장판 짱구는 못말려"
        assert ja == "" and zh == ""
        assert alt == {}

    def test_cn_country_goes_to_title_zh(self):
        ja, ko, zh, alt = assign_native_title_fields("白蛇", "CN")
        assert zh == "白蛇"
        assert ja == "" and ko == ""
        assert alt == {}

    def test_tw_country_goes_to_title_zh(self):
        ja, ko, zh, alt = assign_native_title_fields("幽遊白書", "TW")
        assert zh == "幽遊白書"
        assert ja == "" and ko == ""
        assert alt == {}

    def test_hk_country_goes_to_title_zh(self):
        ja, ko, zh, alt = assign_native_title_fields("頭文字D", "HK")
        assert zh == "頭文字D"
        assert ja == "" and ko == ""
        assert alt == {}

    def test_other_country_goes_to_titles_alt(self):
        ja, ko, zh, alt = assign_native_title_fields("Wakfu", "FR")
        assert ja == "" and ko == "" and zh == ""
        assert alt == {"native": "Wakfu"}

    def test_none_country_defaults_to_jp(self):
        ja, ko, zh, alt = assign_native_title_fields("Dr.STONE", None)
        assert ja == "Dr.STONE"
        assert ko == "" and zh == ""
        assert alt == {}

    def test_empty_string_returns_empty(self):
        ja, ko, zh, alt = assign_native_title_fields("", "JP")
        assert ja == "" and ko == "" and zh == ""
        assert alt == {}

    def test_empty_country_defaults_to_jp(self):
        ja, ko, zh, alt = assign_native_title_fields("進撃の巨人", "")
        assert ja == "進撃の巨人"
        assert ko == "" and zh == ""
        assert alt == {}
