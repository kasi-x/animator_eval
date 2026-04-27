"""Role mapper for Bangumi credits.

Bangumi staff roles arrive as integer codes (e.g. 1, 7, 54).
The authoritative code→meaning source is:
  src/scrapers/queries/labels/staffs.json  — 163 codes with en/zh/jp fields

Each code is mapped to a Role.value using the English label as the primary key,
with the shared ROLE_MAP as the lookup.  A hand-coded fallback table covers
codes whose English labels don't match any ROLE_MAP entry.

Input: integer code as int or string (e.g. 2, "2").
Output: Role.value string.  Falls back to Role.OTHER.value for unmapped codes.
"""
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

import structlog

from src.etl.role_mappers import register
from src.runtime.models import ROLE_MAP, Role

logger = structlog.get_logger()

_STAFFS_JSON = Path(__file__).parents[3] / "src" / "scrapers" / "queries" / "labels" / "staffs.json"

# Hand-coded overrides for codes whose English label doesn't match any ROLE_MAP key.
# Built by inspecting staffs.json en labels against ROLE_MAP.
_CODE_OVERRIDES: dict[int, Role] = {
    1: Role.ORIGINAL_CREATOR,     # "Original Creator/Original Work"
    2: Role.DIRECTOR,              # "Director/Direction"
    3: Role.SCREENPLAY,            # "Script/Screenplay"
    4: Role.EPISODE_DIRECTOR,      # "Storyboard"
    5: Role.EPISODE_DIRECTOR,      # "Episode Direction"
    6: Role.MUSIC,                 # "Music"
    7: Role.CHARACTER_DESIGNER,    # "Original Character Design"
    8: Role.CHARACTER_DESIGNER,    # "Character Design"
    9: Role.LAYOUT,                # "Layout"
    10: Role.SCREENPLAY,           # "Series Composition"
    11: Role.BACKGROUND_ART,       # "Art Direction"
    13: Role.FINISHING,            # "Color Design"
    14: Role.ANIMATION_DIRECTOR,   # "Chief Animation Director"
    15: Role.ANIMATION_DIRECTOR,   # "Animation Direction"
    16: Role.CHARACTER_DESIGNER,   # "Mechanical Design"
    17: Role.PHOTOGRAPHY_DIRECTOR, # "Director of Photography"
    18: Role.DIRECTOR,             # "Supervision/Supervisor"
    19: Role.SETTINGS,             # "Prop Design"
    20: Role.KEY_ANIMATOR,         # "Key Animation"
    21: Role.SECOND_KEY_ANIMATOR,  # "2nd Key Animation"
    22: Role.IN_BETWEEN,           # "Animation Check"
    23: Role.PRODUCER,             # "Assistant Producer"
    24: Role.PRODUCER,             # "Associate Producer"
    25: Role.BACKGROUND_ART,       # "Background Art"
    26: Role.FINISHING,            # "Color Setting"
    27: Role.FINISHING,            # "Digital Paint"
    28: Role.EDITING,              # "Editing"
    29: Role.ORIGINAL_CREATOR,     # "Original Plan"
    30: Role.MUSIC,                # "Theme Song Arrangement"
    31: Role.MUSIC,                # "Theme Song Composition"
    32: Role.MUSIC,                # "Theme Song Lyrics"
    33: Role.MUSIC,                # "Theme Song Performance"
    34: Role.MUSIC,                # "Inserted Song Performance"
    35: Role.PRODUCER,             # "Planning"
    36: Role.PRODUCER,             # "Planning Producer"
    37: Role.PRODUCTION_MANAGER,   # "Production Manager"
    38: Role.SPECIAL,              # "Publicity"
    39: Role.SOUND_DIRECTOR,       # "Recording"
    40: Role.SOUND_DIRECTOR,       # "Recording Assistant"
    41: Role.DIRECTOR,             # "Series Production Director"
    42: Role.PRODUCER,             # "Production"
    43: Role.SETTINGS,             # "Setting"
    44: Role.SOUND_DIRECTOR,       # "Sound Director"
    45: Role.SOUND_DIRECTOR,       # "Sound"
    46: Role.SOUND_DIRECTOR,       # "Sound Effects"
    47: Role.PHOTOGRAPHY_DIRECTOR, # "Special Effects"
    48: Role.SOUND_DIRECTOR,       # "ADR Director"
    49: Role.DIRECTOR,             # "Co-Director"
    50: Role.SETTINGS,             # "Setting (background)"
    51: Role.IN_BETWEEN,           # "In-Between Animation"
    52: Role.PRODUCER,             # "Executive Producer"
    53: Role.PRODUCER,             # "Assistant Producer"
    54: Role.PRODUCER,             # "Producer"
    55: Role.MUSIC,                # "Music Assistant"
    56: Role.PRODUCTION_MANAGER,   # "Assistant Production Manager"
    57: Role.PRODUCER,             # "Casting Director"
    58: Role.PRODUCER,             # "Chief Producer"
    59: Role.PRODUCER,             # "Co-Producer"
    60: Role.EDITING,              # "Dialogue Editing"
    61: Role.PRODUCTION_MANAGER,   # "Post-Production Assistant"
    62: Role.PRODUCTION_MANAGER,   # "Production Assistant"
    63: Role.PRODUCTION_MANAGER,   # "Production"
    64: Role.PRODUCTION_MANAGER,   # "Production Coordination"
    65: Role.MUSIC,                # "Music Work"
    66: Role.SPECIAL,              # "Special Thanks"
    67: Role.PRODUCTION_MANAGER,   # "Animation Work"
    69: Role.CGI_DIRECTOR,         # "CG Director"
    70: Role.ANIMATION_DIRECTOR,   # "Mechanical Animation Direction"
    71: Role.BACKGROUND_ART,       # "Art Design"
    72: Role.EPISODE_DIRECTOR,     # "Assistant Director"
    73: Role.EPISODE_DIRECTOR,     # "OP ED"
    74: Role.DIRECTOR,             # "Chief Director"
    75: Role.CGI_DIRECTOR,         # "3DCG"
    76: Role.PRODUCTION_MANAGER,   # "Work Assistance"
    77: Role.ANIMATION_DIRECTOR,   # "Action Animation Direction"
    80: Role.PRODUCER,             # "Supervising Producer"
    81: Role.SPECIAL,              # "Assistance"
    82: Role.PHOTOGRAPHY_DIRECTOR, # "Photography"
    83: Role.PRODUCTION_MANAGER,   # "Assistant Production Manager Assistance"
    84: Role.PRODUCTION_MANAGER,   # "Design Manager"
    85: Role.MUSIC,                # "Music Producer"
    86: Role.CGI_DIRECTOR,         # "3DCG Director"
    87: Role.PRODUCER,             # "Animation Producer"
    88: Role.ANIMATION_DIRECTOR,   # "Special Effects Animation Direction"
    89: Role.EPISODE_DIRECTOR,     # "Chief Episode Direction"
    90: Role.ANIMATION_DIRECTOR,   # "Assistant Animation Direction"
    91: Role.EPISODE_DIRECTOR,     # "Assistant Episode Direction"
    92: Role.KEY_ANIMATOR,         # "Main Animator"
    93: Role.FINISHING,            # "Coloring"
    94: Role.FINISHING,            # "Color Check"
    95: Role.FINISHING,            # "Color Inspection"
    96: Role.BACKGROUND_ART,       # "Art Board"
    97: Role.BACKGROUND_ART,       # "Art"
    98: Role.BACKGROUND_ART,       # "Image Board"
    99: Role.SETTINGS,             # "2D WORKS"
    100: Role.CGI_DIRECTOR,        # "3D WORKS"
    101: Role.DIRECTOR,            # "Technical Director"
    102: Role.DIRECTOR,            # "Special Effects Director"
    103: Role.FINISHING,           # "Color Script"
    104: Role.EPISODE_DIRECTOR,    # "Storyboard Cooperation"
    105: Role.EPISODE_DIRECTOR,    # "Storyboard Copying"
    106: Role.CHARACTER_DESIGNER,  # "Sub-Character Design"
    107: Role.CHARACTER_DESIGNER,  # "Guest Character Design"
    108: Role.LAYOUT,              # "Layout Supervision"
    109: Role.ANIMATION_DIRECTOR,  # "Layout Animation Director"
    110: Role.ANIMATION_DIRECTOR,  # "Chief Animation Director Assistance"
    111: Role.ANIMATION_DIRECTOR,  # "Prop Animation Director"
    112: Role.CHARACTER_DESIGNER,  # "Concept Design"
    113: Role.CHARACTER_DESIGNER,  # "Costume Design"
    114: Role.SPECIAL,             # "Title Design"
    115: Role.SETTINGS,            # "Setting Cooperation"
    116: Role.SOUND_DIRECTOR,      # "Music Director"
    117: Role.MUSIC,               # "Music Selection"
    118: Role.MUSIC,               # "Inserted Song Lyrics"
    119: Role.MUSIC,               # "Inserted Song Composition"
    120: Role.MUSIC,               # "Inserted Song Arrangement"
    121: Role.PRODUCER,            # "Creative Producer"
    122: Role.PRODUCER,            # "Associate Producer"
    123: Role.PRODUCER,            # "Chief Production Supervisor"
    124: Role.PRODUCER,            # "Line Producer"
    125: Role.SCREENPLAY,          # "Literary Producer"
    127: Role.PRODUCER,            # "Planning Cooperation"
    128: Role.EPISODE_DIRECTOR,    # "OP/ED Direction"
    129: Role.EPISODE_DIRECTOR,    # "Bank Storyboard Direction"
    130: Role.EPISODE_DIRECTOR,    # "Live Storyboard Direction"
    131: Role.EPISODE_DIRECTOR,    # "Meta-story Storyboard Direction"
    132: Role.CHARACTER_DESIGNER,  # "Meta-story Character Design"
    133: Role.CGI_DIRECTOR,        # "Visual Director"
    134: Role.DIRECTOR,            # "Creative Supervisor/Director"
    135: Role.PHOTOGRAPHY_DIRECTOR, # "Tokusatsu Effects"
    136: Role.PHOTOGRAPHY_DIRECTOR, # "Visual Effects"
    137: Role.DIRECTOR,            # "Action Director"
    138: Role.KEY_ANIMATOR,        # "Eyecatch Art"
    139: Role.SPECIAL,             # "Illustration"
    140: Role.ANIMATION_DIRECTOR,  # "Character Animation Director"
    141: Role.ANIMATION_DIRECTOR,  # "Animation Supervisor"
    142: Role.CHARACTER_DESIGNER,  # "Mechanical Design Concept"
    143: Role.CHARACTER_DESIGNER,  # "Concept Art"
    144: Role.SETTINGS,            # "Visual Concept"
    145: Role.SETTINGS,            # "Scene Design"
    146: Role.CHARACTER_DESIGNER,  # "Monster Design"
    147: Role.ORIGINAL_CREATOR,    # "Story Concept"
    148: Role.SCREENPLAY,          # "Scenario Coordinator"
    149: Role.SCREENPLAY,          # "Script Cooperation"
    150: Role.SCREENPLAY,          # "Associate Series Composition"
    151: Role.SCREENPLAY,          # "Series Composition Cooperation"
    152: Role.SOUND_DIRECTOR,      # "Recording Studio"
    153: Role.SOUND_DIRECTOR,      # "Sound Mixing"
    154: Role.SOUND_DIRECTOR,      # "Sound Production Coordinator"
    155: Role.EDITING,             # "Online Editing"
    156: Role.EDITING,             # "Offline Editing"
    157: Role.CGI_DIRECTOR,        # "3D Animator"
    158: Role.PRODUCER,            # "CG Producer"
    159: Role.PRODUCER,            # "Publicity Producer"
    160: Role.PRODUCER,            # "Art Producer"
    161: Role.MUSIC,               # "Sound Producer"
    162: Role.PRODUCTION_MANAGER,  # "CG Production Coordinator"
    163: Role.PRODUCTION_MANAGER,  # "Art Production Coordinator"
    164: Role.BACKGROUND_ART,      # "Assistant Art Director"
    165: Role.FINISHING,           # "Assistant Color Designer"
    166: Role.PHOTOGRAPHY_DIRECTOR, # "Assistant Director of Photography"
    167: Role.PRODUCTION_MANAGER,  # "Assistant Production Desk"
    168: Role.SETTINGS,            # "Assistant Design Manager"
}


@lru_cache(maxsize=1)
def _build_code_map() -> dict[int, str]:
    """Load staffs.json and build int code → Role.value mapping.

    Priority: _CODE_OVERRIDES (hand-curated) wins over JSON-derived lookup.
    Falls back to ROLE_MAP via English label for any code not in overrides.
    """
    try:
        raw_data: dict[str, dict] = json.loads(_STAFFS_JSON.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("bangumi_staffs_json_load_failed", error=str(exc))
        raw_data = {}

    code_map: dict[int, str] = {}

    for code_str, entry in raw_data.items():
        code = int(code_str)
        if code in _CODE_OVERRIDES:
            code_map[code] = _CODE_OVERRIDES[code].value
            continue
        # Try English label via ROLE_MAP as a secondary lookup path.
        en_label = entry.get("en", "").strip().lower()
        role = ROLE_MAP.get(en_label)
        if role is not None:
            code_map[code] = role.value
        else:
            code_map[code] = Role.OTHER.value

    return code_map


def _resolve_code(raw: str) -> str:
    """Convert a Bangumi staff code string to a normalized Role.value."""
    try:
        code = int(raw)
    except (ValueError, TypeError):
        return Role.OTHER.value
    return _build_code_map().get(code, Role.OTHER.value)


@register("bangumi")
def map_bangumi_role(raw: str) -> str:
    """Map a Bangumi staff code (as string) to a normalized Role.value."""
    return _resolve_code(raw)
