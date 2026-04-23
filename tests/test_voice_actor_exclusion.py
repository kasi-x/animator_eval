"""声優・原作者が制作スタッフスコアリングから除外されることを検証."""

from src.analysis.graph import (
    create_person_anime_network,
    create_person_collaboration_network,
    determine_primary_role_for_each_person,
)
from src.analysis.network.trust import compute_trust_scores
from src.models import BronzeAnime as Anime, Credit, Person, Role
from src.pipeline_phases.data_loading import (
    _filter_non_production_persons,
    _is_garbage_person,
)
from src.utils.role_groups import NON_PRODUCTION_ROLES, is_production_credit


def _make_credits_with_voice_actors():
    """テスト用データ: 声優・制作スタッフ混在."""
    persons = [
        Person(id="dir1", name_en="Director A"),
        Person(id="anim1", name_en="Animator B"),
        Person(id="va1", name_en="Voice Actor C"),  # 声優のみ
        Person(id="va2", name_en="Voice Actor D"),  # 声優のみ
        Person(id="dual1", name_en="Dual Role E"),  # 兼任（声優 + 原画）
        Person(id="song1", name_en="Singer F"),  # 主題歌のみ
    ]
    anime_list = [
        Anime(id="a1", title_en="Anime 1", year=2023),
        Anime(id="a2", title_en="Anime 2", year=2024),
    ]
    credits = [
        # 制作スタッフ
        Credit(person_id="dir1", anime_id="a1", role=Role.DIRECTOR),
        Credit(person_id="dir1", anime_id="a2", role=Role.DIRECTOR),
        Credit(person_id="anim1", anime_id="a1", role=Role.KEY_ANIMATOR),
        Credit(person_id="anim1", anime_id="a2", role=Role.KEY_ANIMATOR),
        # 声優のみ
        Credit(person_id="va1", anime_id="a1", role=Role.VOICE_ACTOR),
        Credit(person_id="va1", anime_id="a2", role=Role.VOICE_ACTOR),
        Credit(person_id="va2", anime_id="a1", role=Role.VOICE_ACTOR),
        # 兼任者: 声優 + 制作
        Credit(person_id="dual1", anime_id="a1", role=Role.VOICE_ACTOR),
        Credit(person_id="dual1", anime_id="a1", role=Role.KEY_ANIMATOR),
        Credit(person_id="dual1", anime_id="a2", role=Role.VOICE_ACTOR),
        # 主題歌
        Credit(person_id="song1", anime_id="a1", role=Role.MUSIC),
    ]
    return persons, anime_list, credits


class TestNonProductionRolesConstant:
    def test_contains_voice_actor(self):
        assert Role.VOICE_ACTOR in NON_PRODUCTION_ROLES

    def test_contains_theme_song(self):
        assert Role.MUSIC in NON_PRODUCTION_ROLES

    def test_contains_adr(self):
        assert Role.VOICE_ACTOR in NON_PRODUCTION_ROLES

    def test_contains_original_creator(self):
        assert Role.ORIGINAL_CREATOR in NON_PRODUCTION_ROLES

    def test_does_not_contain_production_roles(self):
        production_roles = [
            Role.DIRECTOR,
            Role.KEY_ANIMATOR,
            Role.ANIMATION_DIRECTOR,
            Role.CHARACTER_DESIGNER,
        ]
        for role in production_roles:
            assert role not in NON_PRODUCTION_ROLES

    def test_is_production_credit(self):
        prod = Credit(person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR)
        va = Credit(person_id="p2", anime_id="a1", role=Role.VOICE_ACTOR)
        oc = Credit(person_id="p3", anime_id="a1", role=Role.ORIGINAL_CREATOR)
        assert is_production_credit(prod) is True
        assert is_production_credit(va) is False
        assert is_production_credit(oc) is False


class TestVoiceActorExcludedFromBipartiteGraph:
    def test_voice_actor_no_edges(self):
        """Voice actors should not have edges in person-anime bipartite graph."""
        persons, anime_list, credits = _make_credits_with_voice_actors()
        g = create_person_anime_network(persons, anime_list, credits)

        # va1 node exists (added from persons list) but has no edges
        assert g.has_node("va1")
        assert g.out_degree("va1") == 0
        assert g.in_degree("va1") == 0

    def test_voice_actor_only_no_edges(self):
        """Voice-actor-only person should have zero edges."""
        persons, anime_list, credits = _make_credits_with_voice_actors()
        g = create_person_anime_network(persons, anime_list, credits)
        assert g.out_degree("va2") == 0
        assert g.out_degree("song1") == 0

    def test_production_staff_has_edges(self):
        """Production staff should still have edges."""
        persons, anime_list, credits = _make_credits_with_voice_actors()
        g = create_person_anime_network(persons, anime_list, credits)
        assert g.has_edge("dir1", "a1")
        assert g.has_edge("anim1", "a1")

    def test_dual_role_keeps_production_edges(self):
        """Persons with both VA and production credits keep production edges."""
        persons, anime_list, credits = _make_credits_with_voice_actors()
        g = create_person_anime_network(persons, anime_list, credits)
        # dual1 has KEY_ANIMATOR on a1, so should have edge to a1
        assert g.has_edge("dual1", "a1")
        # dual1 has only VOICE_ACTOR on a2, so no edge to a2
        assert not g.has_edge("dual1", "a2")


class TestVoiceActorExcludedFromCollaborationGraph:
    def test_voice_actor_not_connected(self):
        """Voice actors should not appear in collaboration edges."""
        persons, _, credits = _make_credits_with_voice_actors()
        g = create_person_collaboration_network(persons, credits)

        # va1, va2, song1 should have no edges
        assert g.degree("va1") == 0
        assert g.degree("va2") == 0
        assert g.degree("song1") == 0

    def test_production_staff_connected(self):
        """Production staff should still be connected."""
        persons, _, credits = _make_credits_with_voice_actors()
        g = create_person_collaboration_network(persons, credits)
        assert g.has_edge("dir1", "anim1")

    def test_dual_role_connected_via_production(self):
        """Dual-role person connected only through production credits."""
        persons, _, credits = _make_credits_with_voice_actors()
        g = create_person_collaboration_network(persons, credits)
        # dual1 has KEY_ANIMATOR on a1, should connect with dir1 and anim1
        assert g.has_edge("dual1", "dir1") or g.has_edge("dir1", "dual1")


class TestVoiceActorExcludedFromTrust:
    def test_voice_actor_no_trust_score(self):
        """Voice actors should not receive trust scores."""
        _, anime_list, credits = _make_credits_with_voice_actors()
        anime_map = {a.id: a for a in anime_list}
        scores = compute_trust_scores(credits, anime_map, current_year=2025)

        # va1 (voice-actor-only) should not appear in trust scores
        # (no production credits → no collaborations with directors)
        assert "va1" not in scores or scores.get("va1", 0) == 0
        assert "va2" not in scores or scores.get("va2", 0) == 0

    def test_production_staff_has_trust(self):
        """Production staff should have trust scores."""
        _, anime_list, credits = _make_credits_with_voice_actors()
        anime_map = {a.id: a for a in anime_list}
        scores = compute_trust_scores(credits, anime_map, current_year=2025)
        assert "anim1" in scores
        assert scores["anim1"] > 0

    def test_dual_role_trust_from_production_only(self):
        """Dual-role person gets trust from production credits only."""
        _, anime_list, credits = _make_credits_with_voice_actors()
        anime_map = {a.id: a for a in anime_list}
        scores = compute_trust_scores(credits, anime_map, current_year=2025)
        # dual1 has KEY_ANIMATOR on a1 with dir1, so should have some trust
        assert "dual1" in scores
        assert scores["dual1"] > 0


class TestVoiceActorExcludedFromRoleClassification:
    def test_voice_actor_only_not_classified(self):
        """Voice-actor-only persons should not appear in role classification."""
        _, _, credits = _make_credits_with_voice_actors()
        result = determine_primary_role_for_each_person(credits)
        assert "va1" not in result
        assert "va2" not in result
        assert "song1" not in result

    def test_dual_role_classified_by_production(self):
        """Dual-role person classified by production credits only."""
        _, _, credits = _make_credits_with_voice_actors()
        result = determine_primary_role_for_each_person(credits)
        assert "dual1" in result
        # dual1 has KEY_ANIMATOR → should be classified as animator
        assert result["dual1"]["primary_category"] == "animator"
        # voice_actor should not be in role_counts
        assert "voice_actor" not in result["dual1"]["role_counts"]


class TestOriginalCreatorExcluded:
    def test_original_creator_no_edges_in_bipartite(self):
        """Original creators should not have edges in bipartite graph."""
        persons = [
            Person(id="dir1", name_en="Director"),
            Person(id="oc1", name_ja="高橋 留美子"),
        ]
        anime_list = [Anime(id="a1", title_en="Anime 1", year=2023)]
        credits = [
            Credit(person_id="dir1", anime_id="a1", role=Role.DIRECTOR),
            Credit(person_id="oc1", anime_id="a1", role=Role.ORIGINAL_CREATOR),
        ]
        g = create_person_anime_network(persons, anime_list, credits)
        assert g.out_degree("oc1") == 0

    def test_original_creator_no_trust(self):
        """Original creators should not receive trust scores."""
        credits = [
            Credit(person_id="dir1", anime_id="a1", role=Role.DIRECTOR),
            Credit(person_id="oc1", anime_id="a1", role=Role.ORIGINAL_CREATOR),
        ]
        anime_map = {"a1": Anime(id="a1", year=2023)}
        scores = compute_trust_scores(credits, anime_map, current_year=2025)
        assert "oc1" not in scores or scores.get("oc1", 0) == 0

    def test_original_creator_not_classified(self):
        """Original creators should not appear in role classification."""
        credits = [
            Credit(person_id="oc1", anime_id="a1", role=Role.ORIGINAL_CREATOR),
        ]
        result = determine_primary_role_for_each_person(credits)
        assert "oc1" not in result


class TestGarbagePersonFiltering:
    def test_garbage_names_detected(self):
        """Known garbage names should be detected."""
        assert _is_garbage_person(Person(id="g1", name_ja="アニメ"))
        assert _is_garbage_person(Person(id="g2", name_ja="ほか"))
        assert _is_garbage_person(Person(id="g3", name_ja="その他"))
        assert _is_garbage_person(Person(id="g4", name_ja="スタッフ"))

    def test_valid_names_not_garbage(self):
        """Valid person names should not be flagged as garbage."""
        assert not _is_garbage_person(Person(id="p1", name_ja="宮崎 駿"))
        assert not _is_garbage_person(Person(id="p2", name_en="Hayao Miyazaki"))
        assert not _is_garbage_person(Person(id="p3", name_ja="高橋 留美子"))

    def test_nameless_person_is_garbage(self):
        """Person with no name is garbage."""
        assert _is_garbage_person(Person(id="p1"))


class TestNonProductionPersonFiltering:
    """_filter_non_production_persons() が正しく非制作スタッフを除外することを検証."""

    def test_voice_actor_only_excluded(self):
        """声優のみのクレジットを持つ人物は除外される."""
        persons = [
            Person(id="va1", name_en="Voice Actor"),
            Person(id="anim1", name_en="Animator"),
        ]
        credits = [
            Credit(person_id="va1", anime_id="a1", role=Role.VOICE_ACTOR),
            Credit(person_id="va1", anime_id="a2", role=Role.VOICE_ACTOR),
            Credit(person_id="anim1", anime_id="a1", role=Role.KEY_ANIMATOR),
        ]
        filtered, removed = _filter_non_production_persons(persons, credits)
        assert "va1" in removed
        assert len(filtered) == 1
        assert filtered[0].id == "anim1"

    def test_dual_role_preserved(self):
        """声優+制作の兼任者は保持される."""
        persons = [Person(id="dual1", name_en="Dual Role")]
        credits = [
            Credit(person_id="dual1", anime_id="a1", role=Role.VOICE_ACTOR),
            Credit(person_id="dual1", anime_id="a1", role=Role.KEY_ANIMATOR),
        ]
        filtered, removed = _filter_non_production_persons(persons, credits)
        assert "dual1" not in removed
        assert len(filtered) == 1

    def test_original_creator_only_excluded(self):
        """原作者のみのクレジットを持つ人物は除外される."""
        persons = [Person(id="oc1", name_ja="高橋 留美子")]
        credits = [
            Credit(person_id="oc1", anime_id="a1", role=Role.ORIGINAL_CREATOR),
            Credit(person_id="oc1", anime_id="a2", role=Role.ORIGINAL_CREATOR),
        ]
        filtered, removed = _filter_non_production_persons(persons, credits)
        assert "oc1" in removed
        assert len(filtered) == 0

    def test_animator_and_original_creator_preserved(self):
        """アニメーター兼原作者は保持される."""
        persons = [Person(id="dual_oc1", name_ja="大友 克洋")]
        credits = [
            Credit(person_id="dual_oc1", anime_id="a1", role=Role.ORIGINAL_CREATOR),
            Credit(person_id="dual_oc1", anime_id="a1", role=Role.KEY_ANIMATOR),
            Credit(person_id="dual_oc1", anime_id="a1", role=Role.DIRECTOR),
        ]
        filtered, removed = _filter_non_production_persons(persons, credits)
        assert "dual_oc1" not in removed
        assert len(filtered) == 1

    def test_production_staff_unaffected(self):
        """制作スタッフは影響を受けない."""
        persons = [
            Person(id="dir1", name_en="Director"),
            Person(id="anim1", name_en="Animator"),
        ]
        credits = [
            Credit(person_id="dir1", anime_id="a1", role=Role.DIRECTOR),
            Credit(person_id="anim1", anime_id="a1", role=Role.KEY_ANIMATOR),
        ]
        filtered, removed = _filter_non_production_persons(persons, credits)
        assert len(removed) == 0
        assert len(filtered) == 2
