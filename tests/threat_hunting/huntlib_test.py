from databricks.sirens.threathunting.huntlib import ThreatHunt
from databricks.sirens.threathunting._entities import ActiveHunt, HuntBackend


def test_threathunt(mocker):
    mocker.patch("databricks.sirens.threathunting.huntlib.ThreatHunt._initialize_backend")
    active_hunt = ThreatHunt(hunt_name="wmic_usage")
    assert isinstance(active_hunt, ThreatHunt)

def test_threathunt_start(mocker):
    mocker.patch("databricks.sirens.threathunting.huntlib.ThreatHunt._initialize_backend")
    mocker.patch("databricks.sirens.threathunting.huntlib.ThreatHunt._update_index_table")
    active_hunt = ThreatHunt(hunt_name="wmic_usage")
    active_hunt_obj = active_hunt.start()
    assert isinstance(active_hunt_obj, ActiveHunt)

def test_threathunt_as_context_manager(mocker):
    mocker.patch("databricks.sirens.threathunting.huntlib.ThreatHunt._initialize_backend")
    mocker.patch("databricks.sirens.threathunting.huntlib.ThreatHunt._start")
    mocker.patch("databricks.sirens.threathunting.huntlib.ThreatHunt._cell_ended")
    with ThreatHunt(hunt_name="wmic") as wmic_hunt:
        assert wmic_hunt

def test_get_active_object_hunt_pos(mocker):
    mocker.patch("databricks.sirens.threathunting.huntlib.ThreatHunt._initialize_backend")
    ThreatHunt(hunt_name="wmic")
    result = ThreatHunt._get_active_obj(hunt_name="wmic", type="hunt")
    assert isinstance(result, ActiveHunt)
    assert result.hunt_name == "wmic"

def test_get_active_object_hunt_neg(mocker):
    mocker.patch("databricks.sirens.threathunting.huntlib.ThreatHunt._initialize_backend")
    ThreatHunt(hunt_name="wmic")
    result = ThreatHunt._get_active_obj(hunt_name="non_existant_hunt", type="hunt")
    assert result is None

def test_get_active_object_backend_pos(mocker):
    mocker.patch("databricks.sirens.threathunting.huntlib.ThreatHunt._initialize_backend")
    ThreatHunt(hunt_name="wmic")
    result = ThreatHunt._get_active_obj(hunt_name="wmic", type="backend")
    assert isinstance(result, HuntBackend)
    assert result.hunt_name == "wmic"

def test_get_active_object_backend_neg(mocker):
    mocker.patch("databricks.sirens.threathunting.huntlib.ThreatHunt._initialize_backend")
    ThreatHunt(hunt_name="wmic")
    result = ThreatHunt._get_active_obj(hunt_name="non_existant_hunt", type="backend")
    assert result is None

def test__update_active_hunt_state(mocker):
    mocker.patch("databricks.sirens.threathunting.huntlib.ThreatHunt._initialize_backend")
    mocker.patch("databricks.sirens.threathunting.huntlib.ThreatHunt._update_index_table")
    active_hunt = ThreatHunt(hunt_name="wmic_usage")
    active_hunt_obj = active_hunt.start()
    result = ThreatHunt._update_active_hunt(hunt=active_hunt_obj, state="paused")
    assert isinstance(result, ActiveHunt)
    assert result.state == "paused"


def test__update_active_hunt_start_time(mocker):
    mocker.patch("databricks.sirens.threathunting.huntlib.ThreatHunt._initialize_backend")
    mocker.patch("databricks.sirens.threathunting.huntlib.ThreatHunt._update_index_table")
    active_hunt = ThreatHunt(hunt_name="wmic_usage")
    active_hunt_obj = active_hunt.start()
    result = ThreatHunt._update_active_hunt(hunt=active_hunt_obj, start_time="11:00.00")
    assert isinstance(result, ActiveHunt)
    assert result.start_time == "11:00.00"

def test__update_active_hunt_end_time(mocker):
    mocker.patch("databricks.sirens.threathunting.huntlib.ThreatHunt._initialize_backend")
    mocker.patch("databricks.sirens.threathunting.huntlib.ThreatHunt._update_index_table")
    active_hunt = ThreatHunt(hunt_name="wmic_usage")
    active_hunt_obj = active_hunt.start()
    result = ThreatHunt._update_active_hunt(hunt=active_hunt_obj, end_time="11:00.00")
    assert isinstance(result, ActiveHunt)
    assert result.end_time == "11:00.00"

def test__update_active_hunt_status(mocker):
    mocker.patch("databricks.sirens.threathunting.huntlib.ThreatHunt._initialize_backend")
    mocker.patch("databricks.sirens.threathunting.huntlib.ThreatHunt._update_index_table")
    active_hunt = ThreatHunt(hunt_name="wmic_usage")
    active_hunt_obj = active_hunt.start()
    result = ThreatHunt._update_active_hunt(hunt=active_hunt_obj, status="finished")
    assert isinstance(result, ActiveHunt)
    assert result.status == "finished"

def test__update_active_hunt_result(mocker):
    mocker.patch("databricks.sirens.threathunting.huntlib.ThreatHunt._initialize_backend")
    mocker.patch("databricks.sirens.threathunting.huntlib.ThreatHunt._update_index_table")
    active_hunt = ThreatHunt(hunt_name="wmic_usage")
    active_hunt_obj = active_hunt.start()
    result = ThreatHunt._update_active_hunt(hunt=active_hunt_obj, result="success")
    assert isinstance(result, ActiveHunt)
    assert result.result == "success"

def test__get_active_hunt_index_pos(mocker):
    mocker.patch("databricks.sirens.threathunting.huntlib.ThreatHunt._initialize_backend")
    mocker.patch("databricks.sirens.threathunting.huntlib.ThreatHunt._update_index_table")
    active_hunt = ThreatHunt(hunt_name="wmic_usage")
    active_hunt.start()
    result = ThreatHunt._get_active_hunt_index("wmic_usage")
    assert result == 0

def test__get_active_hunt_index_neg(mocker):
    mocker.patch("databricks.sirens.threathunting.huntlib.ThreatHunt._initialize_backend")
    mocker.patch("databricks.sirens.threathunting.huntlib.ThreatHunt._update_index_table")
    active_hunt = ThreatHunt(hunt_name="wmic_usage")
    active_hunt.start()
    result = ThreatHunt._get_active_hunt_index("non_existant_hunt")
    assert result is None

def test__cell_ended(mocker):
    mocker.patch("databricks.sirens.threathunting.huntlib.ThreatHunt._initialize_backend")
    mocker.patch("databricks.sirens.threathunting.huntlib.ThreatHunt._update_index_table")
    active_hunt = ThreatHunt(hunt_name="wmic_usage")
    active_hunt.start()
    result = active_hunt._cell_ended()
    assert isinstance(result, ActiveHunt)


