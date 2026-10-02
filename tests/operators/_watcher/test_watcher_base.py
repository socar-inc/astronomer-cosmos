from unittest.mock import MagicMock, Mock, patch

import pytest
from airflow.exceptions import AirflowException, AirflowSkipException

from cosmos.operators._watcher.aggregation import get_tests_status_xcom_key
from cosmos.operators._watcher.base import BaseConsumerSensor, _process_dbt_log_event
from cosmos.operators._watcher.state import (
    get_compiled_sql_xcom_key,
    get_dbt_event_xcom_key,
    get_status_xcom_key,
    safe_xcom_push,
)
from cosmos.operators._watcher.xcom import (
    _backup_xcom_to_variable,
    _init_xcom_backup,
    _restore_xcom_from_variable,
)
from cosmos.operators.local import DbtRunLocalOperator


class TestBaseConsumerSensor:

    def test_extra_context_is_stored_on_instance(self):
        """Consumer sensor stores extra_context so it is available at runtime."""

        class SubclassBaseConsumerSensor(BaseConsumerSensor, DbtRunLocalOperator):
            something_to_be_implemented = True

        extra_context = {"dbt_node_config": {"unique_id": "model.jaffle_shop.stg_orders"}, "run_id": "run_123"}
        sensor = SubclassBaseConsumerSensor(
            task_id="test_sensor",
            producer_task_id="dbt_run_local",
            profile_config=None,
            project_dir="/tmp/sample_project",
            extra_context=extra_context,
        )
        assert sensor.extra_context == extra_context
        assert sensor.model_unique_id == "model.jaffle_shop.stg_orders"

    def test_extra_context_defaults_to_empty_dict_when_not_passed(self):
        """When extra_context is not in kwargs, sensor.extra_context is {}."""

        class SubclassBaseConsumerSensor(BaseConsumerSensor, DbtRunLocalOperator):
            something_to_be_implemented = True

        sensor = SubclassBaseConsumerSensor(
            task_id="test_sensor",
            producer_task_id="dbt_run_local",
            profile_config=None,
            project_dir="/tmp/sample_project",
        )
        assert sensor.extra_context == {}

    @pytest.mark.parametrize(
        "event_name,should_push",
        [
            (None, False),
            ("LogStartLine", False),
            ("NodeFinished", True),
            ("NodeStart", True),
        ],
    )
    def test_process_dbt_log_event_only_pushes_when_event_in_allowlist(self, event_name, should_push):
        """Only dbt events whose names are in _DBT_EVENT_ALLOWLIST are pushed to XCom."""
        task_instance = Mock()

        dbt_log = {
            "data": {
                "node_info": {
                    "unique_id": "model.test.my_model",
                    "node_status": "success",
                    "node_started_at": "2024-01-01T00:00:00",
                    "node_finished_at": "2024-01-01T00:01:00",
                },
                "msg": "model finished",
            },
            "info": {"name": event_name} if event_name is not None else {},
        }

        with patch("cosmos.operators._watcher.base.safe_xcom_push") as mock_push:
            _process_dbt_log_event(task_instance, dbt_log)

            if should_push:
                mock_push.assert_called_once()
                call_kwargs = mock_push.call_args.kwargs
                assert call_kwargs["key"] == "model__test__my_model_dbt_event"
                assert call_kwargs["value"]["status"] == "success"
                assert call_kwargs["value"]["msg"] == "model finished"
            else:
                mock_push.assert_not_called()

    def test_process_dbt_log_event_skips_when_no_unique_id(self):
        """Events with no node_info.unique_id are not pushed."""
        task_instance = Mock()

        dbt_log = {
            "data": {"node_info": {}, "msg": "some log"},
            "info": {"name": "NodeFinished"},
        }

        with patch("cosmos.operators._watcher.base.safe_xcom_push") as mock_push:
            _process_dbt_log_event(task_instance, dbt_log)
            mock_push.assert_not_called()

    def test_process_dbt_log_event_captures_error_from_run_result_message(self):
        """dbt-runner mode: the error text lives in NodeFinished.data.run_result.message (data.msg/info.msg
        are empty there). It must reach the consumer's _dbt_event."""
        task_instance = Mock()
        dbt_log = {
            "data": {
                "node_info": {
                    "unique_id": "model.test.bad_model",
                    "node_status": "error",
                    "node_started_at": "2024-01-01T00:00:00",
                    "node_finished_at": "2024-01-01T00:01:00",
                },
                "run_result": {"status": "error", "message": "Runtime Error ... Catalog Error: Table does not exist!"},
                "msg": "",
            },
            "info": {"name": "NodeFinished", "msg": ""},
        }

        with patch("cosmos.operators._watcher.base.safe_xcom_push") as mock_push:
            _process_dbt_log_event(task_instance, dbt_log)

        value = mock_push.call_args.kwargs["value"]
        assert value["status"] == "error"
        assert value["msg"] == "Runtime Error ... Catalog Error: Table does not exist!"

    def test_execute_complete_raises_airflow_skip_exception_when_status_is_skipped(self):
        """execute_complete raises AirflowSkipException when the trigger sends status='skipped'."""

        class SubclassBaseConsumerSensor(BaseConsumerSensor, DbtRunLocalOperator):
            something_to_be_implemented = True

        sensor = SubclassBaseConsumerSensor(
            task_id="test_sensor",
            producer_task_id="dbt_run_local",
            profile_config=None,
            project_dir="/tmp/sample_project",
            extra_context={"dbt_node_config": {"unique_id": "model.pkg.my_model"}},
        )
        context = {"ti": Mock()}
        with pytest.raises(AirflowSkipException, match="was skipped by the dbt command"):
            sensor.execute_complete(context, {"status": "skipped", "reason": "source_not_fresh"})

    def test_execute_complete_logs_dbt_event_on_success(self):
        """Deferrable path: execute_complete logs the per-node dbt event once on success, not only on failure.

        Regression for #2456 - removing the per-poll trigger log must not drop the single terminal log line."""

        class SubclassBaseConsumerSensor(BaseConsumerSensor, DbtRunLocalOperator):
            something_to_be_implemented = True

        sensor = SubclassBaseConsumerSensor(
            task_id="test_sensor",
            producer_task_id="dbt_run_local",
            profile_config=None,
            project_dir="/tmp/sample_project",
            extra_context={"dbt_node_config": {"unique_id": "model.pkg.my_model"}},
        )
        context = {"ti": Mock()}
        with (
            patch("cosmos.operators._watcher.base.get_xcom_val", return_value={"status": "success", "msg": "ok"}),
            patch("cosmos.operators._watcher.base._log_dbt_event") as mock_log,
        ):
            sensor.execute_complete(context, {"status": "success"})
        mock_log.assert_called_once()

    def test_poke_raises_airflow_skip_exception_when_status_is_skipped(self):
        """poke raises AirflowSkipException when node status is 'skipped'."""

        class SubclassBaseConsumerSensor(BaseConsumerSensor, DbtRunLocalOperator):
            something_to_be_implemented = True

        sensor = SubclassBaseConsumerSensor(
            task_id="test_sensor",
            producer_task_id="dbt_run_local",
            profile_config=None,
            project_dir="/tmp/sample_project",
            extra_context={"dbt_node_config": {"unique_id": "model.pkg.my_model"}},
        )
        mock_ti = Mock()
        mock_ti.try_number = 1
        context = {"ti": mock_ti, "run_id": "run_123"}

        with (
            patch.object(sensor, "_get_producer_task_status", return_value="running"),
            patch.object(sensor, "_get_node_status", return_value="skipped"),
            patch.object(sensor, "_log_startup_events"),
        ):
            with pytest.raises(AirflowSkipException, match="was skipped by the dbt command"):
                sensor.poke(context)

    def test_poke_logs_dbt_event_only_at_terminal(self):
        """poke must not log the per-node dbt event while the node is still running (status None); it logs
        once the node is terminal. Regression for #2456 on the non-deferrable path."""

        class SubclassBaseConsumerSensor(BaseConsumerSensor, DbtRunLocalOperator):
            something_to_be_implemented = True

        sensor = SubclassBaseConsumerSensor(
            task_id="test_sensor",
            producer_task_id="dbt_run_local",
            profile_config=None,
            project_dir="/tmp/sample_project",
            extra_context={"dbt_node_config": {"unique_id": "model.pkg.my_model"}},
        )
        mock_ti = Mock()
        mock_ti.try_number = 1
        context = {"ti": mock_ti, "run_id": "run_123"}

        with (
            patch.object(sensor, "_get_producer_task_status", return_value="running"),
            patch.object(sensor, "_log_startup_events"),
            patch.object(sensor, "_cache_compiled_sql"),
            patch("cosmos.operators._watcher.base.get_xcom_val", return_value={"status": "success", "msg": "ok"}),
            patch("cosmos.operators._watcher.base._log_dbt_event") as mock_log,
        ):
            # Node still running: must not log (it would duplicate on each poke).
            with (
                patch.object(sensor, "_get_node_status", return_value=None),
                patch.object(sensor, "_handle_no_dbt_node_status", return_value=False),
            ):
                assert sensor.poke(context) is False
            mock_log.assert_not_called()

            # Node terminal: logs exactly once.
            with patch.object(sensor, "_get_node_status", return_value="success"):
                assert sensor.poke(context) is True
            mock_log.assert_called_once()


class TestHandleNoDbtNodeStatus:
    """Tests for BaseConsumerSensor._handle_no_dbt_node_status."""

    def _make_sensor(self):
        class SubclassBaseConsumerSensor(BaseConsumerSensor, DbtRunLocalOperator):
            something_to_be_implemented = True

        extra_context = {"dbt_node_config": {"unique_id": "model.jaffle_shop.stg_orders"}}
        sensor = SubclassBaseConsumerSensor(
            task_id="test_sensor",
            producer_task_id="dbt_run_local",
            profile_config=None,
            project_dir="/tmp/sample_project",
            extra_context=extra_context,
        )
        sensor._get_producer_task_status = MagicMock(return_value=None)
        return sensor

    @patch("cosmos.operators._watcher.base.BaseConsumerSensor._fallback_to_non_watcher_run", return_value=True)
    def test_producer_failed_with_no_poke_retries_falls_back(self, mock_fallback):
        sensor = self._make_sensor()
        sensor.poke_retry_number = 0
        context = MagicMock()

        result = sensor._handle_no_dbt_node_status("failed", try_number=1, context=context)

        assert result is True
        mock_fallback.assert_called_once_with(1, context)

    def test_producer_failed_with_poke_retries_raises(self):
        sensor = self._make_sensor()
        sensor.poke_retry_number = 1

        with pytest.raises(AirflowException, match="dbt build command failed"):
            sensor._handle_no_dbt_node_status("failed", try_number=1, context=MagicMock())

    @patch("cosmos.operators._watcher.base.BaseConsumerSensor._fallback_to_non_watcher_run", return_value=True)
    def test_producer_skipped_falls_back(self, mock_fallback):
        sensor = self._make_sensor()
        context = MagicMock()

        result = sensor._handle_no_dbt_node_status("skipped", try_number=1, context=context)

        assert result is True
        mock_fallback.assert_called_once_with(1, context)

    def test_no_status_increments_poke_retry(self):
        sensor = self._make_sensor()
        sensor.poke_retry_number = 0

        result = sensor._handle_no_dbt_node_status(None, try_number=1, context=MagicMock())

        assert result is False
        assert sensor.poke_retry_number == 1


class TestPreserveReportedSuccess:
    """Exercise real status decoding and retry dispatch, without running dbt."""

    @pytest.fixture(autouse=True)
    def disable_fallback(self, monkeypatch):
        monkeypatch.setattr("cosmos.operators._watcher.base.settings.enable_watcher_fallback", False)

    def _make_sensor(self, is_test=False, unique_id="model.pkg.my_model"):
        class Sensor(BaseConsumerSensor, DbtRunLocalOperator):
            @property
            def is_test_sensor(self):
                return is_test

        sensor = Sensor(
            task_id="test_sensor",
            producer_task_id="producer",
            profile_config=None,
            project_dir="/tmp/sample_project",
            extra_context={"dbt_node_config": {"unique_id": unique_id}},
        )
        sensor._get_producer_task_status = Mock(return_value="skipped")
        sensor._override_rtif = Mock()
        sensor._fallback_to_non_watcher_run = Mock(side_effect=AssertionError("Unexpected fallback"))
        sensor.build_and_run_cmd = Mock(side_effect=AssertionError("Unexpected dbt execution"))
        return sensor

    def _context(self, sensor, status, compiled_sql="select 1"):
        uid = sensor.model_unique_id
        status_key = get_tests_status_xcom_key(uid) if sensor.is_test_sensor else get_status_xcom_key(uid)
        payload = status if sensor.is_test_sensor or status is None else {"status": status, "outlet_uris": ["db://out"]}
        values = {
            status_key: payload,
            get_compiled_sql_xcom_key(uid): compiled_sql,
            get_dbt_event_xcom_key(uid): {"status": status, "msg": "terminal event"},
        }
        ti = Mock(try_number=2)

        def pull(task_ids=None, key=None, **kwargs):
            assert task_ids == "producer"
            return values.get(key)

        ti.xcom_pull.side_effect = pull
        return {"ti": ti, "run_id": "same_run"}, values

    @pytest.mark.parametrize("producer_state", ["failed", "skipped"])
    @pytest.mark.parametrize("status,is_test", [("success", False), ("warn", False), ("pass", True)])
    @pytest.mark.parametrize("compiled_sql", ["select 1", None])
    def test_retry_preserves_success_without_fallback(self, producer_state, status, is_test, compiled_sql):
        sensor = self._make_sensor(is_test)
        sensor._get_producer_task_status.return_value = producer_state
        context, values = self._context(sensor, status, compiled_sql)

        with patch("cosmos.operators._watcher.base._log_dbt_event") as log_event:
            assert sensor.poke(context) is True

        sensor._fallback_to_non_watcher_run.assert_not_called()
        sensor.build_and_run_cmd.assert_not_called()
        log_event.assert_called_once_with(values[get_dbt_event_xcom_key(sensor.model_unique_id)])
        assert sensor.compiled_sql == (compiled_sql or "")
        if compiled_sql:
            sensor._override_rtif.assert_called_once_with(context)
        else:
            sensor._override_rtif.assert_not_called()
        if not is_test:
            assert sensor._outlet_uris == ["db://out"]
        expected_key = (
            get_tests_status_xcom_key(sensor.model_unique_id)
            if is_test
            else get_status_xcom_key(sensor.model_unique_id)
        )
        assert context["ti"].xcom_pull.call_args_list[0].kwargs["key"] == expected_key

    @pytest.mark.parametrize("producer_state", ["failed", "skipped"])
    @pytest.mark.parametrize(
        "status,is_test",
        [
            ("error", False),
            ("failed", False),
            ("runtime error", False),
            ("skipped", False),
            (None, False),
            ("unknown", False),
            ("fail", True),
            (None, True),
        ],
    )
    def test_retry_keeps_failure_or_unknown_fail_closed(self, producer_state, status, is_test):
        sensor = self._make_sensor(is_test)
        sensor._get_producer_task_status.return_value = producer_state
        context, _ = self._context(sensor, status)

        with pytest.raises(AirflowException, match="watcher fallback is disabled"):
            sensor.poke(context)

        sensor._fallback_to_non_watcher_run.assert_not_called()
        sensor.build_and_run_cmd.assert_not_called()
        sensor._override_rtif.assert_not_called()

    @pytest.mark.parametrize("payload", [{}, {"status": "running"}, "success"])
    def test_malformed_or_nonterminal_xcom_cannot_become_success(self, payload):
        sensor = self._make_sensor()
        context, values = self._context(sensor, None)
        values[get_status_xcom_key(sensor.model_unique_id)] = payload

        with pytest.raises((AirflowException, AttributeError)):
            sensor.poke(context)

        sensor._fallback_to_non_watcher_run.assert_not_called()
        sensor.build_and_run_cmd.assert_not_called()

    @pytest.mark.parametrize(
        "producer_state,enabled", [("success", False), ("success", True), ("failed", True), ("skipped", True)]
    )
    @pytest.mark.parametrize("is_test", [False, True])
    def test_manual_rerun_path_is_preserved(self, monkeypatch, producer_state, enabled, is_test):
        monkeypatch.setattr("cosmos.operators._watcher.base.settings.enable_watcher_fallback", enabled)
        sensor = self._make_sensor(is_test)
        sensor._get_producer_task_status.return_value = producer_state
        sensor._fallback_to_non_watcher_run.side_effect = None
        sensor._fallback_to_non_watcher_run.return_value = True
        context, _ = self._context(sensor, "pass" if is_test else "success")

        assert sensor.poke(context) is True

        sensor._fallback_to_non_watcher_run.assert_called_once_with(2, context)
        context["ti"].xcom_pull.assert_not_called()

    @pytest.mark.parametrize("producer_state", ["failed", "skipped", "running", None])
    def test_first_attempt_still_reads_reported_success(self, producer_state):
        sensor = self._make_sensor()
        sensor._get_producer_task_status.return_value = producer_state
        context, _ = self._context(sensor, "success")
        context["ti"].try_number = 1

        assert sensor.poke(context) is True
        sensor._fallback_to_non_watcher_run.assert_not_called()

    @pytest.mark.parametrize("producer_state", ["running", "up_for_retry", None])
    def test_active_or_unknown_producer_still_waits_without_a_result(self, producer_state):
        sensor = self._make_sensor()
        sensor._get_producer_task_status.return_value = producer_state
        context, _ = self._context(sensor, None)

        assert sensor.poke(context) is False
        sensor._fallback_to_non_watcher_run.assert_not_called()

    @pytest.mark.parametrize("producer_state", ["upstream_failed", "removed"])
    def test_other_terminal_states_keep_existing_guard(self, producer_state):
        sensor = self._make_sensor()
        sensor._get_producer_task_status.return_value = producer_state
        context, _ = self._context(sensor, "success")

        with pytest.raises(AirflowException, match="watcher fallback is disabled"):
            sensor.poke(context)

        context["ti"].xcom_pull.assert_not_called()
        sensor._fallback_to_non_watcher_run.assert_not_called()

    def test_real_backup_restore_preserves_success_and_actual_failure(self):
        """Use real compression/restore and status parsing; replace only Airflow storage."""
        good = self._make_sensor(unique_id="model.pkg.good")
        bad = self._make_sensor(unique_id="model.pkg.bad")
        good_context, values = self._context(good, "success")
        bad_context, bad_values = self._context(bad, "error")
        values.update(bad_values)
        bad_context["ti"].xcom_pull.side_effect = good_context["ti"].xcom_pull.side_effect

        producer_ti = Mock(dag_id="dag", try_number=1)
        producer_ti.task.task_group.group_id = "group"
        producer_ti.xcom_push.side_effect = lambda key, value: values.__setitem__(key, value)
        producer_context = {"ti": producer_ti, "run_id": "same_run"}
        variables = {}

        with (
            patch("cosmos.operators._watcher.xcom.set_variable", side_effect=variables.__setitem__),
            patch(
                "cosmos.operators._watcher.xcom.get_variable",
                side_effect=lambda key, default=None: variables.get(key, default),
            ),
            patch("cosmos.operators._watcher.xcom.delete_variable", side_effect=variables.__delitem__),
        ):
            _init_xcom_backup(producer_context, persist=False)
            for key, value in list(values.items()):
                safe_xcom_push(producer_ti, key, value)
            _backup_xcom_to_variable(producer_context)
            values.clear()  # Airflow clears the producer's XComs before its retry.
            producer_ti.try_number = 2
            assert _restore_xcom_from_variable(producer_context) is True

        assert variables == {}
        assert good.poke(good_context) is True
        with pytest.raises(AirflowException, match="watcher fallback is disabled"):
            bad.poke(bad_context)
        for sensor in (good, bad):
            sensor._fallback_to_non_watcher_run.assert_not_called()
            sensor.build_and_run_cmd.assert_not_called()
