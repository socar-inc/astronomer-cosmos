"""Terminated Watcher logs must reach HTTP EOF, not a wall-clock reader cutoff."""

import io
import json
from collections import Counter
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pendulum
import pytest
from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodLogsConsumer
from kubernetes.client import models as k8s
from urllib3.exceptions import HTTPError, ProtocolError, ReadTimeoutError
from urllib3.response import HTTPResponse

from cosmos.airflow._override import CosmosKubernetesPodManager, _iter_raw_log_lines
from cosmos.operators._k8s_common import WatcherK8sCallback

START = pendulum.datetime(2026, 10, 2, tz="UTC")


def make_pod(*, terminated=True, exit_code=0):
    state = k8s.V1ContainerState(
        terminated=k8s.V1ContainerStateTerminated(exit_code=exit_code, finished_at=START) if terminated else None,
        running=None if terminated else k8s.V1ContainerStateRunning(started_at=START),
    )
    return k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(name="watcher", namespace="test", uid="original-uid"),
        status=k8s.V1PodStatus(
            container_statuses=[
                k8s.V1ContainerStatus(
                    name="base",
                    image="test",
                    image_id="test",
                    ready=False,
                    restart_count=0,
                    state=state,
                )
            ]
        ),
    )


def log_line(message, seconds=0):
    return f"{START.add(seconds=seconds).to_iso8601_string()} {message}\n".encode()


def response_for(body):
    response = HTTPResponse(body=io.BytesIO(body), headers={"Content-Length": str(len(body))}, preload_content=False)
    response.close = Mock(wraps=response.close)
    response.release_conn = Mock(wraps=response.release_conn)
    return response


def make_manager(*, lines=(), body=b"", pod=None, callbacks=None, extra=None):
    pod = pod or make_pod()
    api = MagicMock()
    response = response_for(body)
    api.read_namespaced_pod_log.return_value = response
    manager = CosmosKubernetesPodManager(kube_client=api, callbacks=callbacks, callback_extra_kwargs=extra)
    manager.read_pod_logs = Mock(return_value=iter(lines))
    manager.read_pod = Mock(return_value=pod)
    manager.container_is_running = Mock(return_value=False)
    manager._log_message = Mock()
    return manager, pod, response


@pytest.mark.parametrize("exit_code", [0, 1])
def test_real_provider_cutoff_recovers_all_1364_incident_results(monkeypatch, exit_code):
    """Reproduce 810 collected + 554 unread, keeping the single dbt failure."""
    store = {}
    ti = SimpleNamespace(xcom_push=lambda key, value: store.__setitem__(key, value))
    clock = [START.add(seconds=119)]
    seen = []

    class ClockCallback:
        @staticmethod
        def progress_callback(*, line, client, mode, container_name, timestamp, pod):
            seen.append(line)
            if len(seen) >= 809:
                clock[0] = START.add(seconds=121)

    events = [
        log_line(
            json.dumps(
                {
                    "info": {"name": "LogModelResult"},
                    "data": {
                        "index": i + 1,
                        "total": 1364,
                        "node_info": {
                            "unique_id": f"model.incident.node_{i}",
                            "resource_type": "model",
                            "node_status": "error" if i == 911 else "success",
                        },
                    },
                }
            ),
            i / 100,
        )
        for i in range(1364)
    ]
    pod = make_pod(exit_code=exit_code)
    manager, _, final = make_manager(
        body=b"".join(events),
        pod=pod,
        callbacks=[ClockCallback, WatcherK8sCallback],
        extra={"context_holder": {"context": {"ti": ti}}},
    )
    followed = MagicMock()
    followed.stream.return_value = iter([b"".join(events[:810]), b"".join(events[810:])])
    monkeypatch.setattr("airflow.providers.cncf.kubernetes.utils.pod_manager.utcnow", lambda: clock[0])
    consumer = PodLogsConsumer(followed, pod, manager, "base")
    manager.read_pod_logs.return_value = consumer

    status = manager.fetch_container_logs(pod, "base", follow=True)

    assert status.running is False
    assert consumer.post_termination_timeout == 120
    assert len(store) == 1364
    assert Counter(v["status"] for v in store.values()) == {"success": 1363, "error": 1}
    # The timestamp boundary is inclusive, so node 809 is delivered again.
    assert len(seen) == 1365
    assert status.last_log_time == START.add(seconds=13.63)
    manager._client.read_namespaced_pod_log.assert_called_once_with(
        name="watcher",
        namespace="test",
        container="base",
        follow=False,
        timestamps=True,
        _preload_content=False,
        _request_timeout=(1800, 300),
    )
    final.close.assert_called_once()
    assert final.release_conn.called
    assert pod.status.container_statuses[0].state.terminated.exit_code == exit_code


def test_raw_lines_preserve_utf8_chunk_splits_and_final_line():
    body = "가나다\nsecond\n끝".encode()
    response = MagicMock()
    response.stream.return_value = iter([body[:1], body[1:5], body[5:12], body[12:]])
    assert list(_iter_raw_log_lines(response)) == ["가나다\n".encode(), b"second\n", "끝".encode()]


def test_real_kpo_new_pod_request_pins_uid_before_processing_logs():
    remote = make_pod()
    request = k8s.V1Pod(metadata=k8s.V1ObjectMeta(name="watcher", namespace="test"))
    operator = KubernetesPodOperator(task_id="new_pod", reattach_on_restart=False)
    operator.pod_manager = Mock()
    operator.pod_manager.create_pod.return_value = remote
    pod = operator.get_or_create_pod(request, context={})
    assert pod is request and pod.metadata.uid is None
    manager, _, response = make_manager(pod=remote, lines=[log_line("live")], body=log_line("final", 1))

    def live_read(**kwargs):
        manager.read_pod.assert_called_once_with(request)
        return iter([log_line("live")])

    manager.read_pod_logs.side_effect = live_read
    assert manager.fetch_container_logs(pod, "base").running is False
    assert [call.args[0] for call in manager._log_message.call_args_list] == ["live", "final"]
    assert pod.metadata.uid is None
    response.close.assert_called_once()


@pytest.mark.parametrize("change", ["uid", "name", "namespace"])
def test_uidless_request_requires_valid_identity_before_any_logs(change):
    request = k8s.V1Pod(metadata=k8s.V1ObjectMeta(name="watcher", namespace="test"))
    remote = make_pod()
    setattr(remote.metadata, change, None if change == "uid" else "changed")
    manager, _, _ = make_manager(pod=remote, lines=[log_line("live")])
    with pytest.raises(AirflowException, match="identity"):
        manager.fetch_container_logs(request, "base")
    manager.read_pod_logs.assert_not_called()
    manager._log_message.assert_not_called()
    manager._client.read_namespaced_pod_log.assert_not_called()


def test_uidless_request_never_adopts_replacement_uid_at_terminal_read():
    request = k8s.V1Pod(metadata=k8s.V1ObjectMeta(name="watcher", namespace="test"))
    original, replacement = make_pod(), make_pod()
    replacement.metadata.uid = "replacement"
    manager, _, _ = make_manager()
    manager.read_pod.side_effect = [original, replacement]
    with pytest.raises(AirflowException, match="identity"):
        manager.fetch_container_logs(request, "base")
    manager._client.read_namespaced_pod_log.assert_not_called()
    manager._log_message.assert_not_called()


def test_multiline_and_equal_timestamp_boundary_preserve_user_callback():
    received = []

    class UserCallback:
        @staticmethod
        def progress_callback(*, line, client, mode, container_name, timestamp, pod):
            received.append(line)

    first = log_line("first", 0)
    second = log_line("same timestamp", 1) + b"continuation\n"
    final = log_line("last no newline", 1).rstrip(b"\n")
    manager, pod, _ = make_manager(
        lines=[first, second],
        body=first + second + final,
        callbacks=[UserCallback],
        extra={"must_not_leak": True},
    )
    manager.fetch_container_logs(pod, "base")
    assert received == ["first", "same timestamp\ncontinuation", "same timestamp\ncontinuation\n", "last no newline"]


@pytest.mark.parametrize("initial_cursor,read_error", [(True, False), (False, True)])
def test_resume_or_read_error_replays_older_retained_events(initial_cursor, read_error):
    before = log_line("old missing event", 1)
    latest = log_line("latest delivered", 3)

    def live():
        yield latest
        if read_error:
            raise ReadTimeoutError(None, None, "interrupted")

    manager, pod, _ = make_manager(lines=live(), body=before + latest)
    result = manager.fetch_container_logs(pod, "base", since_time=START if initial_cursor else None)
    assert [call.args[0] for call in manager._log_message.call_args_list] == [
        "latest delivered",
        "old missing event",
        "latest delivered",
    ]
    assert result.last_log_time == START.add(seconds=3)


def test_timeout_keeps_actual_timestamp_without_two_second_jump(monkeypatch):
    def interrupted():
        yield log_line("before", 1)
        raise ReadTimeoutError(None, None, "interrupted")

    manager, pod, _ = make_manager(body=log_line("before", 1) + log_line("within two seconds", 2))
    manager.read_pod_logs.side_effect = [interrupted(), iter([])]
    manager.container_is_running.side_effect = [True, False]
    monkeypatch.setattr("cosmos.airflow._override.time.sleep", lambda _: None)
    monkeypatch.setattr("cosmos.airflow._override.pendulum.now", lambda: START.add(seconds=10))
    result = manager.fetch_container_logs(pod, "base", follow=True)
    assert manager.read_pod_logs.call_args_list[1].kwargs["since_seconds"] == 9
    assert result.last_log_time == START.add(seconds=2)
    assert any(c.args[0] == "within two seconds" for c in manager._log_message.call_args_list)


def test_clean_live_eof_reconnect_recovers_gap_before_latest_cursor(monkeypatch):
    first, gap, latest = (log_line(message, second) for second, message in ((1, "first"), (2, "gap"), (3, "latest")))
    manager, pod, response = make_manager(body=first + gap + latest)
    manager.read_pod_logs.side_effect = [iter([first]), iter([latest])]
    manager.container_is_running.side_effect = [True, False]
    monkeypatch.setattr("cosmos.airflow._override.time.sleep", lambda _: None)
    monkeypatch.setattr("cosmos.airflow._override.pendulum.now", lambda: START.add(seconds=10))

    result = manager.fetch_container_logs(pod, "base", follow=True)

    assert manager.read_pod_logs.call_count == 2
    assert manager.read_pod_logs.call_args_list[1].kwargs["since_seconds"] == 9
    assert [call.args[0] for call in manager._log_message.call_args_list] == [
        "first",
        "latest",
        "first",
        "gap",
        "latest",
    ]
    assert result.last_log_time == START.add(seconds=3)
    manager._client.read_namespaced_pod_log.assert_called_once()
    response.close.assert_called_once()


@pytest.mark.parametrize("error", [HTTPError("broken"), ReadTimeoutError(None, None, "timeout")])
def test_final_stream_error_is_not_success_and_closes_response(error):
    manager, pod, response = make_manager()
    response.stream = Mock(side_effect=error)
    with pytest.raises(type(error)):
        manager.fetch_container_logs(pod, "base")
    response.close.assert_called_once()
    assert response.release_conn.called
    manager._client.read_namespaced_pod_log.assert_called_once()


def test_content_length_truncation_fails_closed():
    body = log_line("last")
    manager, pod, _ = make_manager()
    response = HTTPResponse(
        body=io.BytesIO(body),
        headers={"Content-Length": str(len(body) + 10)},
        preload_content=False,
        enforce_content_length=False,
    )
    response.close = Mock(wraps=response.close)
    response.release_conn = Mock(wraps=response.release_conn)
    manager._client.read_namespaced_pod_log.return_value = response
    with pytest.raises(ProtocolError):
        manager.fetch_container_logs(pod, "base")
    assert response.enforce_content_length is True
    assert response.close.called and response.release_conn.called


def test_callback_error_propagates_without_retry_and_closes_response():
    callback = Mock()
    callback.progress_callback.side_effect = HTTPError("callback failed")
    manager, pod, response = make_manager(body=log_line("one"), callbacks=[callback])
    with pytest.raises(HTTPError, match="callback failed"):
        manager.fetch_container_logs(pod, "base")
    callback.progress_callback.assert_called_once()
    response.close.assert_called_once()
    assert response.release_conn.called


@pytest.mark.parametrize("change", ["uid", "name", "namespace", "termination"])
def test_unconfirmed_identity_or_termination_never_starts_final_read(change):
    manager, pod, _ = make_manager()
    remote = make_pod()
    if change == "termination":
        remote.status.container_statuses[0].state = k8s.V1ContainerState()
    else:
        setattr(remote.metadata, change, "changed")
    manager.read_pod.return_value = remote
    with pytest.raises(AirflowException):
        manager.fetch_container_logs(pod, "base")
    manager._client.read_namespaced_pod_log.assert_not_called()


def test_identity_change_while_opening_response_never_publishes_status():
    manager, pod, response = make_manager(body=log_line("one"))
    changed = make_pod()
    changed.metadata.uid = "replacement"
    manager.read_pod.side_effect = [pod, changed]
    with pytest.raises(AirflowException, match="identity"):
        manager.fetch_container_logs(pod, "base")
    manager._log_message.assert_not_called()
    response.close.assert_called_once()
    assert response.release_conn.called


def test_identity_change_during_final_read_fails_closed():
    manager, pod, response = make_manager(body=log_line("one"))
    changed = make_pod()
    changed.metadata.uid = "replacement"
    manager.read_pod.side_effect = [pod, pod, changed]
    with pytest.raises(AirflowException, match="identity"):
        manager.fetch_container_logs(pod, "base")
    response.close.assert_called_once()
    assert response.release_conn.called


def test_empty_normal_eof_is_allowed():
    manager, pod, response = make_manager()
    assert manager.fetch_container_logs(pod, "base").running is False
    response.close.assert_called_once()


def test_running_nonfollow_keeps_existing_behavior():
    manager, pod, _ = make_manager(lines=[log_line("running")], pod=make_pod(terminated=False))
    manager.container_is_running.return_value = True
    assert manager.fetch_container_logs(pod, "base", follow=False).running is True
    manager._client.read_namespaced_pod_log.assert_not_called()


def test_replayed_test_results_are_aggregated_once_per_unique_test():
    store = {}
    ti = SimpleNamespace(xcom_push=lambda key, value: store.__setitem__(key, value))
    results = {}
    events = [
        log_line(
            json.dumps(
                {
                    "info": {"name": "LogModelResult"},
                    "data": {
                        "node_info": {
                            "unique_id": f"test.pkg.t{i}",
                            "resource_type": "test",
                            "node_status": "pass",
                        }
                    },
                }
            ),
            i,
        )
        for i in (1, 2)
    ]
    manager, pod, _ = make_manager(
        lines=events[:1],
        body=b"".join(events),
        callbacks=[WatcherK8sCallback],
        extra={
            "context_holder": {"context": {"ti": ti}},
            "tests_per_model": {"model.pkg.m": ["test.pkg.t1", "test.pkg.t2"]},
            "test_results_per_model": results,
        },
    )
    manager.fetch_container_logs(pod, "base", since_time=START)
    assert len(results["model.pkg.m"]) == 2
    assert store["model__pkg__m_tests_status"] == "pass"
