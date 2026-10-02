import math
import time
from collections.abc import Callable, Iterator
from datetime import timedelta
from typing import Any

import pendulum
from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes import __version__ as airflow_k8s_provider_version
from airflow.providers.cncf.kubernetes.callbacks import ExecutionMode, KubernetesPodOperatorCallback
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodLoggingStatus, PodManager, get_container_status
from airflow.utils.timezone import utcnow
from kubernetes import client
from kubernetes.client.models.v1_pod import V1Pod
from packaging.version import Version
from pendulum import DateTime
from urllib3.exceptions import HTTPError, TimeoutError

from cosmos.constants import _K8s_WATCHER_MIN_K8S_PROVIDER_VERSION


def _iter_raw_log_lines(response: Any) -> Iterator[bytes]:
    """Split a finite HTTP body without losing split UTF-8 or an unterminated last line."""
    pending = b""
    for chunk in response.stream(amt=65536, decode_content=True):
        lines = (pending + chunk).split(b"\n")
        pending = lines.pop()
        for line in lines:
            yield line + b"\n"
    if pending:
        yield pending


# This is being added to overcome the issue with the KubernetesPodOperator logs repeating:
# https://github.com/apache/airflow/issues/59366
# It can be removed once it is fixed in the upstream provider.
class CosmosKubernetesPodManager(PodManager):  # type: ignore[misc]
    """Create, monitor, and otherwise interact with Kubernetes pods for use with the KubernetesPodOperator."""

    def __init__(
        self,
        kube_client: client.CoreV1Api,
        callbacks: list[type[KubernetesPodOperatorCallback]] | None = None,
        callback_extra_kwargs: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(kube_client=kube_client, callbacks=callbacks)
        self._callback_extra_kwargs = callback_extra_kwargs or {}

    def _extra_kwargs_for(self, callback: Any) -> dict[str, Any]:
        """Return ``callback_extra_kwargs`` only for callbacks that opt in via the marker.

        Cosmos threads its internal state (``tests_per_model``, ``test_results_per_model``,
        ``context_holder``, ``upstream_failure_skipped_ids``) to ``WatcherK8sCallback`` through
        ``callback_extra_kwargs``. The K8s-based watcher producer operators preserve
        user-supplied callbacks, which must not receive these Cosmos-only kwargs, or their
        ``progress_callback`` could raise ``TypeError`` while reading pod logs. Only callbacks
        marked with ``receives_cosmos_callback_kwargs = True`` (i.e. ``WatcherK8sCallback``)
        are given them.
        """
        if getattr(callback, "receives_cosmos_callback_kwargs", False):
            return self._callback_extra_kwargs
        return {}

    def fetch_container_logs(  # noqa: C901
        self,
        pod: V1Pod,
        container_name: str,
        *,
        follow: bool = False,
        since_time: DateTime | None = None,
        post_termination_timeout: int = 120,
        container_name_log_prefix_enabled: bool = True,
        log_formatter: Callable[[str, str], str] | None = None,
    ) -> PodLoggingStatus:
        """
        Follow the logs of container and stream to airflow logging.

        Returns when container exits.

        Between when the pod starts and logs being available, there might be a delay due to CSR not approved
        and signed yet. In such situation, ApiException is thrown. This is why we are retrying on this
        specific exception.

        :meta private:
        """

        if Version(airflow_k8s_provider_version) >= Version("10.10.0"):
            from airflow.providers.cncf.kubernetes.utils.pod_manager import parse_log_line
        elif Version(airflow_k8s_provider_version) >= _K8s_WATCHER_MIN_K8S_PROVIDER_VERSION:
            parse_log_line = self.parse_log_line
        else:
            raise ValueError(
                f"Unsupported K8s provider version: {airflow_k8s_provider_version}. "
                f"Minimum required version is {_K8s_WATCHER_MIN_K8S_PROVIDER_VERSION}"
            )

        request_timeout = (60 * 30, 60 * 5)
        last_log_time = since_time
        replay_all = since_time is not None
        pod_identity = (pod.metadata.name, pod.metadata.namespace, pod.metadata.uid)
        if not pod_identity[2]:
            # KPO returns the original request object after creating a new Pod,
            # without the server-assigned UID. Pin it before processing any logs.
            remote = self.read_pod(pod)
            if (remote.metadata.name, remote.metadata.namespace) != pod_identity[:2] or not remote.metadata.uid:
                raise AirflowException("Pod identity could not be established before reading container logs")
            pod_identity = (remote.metadata.name, remote.metadata.namespace, remote.metadata.uid)

        def process_logs(logs: Iterator[bytes], *, ignore_before: DateTime | None = None) -> None:
            """Use the same multiline, callback and logging path for live and finite reads."""
            nonlocal last_log_time
            message_to_log = None
            message_timestamp = None

            def deliver() -> None:
                nonlocal message_to_log, last_log_time
                message, message_to_log = message_to_log, None
                if message is None or (
                    ignore_before is not None and message_timestamp is not None and message_timestamp < ignore_before
                ):
                    return
                for callback in self._callbacks:
                    callback.progress_callback(
                        line=message,
                        client=self._client,
                        mode=ExecutionMode.SYNC,
                        container_name=container_name,
                        timestamp=message_timestamp,
                        pod=pod,
                        **self._extra_kwargs_for(callback),
                    )
                self._log_message(message, container_name, container_name_log_prefix_enabled, log_formatter)
                if message_timestamp is not None and (last_log_time is None or message_timestamp > last_log_time):
                    last_log_time = message_timestamp

            try:
                for raw_line in logs:
                    line = raw_line.decode("utf-8", errors="backslashreplace")
                    line_timestamp, message = parse_log_line(line)
                    if line_timestamp:
                        deliver()
                        message_to_log, message_timestamp = message, line_timestamp
                    else:
                        message_to_log = f"{message_to_log}\n{message}"
            finally:
                deliver()

        def consume_logs() -> Exception | None:
            """Suppress only live-read errors, never exceptions raised by callbacks."""
            exception = None

            def live_lines() -> Iterator[bytes]:
                nonlocal exception, replay_all
                since_seconds = None
                if last_log_time:
                    try:
                        since_seconds = math.ceil((pendulum.now() - last_log_time).total_seconds())
                    except TypeError:
                        self.log.warning(
                            "Error calculating since_seconds with since_time %s. Using None instead.", last_log_time
                        )
                try:
                    yield from self.read_pod_logs(
                        pod=pod,
                        container_name=container_name,
                        timestamps=True,
                        since_seconds=since_seconds,
                        follow=follow,
                        post_termination_timeout=post_termination_timeout,
                        _request_timeout=request_timeout,
                    )
                except (TimeoutError, HTTPError) as error:
                    exception, replay_all = error, True
                    if not isinstance(error, TimeoutError):
                        self._http_error_timestamps = getattr(self, "_http_error_timestamps", [])
                        self._http_error_timestamps = [
                            t for t in self._http_error_timestamps if t > utcnow() - timedelta(seconds=60)
                        ]
                        self._http_error_timestamps.append(utcnow())
                        if len(self._http_error_timestamps) > 2:
                            self.log.exception(
                                "Reading of logs interrupted for container %r; will retry.", container_name
                            )

            process_logs(live_lines())
            return exception

        def verify_terminated_pod() -> None:
            remote = self.read_pod(pod)
            if (remote.metadata.name, remote.metadata.namespace, remote.metadata.uid) != pod_identity:
                raise AirflowException("Pod identity changed while draining terminal container logs")
            status = get_container_status(remote, container_name)
            if status is None or status.state is None or status.state.terminated is None:
                raise AirflowException(f"Container {container_name!r} termination is not confirmed")

        def drain_terminal_logs() -> None:
            verify_terminated_pod()
            # A terminal container's retained log is finite. The provider reader can
            # stop at finished_at + 120s even with unread chunks, so bypass only that
            # iterator. No relative time filter: reconnects may already have skipped
            # events before the last delivered timestamp.
            response = self._client.read_namespaced_pod_log(
                name=pod.metadata.name,
                namespace=pod.metadata.namespace,
                container=container_name,
                follow=False,
                timestamps=True,
                _preload_content=False,
                _request_timeout=request_timeout,
            )
            try:
                # The named Pod can be replaced while the HTTP request opens.
                # Reject that response before publishing any of its statuses.
                verify_terminated_pod()
                response.enforce_content_length = True
                process_logs(_iter_raw_log_lines(response), ignore_before=None if replay_all else last_log_time)
                verify_terminated_pod()
            finally:
                try:
                    response.close()
                finally:
                    response.release_conn()

        # note: `read_pod_logs` follows the logs, so we shouldn't necessarily *need* to
        # loop as we do here. But in a long-running process we might temporarily lose connectivity.
        # So the looping logic is there to let us resume following the logs.
        while True:
            exc = consume_logs()
            if not self.container_is_running(pod, container_name=container_name):
                drain_terminal_logs()
                return PodLoggingStatus(running=False, last_log_time=last_log_time)
            if not follow:
                return PodLoggingStatus(running=True, last_log_time=last_log_time)
            # Even a clean live EOF can leave a gap before the next relative-time
            # request. Reconcile the full retained log after any reconnect.
            replay_all = True
            # a timeout is a normal thing and we ignore it and resume following logs
            if not isinstance(exc, TimeoutError):
                self.log.warning(
                    "Pod %s log read interrupted but container %s still running. "
                    "Retained logs will be replayed after termination; entries may be repeated.",
                    pod.metadata.name,
                    container_name,
                )
            time.sleep(1)
