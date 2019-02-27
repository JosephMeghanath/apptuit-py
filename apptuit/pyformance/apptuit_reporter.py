# coding=utf-8
"""
Apptuit Pyformance Reporter
"""
import gc
import os
import resource
import socket
import sys
import threading

from pyformance import MetricsRegistry
from pyformance.reporters.reporter import Reporter

from apptuit import Apptuit, DataPoint, ApptuitSendException, TimeSeriesName
from ..utils import _get_tags_from_environment, strtobool

NUMBER_OF_TOTAL_POINTS = "apptuit.reporter.send.total"
NUMBER_OF_SUCCESSFUL_POINTS = "apptuit.reporter.send.successful"
NUMBER_OF_FAILED_POINTS = "apptuit.reporter.send.failed"
API_CALL_TIMER = "apptuit.reporter.send.time"
DISABLE_HOST_TAG = "APPTUIT_DISABLE_HOST_TAG"
BATCH_SIZE = 50000


def default_error_handler(status_code, successful, failed, errors):
    """
    This is the default error handler for the ApptuitReporter.
    It simply writes the errors to stderr.
    Parameters
    ----------
        status_code: response status_code of Apptuit.send()
        successful: number of datapoints updated successfully
        failed: number of datapoints updating failed
        errors: errors in response
    """
    msg = "%d points out of %d had errors\n" \
          "HTTP status returned from Apptuit: %d\n" \
          "Detailed error messages: %s\n" % \
          (failed, successful + failed, status_code, str(errors))
    sys.stderr.write(msg)


class ApptuitReporter(Reporter):
    """
        Pyformance based reporter for Apptuit. It provides high level
        primitives, such as meter, counter, gauge, etc., for collecting
        data and reports them asynchronously to Apptuit.
    """

    def _get_gc_metric_names(self):
        """
        To get a list of gc metric names.
        :return: a list of gc metric names.
        """
        gc_metric_names = [
            TimeSeriesName.encode_metric("python.gc.collection",
                                         {"type": "collection_0", "worker_id": self.pid}),
            TimeSeriesName.encode_metric("python.gc.collection",
                                         {"type": "collection_1", "worker_id": self.pid}),
            TimeSeriesName.encode_metric("python.gc.collection",
                                         {"type": "collection_2", "worker_id": self.pid}),
            TimeSeriesName.encode_metric("python.gc.threshold",
                                         {"type": "threshold_0", "worker_id": self.pid}),
            TimeSeriesName.encode_metric("python.gc.threshold",
                                         {"type": "threshold_1", "worker_id": self.pid}),
            TimeSeriesName.encode_metric("python.gc.threshold",
                                         {"type": "threshold_2", "worker_id": self.pid})
        ]
        return gc_metric_names

    def _get_thread_metic_names(self):
        """
        To get a list of thread metric names.
        :return: a list of thread metric names.
        """
        thread_metric_names = [
            TimeSeriesName.encode_metric("python.thread",
                                         {"type": "demon", "worker_id": self.pid}),
            TimeSeriesName.encode_metric("python.thread",
                                         {"type": "alive", "worker_id": self.pid}),
            TimeSeriesName.encode_metric("python.thread",
                                         {"type": "dummy", "worker_id": self.pid}),
        ]
        return thread_metric_names

    def _get_resource_metic_names(self):
        """
        To get a list of resource metric names.
        :return: a list of resource metric names.
        """
        resource_metric_names = [
            TimeSeriesName.encode_metric("python.cpu.time.used.seconds",
                                         {"type": "user", "worker_id": self.pid, }),
            TimeSeriesName.encode_metric("python.cpu.time.used.seconds",
                                         {"type": "system", "worker_id": self.pid, }),
            TimeSeriesName.encode_metric("python.memory.usage.kilobytes",
                                         {"type": "main", "worker_id": self.pid, }),
            TimeSeriesName.encode_metric("python.memory.usage.kilobytes",
                                         {"type": "shared", "worker_id": self.pid, }),
            TimeSeriesName.encode_metric("python.memory.usage.kilobytes",
                                         {"type": "unshared", "worker_id": self.pid, }),
            TimeSeriesName.encode_metric("python.memory.usage.kilobytes",
                                         {"type": "unshared_stack_size",
                                          "worker_id": self.pid, }),
            TimeSeriesName.encode_metric("python.page.faults",
                                         {"type": "major", "worker_id": self.pid, }),
            TimeSeriesName.encode_metric("python.page.faults",
                                         {"type": "minor", "worker_id": self.pid, }),
            TimeSeriesName.encode_metric("python.process.swaps",
                                         {"worker_id": self.pid, }),
            TimeSeriesName.encode_metric("python.block.operations",
                                         {"type": "input", "worker_id": self.pid, }),
            TimeSeriesName.encode_metric("python.block.operations",
                                         {"type": "output", "worker_id": self.pid, }),
            TimeSeriesName.encode_metric("python.ipc.messages",
                                         {"type": "sent", "worker_id": self.pid, }),
            TimeSeriesName.encode_metric("python.ipc.messages",
                                         {"type": "received", "worker_id": self.pid, }),
            TimeSeriesName.encode_metric("python.system.signals",
                                         {"worker_id": self.pid, }),
            TimeSeriesName.encode_metric("python.context.switches",
                                         {"type": "voluntary", "worker_id": self.pid, }),
            TimeSeriesName.encode_metric("python.context.switches",
                                         {"type": "involuntary", "worker_id": self.pid, }),
        ]
        return resource_metric_names

    def __init__(self, sanitize_mode, registry=None, reporting_interval=10, token=None,
                 api_endpoint="https://api.apptuit.ai", prefix="", tags=None,
                 error_handler=default_error_handler, disable_host_tag=None,
                 collect_process_metrics=False):
        """
        Parameters
        ----------
            registry: An instance of MetricsRegistry from pyformance. It is
                used as a container for all the metrics. If None, a new instance will be
                created internally
            reporting_interval: Reporting interval in seconds
            token: Apptuit API token
            prefix: Optional prefix for metric names, this will be prepended to all the
                metric names
            tags: A dictionary of tag keys and values which will be included with
                all the metrics reported by this reporter
            error_handler: A function to be executed in case of errors when reporting
                the data. If not specified, the default error handler will be used which by
                default writes the errors to stderr. The expected signature of an error handler
                is: error_handler(status_code, successful_points, failed_points, errors). Here
                status_code is the HTTP status code of the failed API call, successful_points is
                number of points processed successfully, failed_points is number of failed points
                and errors is a list of error messages describing reason of each failure.
            disable_host_tag: By default a host tag will be added to all the metrics reported by
                the reporter. Set disable_host_tag to False if you wish to disable it
            collect_process_metrics: A boolean variable specifying if process metrics should be
                collected or not, if set to True then process metrics will be collected. By default,
                this will collect resource, thread, and gc metrics.
            prometheus_compatible: A boolean value to make the metric names prometheus compatible,
                by default this is false, If set to True you can make the metric names prometheus
                compatible.
        """
        super(ApptuitReporter, self).__init__(registry=registry,
                                              reporting_interval=reporting_interval)
        self.endpoint = api_endpoint
        self.token = token
        self.tags = tags
        environ_tags = _get_tags_from_environment()
        if environ_tags:
            if self.tags is not None:
                environ_tags.update(self.tags)
            self.tags = environ_tags
        if disable_host_tag is None:
            disable_host_tag = os.environ.get(DISABLE_HOST_TAG, False)
            if disable_host_tag:
                disable_host_tag = strtobool(disable_host_tag)

        if not disable_host_tag:
            if self.tags:
                if self.tags.get("host") is None:
                    self.tags["host"] = socket.gethostname()
            else:
                self.tags = {"host": socket.gethostname()}
        self.prefix = prefix if prefix is not None else ""
        self.__decoded_metrics_cache = {}
        self.client = Apptuit(sanitize_mode, token, api_endpoint, ignore_environ_tags=True)
        self._meta_metrics_registry = MetricsRegistry()
        self.error_handler = error_handler
        self.pid = os.getpid()
        self.collect_process_metrics = collect_process_metrics
        if self.collect_process_metrics:
            self.resource_metric_names = self._get_resource_metic_names()
            self.thread_metrics_names = self._get_thread_metic_names()
            self.gc_metric_names = self._get_gc_metric_names()
            self.previous_resource_metrics = [0] * len(self.resource_metric_names)
            self.previous_gc_metrics = [0] * len(self.gc_metric_names)

    def _update_counter(self, key, value):
        """
        To increment the counter with `key` by `value`.
        :param key: Name of counter.
        :param value: value to increment.
        """
        self._meta_metrics_registry.counter(key).inc(value)

    def _collect_counter_from_list(self, metric_names, metric_values):
        """
        To increment list of counters `metric_names` with values `metric_values`.
        :param metric_names: A list of counter names.
        :param metric_values: A list of corresponding value to increment.
        """
        for ind, metric_value in enumerate(metric_values):
            metric_counter = self.registry.counter(metric_names[ind])
            metric_counter.inc(metric_value)

    def _collect_gauge_from_list(self, metric_names, metric_values):
        """
        To increment list of gauge `metric_names` with values `metric_values`.
        :param metric_names: A list of gauge names.
        :param metric_values: A list of corresponding value to set.
        """
        for ind, metric in enumerate(metric_values):
            metric_counter = self.registry.gauge(metric_names[ind])
            metric_counter.set_value(metric)

    def _collect_process_metrics(self):
        """
        To collect all the process metrics.
        """
        resource_metrics = resource.getrusage(resource.RUSAGE_SELF)
        resource_metrics = [cur_val - pre_val
                            for cur_val, pre_val in
                            zip(resource_metrics, self.previous_resource_metrics)]
        self._collect_counter_from_list(self.resource_metric_names, resource_metrics)
        th_values = threading.enumerate()
        thread_metrics = [
            [t.daemon is True for t in th_values].count(True),
            [t.daemon is False for t in th_values].count(True),
            [isinstance(t, threading._DummyThread) for t in th_values].count(True)
        ]
        self._collect_gauge_from_list(self.thread_metrics_names, thread_metrics)
        if gc.isenabled():
            collection = list(gc.get_count())
            threshold = list(gc.get_threshold())
            gc_metrics = collection + threshold
            gc_metrics = [cur_val - pre_val
                          for cur_val, pre_val in zip(gc_metrics, self.previous_gc_metrics)]
            self._collect_counter_from_list(self.gc_metric_names, gc_metrics)
            self.previous_gc_metrics = gc_metrics
        self.previous_resource_metrics = resource_metrics

    def report_now(self, registry=None, timestamp=None):
        """
        Report the data
        Params:
            registry: pyformance Registry containing all metrics
            timestamp: timestamp of the data point
        """
        pid = os.getpid()
        if pid != self.pid:
            self.registry.clear()
            self.pid = pid
            if self.collect_process_metrics:
                self.resource_metric_names = self._get_resource_metic_names()
                self.thread_metrics_names = self._get_thread_metic_names()
                self.gc_metric_names = self._get_gc_metric_names()
                self.previous_resource_metrics = [0] * len(self.resource_metric_names)
                self.previous_gc_metrics = [0] * len(self.gc_metric_names)
            return
        if self.collect_process_metrics:
            self._collect_process_metrics()
        dps = self._collect_data_points(registry or self.registry, timestamp)
        meta_dps = self._collect_data_points(self._meta_metrics_registry)
        if not dps:
            return
        dps_len = len(dps)
        success_count = 0
        failed_count = 0
        errors = []
        for i in range(0, dps_len, BATCH_SIZE):
            try:
                with self._meta_metrics_registry.timer(API_CALL_TIMER).time():
                    end_index = min(dps_len, i + BATCH_SIZE)
                    self.client.send(dps[i: end_index])
                    points_sent_count = end_index - i
                    self._update_counter(NUMBER_OF_TOTAL_POINTS, points_sent_count)
                    self._update_counter(NUMBER_OF_SUCCESSFUL_POINTS, points_sent_count)
                    self._update_counter(NUMBER_OF_FAILED_POINTS, 0)
                    success_count += points_sent_count
            except ApptuitSendException as exception:
                self._update_counter(NUMBER_OF_SUCCESSFUL_POINTS, exception.success)
                self._update_counter(NUMBER_OF_FAILED_POINTS, exception.failed)
                success_count += exception.success
                failed_count += exception.failed
                errors += exception.errors
                if self.error_handler:
                    self.error_handler(
                        exception.status_code,
                        exception.success,
                        exception.failed,
                        exception.errors
                    )
        self.client.send(meta_dps)
        if failed_count != 0:
            raise ApptuitSendException("Failed to send %d out of %d points" %
                                       (failed_count, dps_len), success=success_count,
                                       failed=failed_count, errors=errors)

    def _get_tags(self, key):
        """
        Get tags of a metric
        Params:
            metric key
        Returns:
            metric name, dictionary of tags
        """
        val = self.__decoded_metrics_cache.get(key)
        if val:
            return val[0], val[1]

        metric_name, metric_tags = TimeSeriesName.decode_metric(key)
        self.__decoded_metrics_cache[key] = (metric_name, metric_tags)
        return metric_name, metric_tags

    def _collect_data_points(self, registry, timestamp=None):
        """
        will collect all metrics from registry and convert them to DataPoints
        Params:
            registry: pyformance registry object
            timestamp: timestamp of the data point
        Returns:
            list of DataPoints
        """
        timestamp = timestamp or int(round(self.clock.time()))
        metrics = registry.dump_metrics()
        dps = []
        global_tags = self.tags if self.tags else {}
        for key in metrics.keys():
            metric_name, metric_tags = self._get_tags(key)
            if metric_tags and global_tags:
                tags = global_tags.copy()
                tags.update(metric_tags)
            elif metric_tags:
                tags = metric_tags
            else:
                tags = global_tags
            for value_key in metrics[key].keys():
                data_point = DataPoint(
                    metric=self.prefix + metric_name + '.' + value_key,
                    tags=tags, timestamp=timestamp, value=metrics[key][value_key])
                dps.append(data_point)
        return dps
