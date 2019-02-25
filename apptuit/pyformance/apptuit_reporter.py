"""
Apptuit Pyformance Reporter
"""
import os
import resource
import sys
import threading
import gc as garbage_collector

from pyformance import MetricsRegistry
from pyformance.reporters.reporter import Reporter
from apptuit import Apptuit, DataPoint, timeseries, ApptuitSendException, TimeSeriesName
from apptuit.utils import _get_tags_from_environment

NUMBER_OF_TOTAL_POINTS = "apptuit.reporter.send.total"
NUMBER_OF_SUCCESSFUL_POINTS = "apptuit.reporter.send.successful"
NUMBER_OF_FAILED_POINTS = "apptuit.reporter.send.failed"
API_CALL_TIMER = "apptuit.reporter.send.time"
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
        gc_metric_names = [
            TimeSeriesName.encode_metric("python.garbage.collector.collection",
                                         {"type": "collection_0", "worker_id": self.pid}),
            TimeSeriesName.encode_metric("python.garbage.collector.collection",
                                         {"type": "collection_1", "worker_id": self.pid}),
            TimeSeriesName.encode_metric("python.garbage.collector.collection",
                                         {"type": "collection_2", "worker_id": self.pid}),
            TimeSeriesName.encode_metric("python.garbage.collector.threshold", {"type": "threshold_0", "worker_id": self.pid}),
            TimeSeriesName.encode_metric("python.garbage.collector.threshold", {"type": "threshold_1", "worker_id": self.pid}),
            TimeSeriesName.encode_metric("python.garbage.collector.threshold", {"type": "threshold_2", "worker_id": self.pid})
        ]
        return gc_metric_names

    def _get_thread_metic_names(self):
        thread_metric_names = [
            TimeSeriesName.encode_metric("python.thread", {"type": "demon", "worker_id": self.pid}),
            TimeSeriesName.encode_metric("python.thread", {"type": "alive", "worker_id": self.pid}),
            TimeSeriesName.encode_metric("python.thread", {"type": "dummy", "worker_id": self.pid}),
        ]
        return thread_metric_names

    def _get_resource_metic_names(self):
        resource_metric_names = [
            TimeSeriesName.encode_metric("python.cpu.time.used.seconds", {"type": "user", "worker_id": self.pid, }),
            TimeSeriesName.encode_metric("python.cpu.time.used.seconds", {"type": "system", "worker_id": self.pid, }),
            TimeSeriesName.encode_metric("python.memory.usage.kilobytes", {"type": "main", "worker_id": self.pid, }),
            TimeSeriesName.encode_metric("python.memory.usage.kilobytes", {"type": "shared", "worker_id": self.pid, }),
            TimeSeriesName.encode_metric("python.memory.usage.kilobytes", {"type": "unshared", "worker_id": self.pid, }),
            TimeSeriesName.encode_metric("python.memory.usage.kilobytes",
                                         {"type": "unshared_stack_size", "worker_id": self.pid, }),
            TimeSeriesName.encode_metric("python.page.faults", {"type": "without_IO", "worker_id": self.pid, }),
            TimeSeriesName.encode_metric("python.page.faults", {"type": "with_IO", "worker_id": self.pid, }),
            TimeSeriesName.encode_metric("python.process.swaps", {"worker_id": self.pid, }),
            TimeSeriesName.encode_metric("python.block.operations", {"type": "input", "worker_id": self.pid, }),
            TimeSeriesName.encode_metric("python.block.operations", {"type": "output", "worker_id": self.pid, }),
            TimeSeriesName.encode_metric("python.ipc.messages", {"type": "sent", "worker_id": self.pid, }),
            TimeSeriesName.encode_metric("python.ipc.messages", {"type": "received", "worker_id": self.pid, }),
            TimeSeriesName.encode_metric("python.system.signals", {"worker_id": self.pid, }),
            TimeSeriesName.encode_metric("python.context.switch", {"type": "voluntary", "worker_id": self.pid, }),
            TimeSeriesName.encode_metric("python.context.switch", {"type": "involuntary", "worker_id": self.pid, }),
        ]
        return resource_metric_names

    def __init__(self, registry=None, reporting_interval=10, token=None,
                 api_endpoint="https://api.apptuit.ai", prefix="", tags=None,
                 error_handler=default_error_handler, process_metrics=False):
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
                number of points processed succesfully, failed_points is number of failed points
                and errors is a list of error messages describing reason of each failure.
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
        self.prefix = prefix if prefix is not None else ""
        self.__decoded_metrics_cache = {}
        self.client = Apptuit(token, api_endpoint, ignore_environ_tags=True)
        self._meta_metrics_registry = MetricsRegistry()
        self.error_handler = error_handler
        self.pid = os.getpid()
        self.process_metrics = process_metrics
        if self.process_metrics:
            self.resource_metric_names = self._get_resource_metic_names()
            self.thread_metrics_names = self._get_thread_metic_names()
            self.gc_metric_names = self._get_gc_metric_names()

    def _update_counter(self, key, value):
        self._meta_metrics_registry.counter(key).inc(value)

    def _collect_metrics_from_list(self, metric_names, metric_val):
        for ind, metric in enumerate(metric_val):
            metric_counter = self.registry.gauge(metric_names[ind])
            metric_counter.set_value(metric)

    def collect_resource_metrics(self):

        resource_metrics = resource.getrusage(resource.RUSAGE_SELF)
        self._collect_metrics_from_list(self.resource_metric_names, resource_metrics)
        th = threading.enumerate()
        thread_metrics = [
            [t.daemon is True for t in th].count(True),
            [t.daemon is False for t in th].count(True),
            [type(t) is threading._DummyThread for t in th].count(True)
        ]
        self._collect_metrics_from_list(self.thread_metrics_names, thread_metrics)
        if garbage_collector.isenabled():
            collection = list(garbage_collector.get_count())
            threshold = list(garbage_collector.get_threshold())
            self._collect_metrics_from_list(self.gc_metric_names, collection + threshold)

    def report_now(self, registry=None, timestamp=None):
        """
        Report the data
        Params:
            registry: pyformance Registry containing all metrics
            timestamp: timestamp of the data point
        """
        if os.getpid() != self.pid:
            self.registry = MetricsRegistry()
            self.pid = os.getpid()
            if self.process_metrics:
                self.resource_metric_names = self._get_resource_metic_names()
                self.thread_metrics_names = self._get_thread_metic_names()
                self.gc_metric_names = self._get_gc_metric_names()
            return
        if self.process_metrics:
            self.collect_resource_metrics()
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

        metric_name, metric_tags = timeseries.decode_metric(key)
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
                dp = DataPoint(metric="{0}{1}.{2}".format(self.prefix, metric_name, value_key),
                               tags=tags, timestamp=timestamp, value=metrics[key][value_key])
                dps.append(dp)
        return dps
