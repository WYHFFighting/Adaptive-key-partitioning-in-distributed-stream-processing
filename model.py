import random
from collections import defaultdict
from args import args
import time
import threading
from collections import OrderedDict
import numpy as np


class Operator:
    def __init__(self, num_workers):
        self.num_workers = num_workers
        self.workers = [Worker() for _ in range(self.num_workers)]

    def process(self, batch, routing_table, extended_num_workers):
        """Process incoming tuple."""
        for _ in range(extended_num_workers):
            self.add_worker()

        for tup in batch:
            self.workers[routing_table[tup[1]]].process_tuple(tup)

    def add_worker(self):
        self.num_workers += 1
        self.workers.append(Worker())

    def get_load_imbalance(self):
        worker_load_list = [len(w.tuples) for w in self.workers]
        return (max(worker_load_list) - np.mean(worker_load_list)) / np.mean(worker_load_list)


class Worker:
    def __init__(self):
        self.tuples = []

    def process_tuple(self, data_tuple):
        if isinstance(data_tuple, list):
            self.tuples.extend(data_tuple)
        else:
            self.tuples.append(data_tuple)
        # More processing logic can be added here


class Scheduler:
    def __init__(self, num_workers):
        # self.routing_table = defaultdict(list)
        # self.routing_table = defaultdict(set)
        self.routing_table = {}
        self.high_freq_threshold_table = RecentDict(max_size = int(5e4))
        self.high_freq_mapped_workers = RecentDict(max_size = int(5e4))
        # 记录每个 key 映射的 worker，用于后续计算 replication_factor
        self.key_mapped_workers = RecentDict(max_size = int(5e4))
        self.space_saving = SpaceSaving()
        self.num_workers = num_workers
        self.frequency_threshold = args.theta * args.segment_size
        self.hb = [0] * self.num_workers
        self.hash_function = lambda x: hash(x) % self.num_workers
        # self.tot_unique_keys = set()

    def process(self, batch):
        unique_keys = self.space_saving.process(batch)
        # self.tot_unique_keys.update(unique_keys)
        for key in unique_keys:
            worker_id = self.assign_operator(key)
            if key in self.key_mapped_workers:
                self.key_mapped_workers[key].add(worker_id)
            else:
                self.key_mapped_workers[key] = {worker_id}
            self.routing_table[key] = worker_id

        return self.routing_table

    def assign_operator(self, key):
        frequency = self.space_saving.get_frequency(key)
        if frequency < self.frequency_threshold:
            worker_id = self.hash_function(key)
        else:
            if key not in self.high_freq_threshold_table:
                worker_id = self.find_least_load()
                self.high_freq_threshold_table[key] = frequency
                self.high_freq_mapped_workers[key] = [worker_id]
            elif frequency >= self.high_freq_threshold_table[key]:
                worker_id = self.extend_workers()
                self.high_freq_threshold_table[key] = self.high_freq_threshold_table[key] * args.delta
                self.high_freq_mapped_workers[key].append(worker_id)
            else:
                # print('*******************************')
                worker_id = self.find_least_load(key)

        self.hb[worker_id] += frequency

        return worker_id

    def extend_workers(self):
        self.hb.append(0)
        self.num_workers += 1
        self.hash_function = lambda x: hash(x) % self.num_workers

        return self.num_workers - 1

    def find_least_load(self, key = None):
        if key is None:
            return self.hb.index(min(self.hb))
        else:
            target_worker_id, minimum_load = 0, np.inf
            for worker_id in self.high_freq_mapped_workers[key]:
                if self.hb[worker_id] < minimum_load:
                    target_worker_id, minimum_load = worker_id, self.hb[worker_id]

            return target_worker_id

    def start_auto_trim(self, auto_trim_func):
        # cleared and retained the top 50k records every 10 min
        auto_trim_func(self.high_freq_threshold_table, interval = 600)
        auto_trim_func(self.high_freq_mapped_workers, interval = 600)
        auto_trim_func(self.key_mapped_workers, interval = 600)

    def reset_ss(self):
        self.space_saving = SpaceSaving()

    def get_replication_factor(self):
        ans = 0
        for value in self.key_mapped_workers.values():
            ans += len(value)

        return ans / len(self.key_mapped_workers.keys())


class SpaceSaving:
    def __init__(self, num_counters = args.segment_size):
        self.counters = {}  # Maps key to (count, error)
        self.num_counters = num_counters
        self.error_bound = args.epsilon * args.segment_size

    def process(self, batch):
        unique_keys = set()
        for _, key, _ in batch:
            self.update(key)
            unique_keys.add(key)

        return unique_keys

    def update(self, key):
        if key in self.counters:
            self.counters[key] += 1
        elif len(self.counters) < self.num_counters:
            self.counters[key] = 1
        else:
            least_key = min(self.counters, key = self.counters.get)
            if self.counters[least_key] < self.error_bound:
                self.counters[key] = self.counters.pop(least_key) + 1
            else:
                self.counters[least_key] += 1

    def get_frequency(self, key):
        if key in self.counters:
            return self.counters[key]
        return 0


class RecentDict(OrderedDict):
    def __init__(self, max_size = 100, *args2, **kwargs):
        self.max_size = max_size
        super().__init__(*args2, **kwargs)

    def __setitem__(self, key, value):
        if key in self:
            # 如果键已经存在，将其移到末尾以保持最近使用顺序
            self.move_to_end(key)
        super().__setitem__(key, value)
        # if len(self) > self.max_size:
        #     # 如果元素数量超过max_size，删除最早添加的元素
        #     self.popitem(last = False)

    def trim(self):
        """每次调用时，移除多余的元素，保留最近添加的前 max_size 个"""
        while len(self) > self.max_size:
            self.popitem(last = False)


class DataStream:
    def __init__(self, keys_range, values_range):
        self.keys_range = keys_range
        self.values_range = values_range

    def generate_tuple(self):
        """Simulate generating a single data tuple."""
        return random.randint(1, self.keys_range), random.randint(1, self.values_range)


