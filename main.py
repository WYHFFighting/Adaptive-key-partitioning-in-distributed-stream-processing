# -*- coding: utf-8 -*-
import numpy as np
from model import *
import pickle
from args import args
from utils import *


def run():
    with open(args.data_path, 'rb') as file:
        stream = pickle.load(file)

    substream_list = [stream[i: i + args.segment_size] for i in range(0, len(stream), args.segment_size)]
    # operators = [Operator() for _ in range(5)]
    operator = Operator(num_workers = 5)
    scheduler = Scheduler(num_workers = operator.num_workers)
    scheduler.start_auto_trim(auto_trim_dict)

    for subS in substream_list:
        scheduler.reset_ss()

        for i in range(0, args.segment_size, args.m):
            micro_batch = subS[i: i + args.m]
            routing_table = scheduler.process(micro_batch)
            extended_num_workers = scheduler.num_workers - operator.num_workers
            operator.process(micro_batch, routing_table, extended_num_workers)

        # break

    for idx, worker in enumerate(operator.workers):
        print(f"Worker {idx} processed {len(worker.tuples)} tuples.")

    print()
    print('load imbalance:')
    print(operator.get_load_imbalance())
    print('replication factor:')
    print(scheduler.get_replication_factor())


if __name__ == "__main__":
    run()
