# -*- coding: utf-8 -*-
from model import *
import threading


# def simulate_stream_processing():
#     stream = DataStream(100, 100)
#     operators = [Operator() for _ in range(5)]
#     scheduler = Scheduler(num_operators=len(operators))
#
#     for _ in range(1000):
#         key, value = stream.generate_tuple()
#         operator_id = scheduler.process_tuple(key, value)
#         operators[operator_id].process_tuple((key, value))
#
#     for idx, operator in enumerate(operators):
#         print(f"Operator {idx} processed {len(operator.tuples)} tuples.")


def auto_trim_dict(recent_dict, interval = 600):
    """
    定时任务，每隔 interval 秒（默认600秒 = 10分钟）删减字典
    """
    def run():
        while True:
            time.sleep(interval)
            recent_dict.trim()

    threading.Thread(target = run, daemon = True).start()


if __name__ == "__main__":
    # simulate_stream_processing()
    pass
