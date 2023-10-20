from PetriNet import PetriNet
from pybeamline.sources import string_test_source, log_source
from pybeamline.bevent import BEvent
from reactivex import operators
from math import ceil
from typing import Dict, Tuple, Union
import warnings

warnings.filterwarnings("ignore")


class DcTuple:
    def __init__(self, case_id: str, task_name: str, time_delta: int) -> None:
        self.case_id = case_id
        self.task_name = task_name
        self.frequency = 1
        self.time_delta = time_delta


class DcCountingSet:
    def __init__(self) -> None:
        self.counting_dict: Dict[str, DcTuple] = {}

    def update_case(
        self, case_id: str, task_name: str, bucket_id: int
    ) -> Union[str, None]:
        """
        if case exists, add frequency automatically, return last task name
        if case doesn't exist, add new tuple automatically, return None
        """
        if case_id in self.counting_dict.keys():
            dc_tuple = self.counting_dict[case_id]
            last_task_name = dc_tuple.task_name

            dc_tuple.task_name = task_name
            dc_tuple.frequency += 1

            return last_task_name
        else:
            self.counting_dict[case_id] = DcTuple(case_id, task_name, bucket_id - 1)

    def cleanup(self, bucket_id: int) -> None:
        del_id_list = []
        for case_id in self.counting_dict.keys():
            dc_tuple = self.counting_dict[case_id]
            if dc_tuple.frequency + dc_tuple.time_delta <= bucket_id:
                del_id_list.append(case_id)

        for id in del_id_list:
            del self.counting_dict[id]


class DrTuple:
    def __init__(self, task_relation: Tuple[str, str], time_delta: int) -> None:
        self.task_relation = task_relation
        self.frequency = 1
        self.time_delta = time_delta


class DrCountingSet:
    def __init__(self) -> None:
        self.counting_dict: Dict[Tuple[str, str], DrTuple] = {}

    def update(self, task_relation: Tuple[str, str], bucket_id: int) -> bool:
        """
        if relation exists, update the frequency automatically
        if not, add new tuple automatically
        """
        if task_relation in self.counting_dict.keys():
            self.counting_dict[task_relation].frequency += 1
            return True
        else:
            self.counting_dict[task_relation] = DrTuple(task_relation, bucket_id - 1)
            return False

    def cleanup(self, bucket_id: int) -> None:
        del_list = []

        for relation in self.counting_dict:
            dr_tuple = self.counting_dict[relation]
            if dr_tuple.frequency + dr_tuple.time_delta <= bucket_id:
                del_list.append(relation)

        for id in del_list:
            del self.counting_dict[id]


class HeuristicMiner:
    def __init__(self, error_epsilon: float, heuristic_threshold: float) -> None:
        self.error_epsilon = error_epsilon
        self.heuristic_threshold = heuristic_threshold

        self.dc_set = DcCountingSet()
        self.dr_set = DrCountingSet()
        self.counter = 1
        self.bucket_size = ceil(1.0 / self.error_epsilon)

    def get_new_event(self, event: BEvent) -> None:
        new_case_id: str = event.get_trace_name()
        new_task_name: str = event.get_event_name()
        bucket_id = ceil(self.counter * 1.0 / self.bucket_size)

        # update counting set
        last_task_name = self.dc_set.update_case(new_case_id, new_task_name, bucket_id)
        if last_task_name != None:
            self.dr_set.update((last_task_name, new_task_name), bucket_id)

        # cleanup
        if self.counter % self.bucket_size == 0:
            self.dc_set.cleanup(bucket_id)
            self.dr_set.cleanup(bucket_id)
        
        self.counter += 1


# test code
if __name__ == "__main__":
    b_events = log_source("extension-log-4.xes")
    # b_events = b_events.pipe(operators.take(5))

    miner = HeuristicMiner(0.000000000001, 0.8)
    b_events.subscribe(lambda x: miner.get_new_event(x))

    # for relation in miner.dr_set.counting_dict.keys():
    #     print(relation, miner.dr_set.counting_dict[relation].frequency)