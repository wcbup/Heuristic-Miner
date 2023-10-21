from PetriNet import PetriNet
import pm4py
from pybeamline.sources import string_test_source, log_source
from pybeamline.bevent import BEvent
from reactivex import operators
from math import ceil
from typing import Dict, Tuple, Union, Set, List
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


class DrCountingSet:
    def __init__(self) -> None:
        # the first item in the tuple is frequency
        # the second item in the tuple is delta
        self.counting_dict: Dict[str, Dict[str, Tuple[int, int]]] = {}

    def update(self, pred_task: str, succ_task: str, bucket_id: int) -> bool:
        """
        if relation exists, update the frequency automatically
        if not, add new tuple automatically
        """
        if pred_task in self.counting_dict.keys():
            tmp_dict = self.counting_dict[pred_task]
            if succ_task not in tmp_dict:
                tmp_dict[succ_task] = (1, bucket_id - 1)
            else:
                old_tuple = tmp_dict[succ_task]
                tmp_dict[succ_task] = (old_tuple[0] + 1, old_tuple[1])
        else:
            self.counting_dict[pred_task] = {succ_task: (1, bucket_id - 1)}

    def cleanup(self, bucket_id: int) -> None:
        del_list = []

        for pred_task in self.counting_dict.keys():
            tmp_dict = self.counting_dict[pred_task]
            for succ_task in tmp_dict.keys():
                tmp_tuple = tmp_dict[succ_task]
                if tmp_tuple[0] + tmp_tuple[1] <= bucket_id:
                    del_list.append((pred_task, succ_task))

        for pred_task, succ_task in del_list:
            del self.counting_dict[pred_task][succ_task]


class TaskNode:
    def __init__(self, name: str) -> None:
        self.name = name
        self.pred_task_list: List[str] = []
        self.succ_task_list: List[str] = []

    def parse_depend_matrix(
        self, depend_matrix: Dict[str, Dict[str, float]], threshold: float
    ) -> None:
        for pred_task in depend_matrix.keys():
            for succ_task in depend_matrix.keys():
                if (
                    pred_task == self.name
                    and depend_matrix[pred_task][succ_task] >= threshold
                ):
                    self.succ_task_list.append(succ_task)

                if (
                    succ_task == self.name
                    and depend_matrix[pred_task][succ_task] >= threshold
                ):
                    self.pred_task_list.append(pred_task)


class HeuristicMiner:
    def __init__(self, error_epsilon: float, heuristic_threshold: float) -> None:
        self.error_epsilon = error_epsilon
        self.heuristic_threshold = heuristic_threshold

        self.dc_set = DcCountingSet()
        self.dr_set = DrCountingSet()
        self.counter = 1
        self.bucket_size = ceil(1.0 / self.error_epsilon)

        self.task_dict: Dict[str, TaskNode] = {}
        self.depend_matrix: Dict[str, Dict[str, float]] | None = None

    def get_new_event(self, event: BEvent) -> None:
        new_case_id: str = event.get_trace_name()
        new_task_name: str = event.get_event_name()
        bucket_id = ceil(self.counter * 1.0 / self.bucket_size)

        self.task_dict[new_task_name] = TaskNode(new_task_name)

        # update counting set
        last_task_name = self.dc_set.update_case(new_case_id, new_task_name, bucket_id)
        if last_task_name != None:
            self.dr_set.update(last_task_name, new_task_name, bucket_id)

        # cleanup
        if self.counter % self.bucket_size == 0:
            # print("before cleanup")
            # self.print_set()

            self.dc_set.cleanup(bucket_id)
            self.dr_set.cleanup(bucket_id)

            # print("after cleanup")
            # self.print_set()

        self.counter += 1

    def print_set(self) -> None:
        # print("---dc---")
        # for case_id in self.dc_set.counting_dict.keys():
        #     dc_tuple = self.dc_set.counting_dict[case_id]
        #     print(case_id)
        #     print(f" {dc_tuple.task_name} f:{dc_tuple.frequency} {dc_tuple.time_delta}")
        #     print()
        # print()
        print("---dr---")
        for pred_task in self.dr_set.counting_dict.keys():
            tmp_dict = self.dr_set.counting_dict[pred_task]
            print(pred_task)
            for succ_task in tmp_dict.keys():
                tmp_tuple = tmp_dict[succ_task]
                print(f" {succ_task} f:{tmp_tuple[0]} {tmp_tuple[1]}")
                print()

    def generate_petriNet(self) -> PetriNet:
        def get_depend_frequency(pred_task: str, succ_task: str) -> int:
            nonlocal self
            dr_dict = self.dr_set.counting_dict
            if pred_task not in dr_dict.keys():
                return 0
            elif succ_task not in dr_dict[pred_task].keys():
                return 0
            else:
                return dr_dict[pred_task][succ_task][0]

        self.depend_matrix = {}
        # get dependency matrix
        for pred_task in self.task_dict.keys():
            self.depend_matrix[pred_task] = {}
            for succ_task in self.task_dict.keys():
                if pred_task != succ_task:
                    pred2succ = get_depend_frequency(pred_task, succ_task)
                    succ2pred = get_depend_frequency(succ_task, pred_task)
                    self.depend_matrix[pred_task][succ_task] = (
                        pred2succ - succ2pred
                    ) / (pred2succ + succ2pred + 1)
                else:
                    tmp = get_depend_frequency(pred_task, succ_task)
                    self.depend_matrix[pred_task][succ_task] = tmp / (tmp + 1)

        # for pred_task in self.task_set:
        #     print(pred_task)
        #     for succ_task in self.task_set:
        #         print(f" {succ_task} {self.depend_matrix[pred_task][succ_task]}")


# test code
if __name__ == "__main__":
    # b_events = log_source("ExampleLog.xes")

    traces_list = []

    def add_traces(traces_list: List[str], new_trace: str, frequency: int) -> None:
        for i in range(frequency):
            traces_list.append(new_trace)

    add_traces(traces_list, "AE", 5)
    add_traces(traces_list, "ABCE", 10)
    add_traces(traces_list, "ACBE", 10)
    add_traces(traces_list, "ABE", 1)
    add_traces(traces_list, "ACE", 1)
    add_traces(traces_list, "ADE", 10)
    add_traces(traces_list, "ADDE", 2)
    add_traces(traces_list, "ADDDE", 1)
    b_events = log_source(traces_list)

    # b_events = b_events.pipe(operators.take(5))

    miner = HeuristicMiner(0.00005, 0.8)
    b_events.subscribe(lambda x: miner.get_new_event(x))

    # for pred_task in miner.dr_set.counting_dict.keys():
    #     tmp_dict = miner.dr_set.counting_dict[pred_task]
    #     print(pred_task)
    #     for succ_task in tmp_dict:
    #         print(f" {succ_task} {tmp_dict[succ_task]}")
    #     print()

    miner.print_set()
    # print(miner.counter)
    miner.generate_petriNet()
