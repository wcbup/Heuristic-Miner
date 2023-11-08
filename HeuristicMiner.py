from __future__ import annotations
from PetriNet import PetriNet
import pm4py
from Painter import Painter
from pybeamline.sources import string_test_source, log_source
from pybeamline.bevent import BEvent
from reactivex import operators
from math import ceil
from typing import Dict, Tuple, Union, Set, List, Callable
from copy import copy
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
        self.pred_task_set: Set[TaskNode] = set()
        self.succ_task_set: Set[TaskNode] = set()

    def parse_depend_matrix(
        self,
        depend_matrix: Dict[str, Dict[str, float]],
        threshold: float,
        task_dict: Dict[str, TaskNode],
    ) -> None:
        self.pred_task_set: Set[TaskNode] = set()
        self.succ_task_set: Set[TaskNode] = set()
        for pred_task in depend_matrix.keys():
            for succ_task in depend_matrix.keys():
                if (
                    pred_task == self.name
                    and depend_matrix[pred_task][succ_task] >= threshold
                ):
                    self.succ_task_set.add(task_dict[succ_task])

                if (
                    succ_task == self.name
                    and depend_matrix[pred_task][succ_task] >= threshold
                ):
                    self.pred_task_set.add(task_dict[pred_task])


class XOR_Relation:
    def __init__(self, task_dict: Dict[str, TaskNode]) -> None:
        self.relation_set_list: List[Tuple[Set[TaskNode], Set[TaskNode]]] = []
        for pred_task in task_dict.values():
            for succ_task in pred_task.succ_task_set:
                self.relation_set_list.append((set([pred_task]), set([succ_task])))

    def extend_one_relation(
        self, threshold: float, get_dep_fre: Callable[[str, str], int]
    ) -> bool:
        """
        extend one relation set
        if success, return true
        else return false
        """

        def is_new_pred_valid(
            pred_task_set: Set[TaskNode],
            succ_task_set: Set[TaskNode],
            new_pred_task: TaskNode,
        ) -> bool:
            nonlocal get_dep_fre
            nonlocal threshold

            def is_ab_c_xor(a: TaskNode, b: TaskNode, c: TaskNode) -> bool:
                """
                is a xor b -> c valid?
                """
                nonlocal get_dep_fre
                nonlocal threshold
                tmp = (get_dep_fre(a.name, b.name) + get_dep_fre(b.name, a.name)) / (
                    get_dep_fre(a.name, c.name) + get_dep_fre(b.name, c.name) + 1
                )
                if tmp < threshold:
                    return True
                else:
                    return False

            for pred_task in pred_task_set:
                for succ_task in succ_task_set:
                    if not is_ab_c_xor(pred_task, new_pred_task, succ_task):
                        return False

            return True

        def is_new_succ_valid(
            pred_task_set: Set[TaskNode],
            succ_task_set: Set[TaskNode],
            new_succ_task: TaskNode,
        ) -> bool:
            nonlocal get_dep_fre
            nonlocal threshold

            def is_a_bc_xor(a: TaskNode, b: TaskNode, c: TaskNode) -> bool:
                """
                is a -> b xor b valid?
                """
                nonlocal get_dep_fre
                nonlocal threshold
                tmp = (get_dep_fre(b.name, c.name) + get_dep_fre(c.name, b.name)) / (
                    get_dep_fre(a.name, b.name) + get_dep_fre(a.name, c.name) + 1
                )
                if tmp < threshold:
                    return True
                else:
                    return False

            for pred_task in pred_task_set:
                for succ_task in succ_task_set:
                    if not is_a_bc_xor(pred_task, succ_task, new_succ_task):
                        return False

            return True

        for relation in self.relation_set_list:
            pred_task_set = relation[0]
            succ_task_set = relation[1]

            # print(f"{[x.name for x in pred_task_set], [x.name for x in succ_task_set]}")

            common_succ_set: Set[TaskNode] = set()
            for pred_task in pred_task_set:
                tmp_succ_set = pred_task.succ_task_set
                if common_succ_set == set():
                    common_succ_set = copy(tmp_succ_set)
                else:
                    common_succ_set = common_succ_set.intersection(tmp_succ_set)

            common_succ_set -= succ_task_set

            for succ_task in common_succ_set:
                if is_new_succ_valid(pred_task_set, succ_task_set, succ_task):
                    succ_task_set.add(succ_task)
                    return True

            common_pred_set: Set[TaskNode] = set()
            for succ_task in succ_task_set:
                tmp_pred_set = succ_task.pred_task_set
                if common_pred_set == set():
                    common_pred_set = copy(tmp_pred_set)
                else:
                    common_pred_set = common_pred_set.intersection(tmp_pred_set)

            common_pred_set -= pred_task_set

            for pred_task in common_pred_set:
                if is_new_pred_valid(pred_task_set, succ_task_set, pred_task):
                    pred_task_set.add(pred_task)
                    return True

        return False

    def print(self) -> None:
        for relation in self.relation_set_list:
            print(f"{[x.name for x in relation[0]], [x.name for x in relation[1]]}")

    def remove_common_relation(self) -> None:
        flag = True
        while flag:
            flag = False
            relation_len = len(self.relation_set_list)
            for i in range(relation_len):
                for j in range(relation_len):
                    if i == j:
                        continue
                    if self.relation_set_list[i] == self.relation_set_list[j]:
                        flag = True
                        del self.relation_set_list[j]
                        break
                if flag == True:
                    break


class IdGenerator:
    def __init__(self) -> None:
        self.index = 0

    def get_new_index(self) -> int:
        self.index += 1
        return self.index


class HeuristicMiner:
    def __init__(
        self, error_epsilon: float, depend_threshold: float, xor_threshold: float
    ) -> None:
        self.error_epsilon = error_epsilon
        self.depend_threshold = depend_threshold
        self.xor_threshold = xor_threshold

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

        if new_task_name not in self.task_dict.keys():
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
        if self.counter % 5000 == 0:
            tmp_petriNet = self.generate_petriNet()
            tmp_painter = Painter()
            tmp_painter.generate_dot_code(tmp_petriNet)
            tmp_painter.generate_graph_show(False)

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

    def print_tasks(self) -> None:
        for task_name in self.task_dict.keys():
            print(task_name)
            print(f" pred: {[x.name for x in self.task_dict[task_name].pred_task_set]}")
            print(f" succ: {[x.name for x in self.task_dict[task_name].succ_task_set]}")
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

        for pred_task in self.task_dict.keys():
            print(pred_task)
            for succ_task in self.task_dict.keys():
                print(f" {succ_task} {self.depend_matrix[pred_task][succ_task]}")

        for task_name in self.task_dict.keys():
            self.task_dict[task_name].parse_depend_matrix(
                self.depend_matrix, self.depend_threshold, self.task_dict
            )

        # self.print_tasks()

        xor_relations = XOR_Relation(self.task_dict)
        # xor_relations.print()
        # print()
        while xor_relations.extend_one_relation(
            self.xor_threshold, get_depend_frequency
        ):
            None
            # xor_relations.print()
            # print()

        self.print_tasks()

        xor_relations.remove_common_relation()

        # xor_relations.print()

        petriNet = PetriNet()
        id_generator = IdGenerator()
        for task_name in self.task_dict.keys():
            petriNet.add_transition(task_name, id_generator.get_new_index())

        for relation in xor_relations.relation_set_list:
            pred_task_set = relation[0]
            succ_task_set = relation[1]

            tmp_place_id = id_generator.get_new_index()
            petriNet.add_place(tmp_place_id)
            for pred_task in pred_task_set:
                petriNet.add_edge(
                    petriNet.transition_name_to_id(pred_task.name), tmp_place_id
                )
            for succ_task in succ_task_set:
                petriNet.add_edge(
                    tmp_place_id, petriNet.transition_name_to_id(succ_task.name)
                )

        first_task_set: Set[TaskNode] = set()
        last_task_set: Set[TaskNode] = set()
        for task in self.task_dict.values():
            if task.pred_task_set == set():
                first_task_set.add(task)
            if task.succ_task_set == set():
                last_task_set.add(task)

        # print(f"{[x.name for x in first_task_set]}")
        # print(f"{[x.name for x in last_task_set]}")

        if first_task_set != set():
            start_place_id = id_generator.get_new_index()
            petriNet.add_place(start_place_id)
            petriNet.add_marking(start_place_id)
            for task in first_task_set:
                petriNet.add_edge(start_place_id, petriNet.transition_name_to_id(task.name))

        if last_task_set != set():
            end_place_id = id_generator.get_new_index()
            petriNet.add_place(end_place_id)
            for task in last_task_set:
                petriNet.add_edge(petriNet.transition_name_to_id(task.name), end_place_id)

        return petriNet


# test code
if __name__ == "__main__":
    # b_events = log_source("ExampleLog.xes")
    b_events = log_source("extension-log-noisy-4.xes")

    traces_list = []

    def add_traces(traces_list: List[str], new_trace: str, frequency: int) -> None:
        for i in range(frequency):
            traces_list.append(new_trace)

    # add_traces(traces_list, "AE", 5)
    # add_traces(traces_list, "ABCE", 10)
    # add_traces(traces_list, "ACBE", 10)
    # add_traces(traces_list, "ABE", 1)
    # add_traces(traces_list, "ACE", 1)
    # add_traces(traces_list, "ADE", 10)
    # add_traces(traces_list, "ADDE", 2)
    # add_traces(traces_list, "ADDDE", 1)

    # add_traces(traces_list, "ABCD", 9)
    # add_traces(traces_list, "ACBD", 9)
    # add_traces(traces_list, "AED", 9)
    # add_traces(traces_list, "ABCED", 1)
    # add_traces(traces_list, "AECBD", 1)
    # add_traces(traces_list, "AD", 1)

    # add_traces(traces_list, "ABCD", 3000)
    # add_traces(traces_list, "ACBD", 2000)
    # add_traces(traces_list, "AED", 2000)

    # add_traces(traces_list, "ACD", 2000)
    # add_traces(traces_list, "BCE", 2000)

    # b_events = log_source(traces_list)

    # b_events = b_events.pipe(operators.take(5))

    miner = HeuristicMiner(0.000000002, 0.9605, 0.8)
    b_events.subscribe(lambda x: miner.get_new_event(x))

    # for pred_task in miner.dr_set.counting_dict.keys():
    #     tmp_dict = miner.dr_set.counting_dict[pred_task]
    #     print(pred_task)
    #     for succ_task in tmp_dict:
    #         print(f" {succ_task} {tmp_dict[succ_task]}")
    #     print()

    miner.print_set()
    # print(miner.counter)
    petriNet = miner.generate_petriNet()
    painter = Painter()
    painter.generate_dot_code(petriNet)
    painter.generate_graph_show(False)
    with open("./tmp.json", "w") as f:
        f.write(petriNet.generate_json())
