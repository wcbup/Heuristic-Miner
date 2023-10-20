from __future__ import annotations
from typing import List, Set, Dict, Union, Tuple
from datetime import datetime
import xml.etree.ElementTree as ET
from copy import copy, deepcopy
from PetriNet import *

def alpha(log: Dict[str, List[Dict[str, Union[int, str, datetime]]]]) -> PetriNet:
    dependency_graph = dependency_graph_file(log)

    for pred in dependency_graph.keys():
        print(pred)
        for succ in dependency_graph[pred]:
            print(f" {succ} {dependency_graph[pred][succ]}")
        print()
        

    task_set: Set[str] = set()
    for pred_task in dependency_graph.keys():
        if pred_task not in task_set:
            task_set.add(pred_task)
        for succ_task in dependency_graph[pred_task].keys():
            if succ_task not in task_set:
                task_set.add(succ_task)

    # print(task_set)

    def check_direct_succ(pred_task: str, succ_task: str) -> bool:
        nonlocal dependency_graph
        if pred_task not in dependency_graph.keys():
            return False
        elif succ_task not in dependency_graph[pred_task].keys():
            return False
        else:
            return True

    def check_causality(pred_task: str, succ_task: str) -> bool:
        nonlocal check_direct_succ
        return check_direct_succ(pred_task, succ_task) and not check_direct_succ(
            succ_task, pred_task
        )

    first_task_set: Set[str] = copy(task_set)
    for pred_task in dependency_graph.keys():
        for succ_task in dependency_graph[pred_task].keys():
            if succ_task in first_task_set:
                first_task_set.remove(succ_task)

    # print(first_task_set)

    last_task_set: Set[str] = copy(task_set)
    for pred_task in dependency_graph.keys():
        if pred_task in last_task_set:
            last_task_set.remove(pred_task)

    # print(last_task_set)

    xw_list: List[Tuple[Set[str], Set[str]]] = []

    for pred_task in dependency_graph.keys():
        for succ_task in dependency_graph[pred_task].keys():
            if check_causality(pred_task, succ_task):
                xw_list.append((set([pred_task]), set([succ_task])))

    def merge_set(
        xw_list: List[Tuple[Set[str], Set[str]]]
    ) -> Tuple[bool, List[Tuple[Set[str], Set[str]]]]:
        """
        only merge two sets at one time
        return true if merge success
        """
        nonlocal check_direct_succ

        def check_set_valid(new_set: Set[str]) -> bool:
            nonlocal check_direct_succ
            for pred_task in new_set:
                for succ_task in new_set:
                    if pred_task == succ_task:
                        continue
                    if check_direct_succ(pred_task, succ_task):
                        return False
            return True

        result = copy(xw_list)
        for a_pred_set, a_succ_set in xw_list:
            for b_pred_set, b_succ_set in xw_list:
                if a_pred_set == b_pred_set:
                    if a_succ_set == b_succ_set:
                        continue
                    else:
                        new_succ_set = a_succ_set.union(b_succ_set)
                        if check_set_valid(new_succ_set):
                            result.remove((a_pred_set, a_succ_set))
                            result.remove((b_pred_set, b_succ_set))
                            result.append((a_pred_set, new_succ_set))
                            return True, result
                elif a_succ_set == b_succ_set:
                    new_pred_set = a_pred_set.union(b_pred_set)
                    if check_set_valid(new_pred_set):
                        result.remove((a_pred_set, a_succ_set))
                        result.remove((b_pred_set, b_succ_set))
                        result.append((new_pred_set, a_succ_set))
                        return True, result

        return False, result

    flag, xw_list = merge_set(xw_list)
    while flag:
        flag, xw_list = merge_set(xw_list)

    # print(xw_list)

    class IdGenerator:
        def __init__(self) -> None:
            self.id = 0

        def get_new_id(self) -> int:
            self.id += 1
            return self.id

    id_generator = IdGenerator()
    result_petri_net: PetriNet = PetriNet()
    for task in task_set:
        result_petri_net.add_transition(task, id_generator.get_new_id())

    for pred_set, succ_set in xw_list:
        place_id = id_generator.get_new_id()
        result_petri_net.add_place(place_id)
        for pred_name in pred_set:
            pred_id = result_petri_net.transition_name_to_id(pred_name)
            result_petri_net.add_edge(pred_id, place_id)
        for succ_name in succ_set:
            succ_id = result_petri_net.transition_name_to_id(succ_name)
            result_petri_net.add_edge(place_id, succ_id)

    for last_name in last_task_set:
        place_id = id_generator.get_new_id()
        result_petri_net.add_place(place_id)
        last_id = result_petri_net.transition_name_to_id(last_name)
        result_petri_net.add_edge(last_id, place_id)

    for first_name in first_task_set:
        place_id = id_generator.get_new_id()
        result_petri_net.add_place(place_id)
        first_id = result_petri_net.transition_name_to_id(first_name)
        result_petri_net.add_edge(place_id, first_id)
        result_petri_net.add_marking(place_id)

    # for pred_task in dependency_graph.keys():
    #     for succ_task in dependency_graph[pred_task].keys():
    #         print(
    #             f"{pred_task} -> {succ_task}: {dependency_graph[pred_task][succ_task]}"
    #         )

    # print("tasks:")
    # for task in task_set:
    #     print(task)

    # for pred_set, succ_set in xw_list:
    #     print(pred_set, succ_set)

    # print(len(first_task_set))
    # print(len(last_task_set))

    return result_petri_net