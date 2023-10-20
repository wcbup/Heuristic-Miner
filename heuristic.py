from __future__ import annotations
from typing import List, Set, Dict, Union, Tuple
from datetime import datetime
import xml.etree.ElementTree as ET
from copy import copy, deepcopy


class Place:
    def __init__(self, id: int) -> None:
        self.id = id
        self.predecessor_id_set: Set[int] = set()
        self.successor_id_set: Set[int] = set()
        self.token = 0


class Transition:
    def __init__(self, name: str, id: int) -> None:
        self.name = name
        self.id = id
        self.predecessor_id_set: Set[int] = set()
        self.successor_id_set: Set[int] = set()


class PetriNet:
    def __init__(self) -> None:
        self.node_dic: Dict[int, Union[Place, Transition]] = {}
        self.transition_dict: Dict[str, int] = {}

    def add_place(self, id: int) -> PetriNet:
        self.node_dic[id] = Place(id)

        return self

    def add_transition(self, name: str, id: int) -> PetriNet:
        self.node_dic[id] = Transition(name, id)
        self.transition_dict[name] = id
        return self

    def transition_name_to_id(self, name: str) -> int:
        return self.transition_dict[name]

    def add_edge(self, source_id: int, target_id: int) -> PetriNet:
        self.node_dic[source_id].successor_id_set.add(target_id)
        self.node_dic[target_id].predecessor_id_set.add(source_id)
        return self

    def get_tokens(self, place_id: int) -> int:
        return self.node_dic[place_id].token

    def is_enabled(self, transition_id: int) -> bool:
        for place_id in self.node_dic[transition_id].predecessor_id_set:
            if self.node_dic[place_id].token <= 0:
                return False

        return True

    def add_marking(self, place_id: int) -> PetriNet:
        self.node_dic[place_id].token += 1
        return self

    def fire_transition(self, transition_id: int) -> Tuple[int, int, int]:
        """
        return a tuple consisting
        (new_m, new_c, new_p)
        """
        new_m = 0
        new_c = 0
        new_p = 0
        for place_id in self.node_dic[transition_id].predecessor_id_set:
            if self.node_dic[place_id].token <= 0:
                new_m += 1
            else:
                self.node_dic[place_id].token -= 1
            new_c += 1

        for place_id in self.node_dic[transition_id].successor_id_set:
            self.node_dic[place_id].token += 1
            new_p += 1

        return new_m, new_c, new_p


def read_from_file(
    filename: str,
) -> Dict[str, List[Dict[str, Union[int, str, datetime]]]]:
    tree = ET.parse(filename)
    root = tree.getroot()
    result: Dict[str, List[Dict[str, Union[int, str, datetime]]]] = {}

    def parse_event_attribute(
        attribute_node: ET.Element,
    ) -> Tuple[str, Union[str, int, datetime]]:
        key = attribute_node.attrib["key"]
        value_str = attribute_node.attrib["value"]
        match attribute_node.tag:
            case "{http://www.xes-standard.org/}string":
                value = value_str
            case "{http://www.xes-standard.org/}int":
                value = int(value_str)
            case "{http://www.xes-standard.org/}date":
                year_mon_day_str, hour_min_sec_str = value_str.split("T")
                time_strs = year_mon_day_str.split("-")
                year = int(time_strs[0])
                month = int(time_strs[1])
                day = int(time_strs[2])
                time_strs = hour_min_sec_str.split(":")
                hour = int(time_strs[0])
                minute = int(time_strs[1])
                value = datetime(year, month, day, hour, minute)
        return key, value

    for child in root:
        if child.tag == "{http://www.xes-standard.org/}trace":
            for node in child:
                if node.tag == "{http://www.xes-standard.org/}string":
                    case_id = node.attrib["value"]
            if case_id not in result.keys():
                result[case_id] = []
            for event_node in child:
                if event_node.tag == "{http://www.xes-standard.org/}event":
                    event: Dict[str, Union[str, int, datetime]] = {}
                    for attribute_node in event_node:
                        key, value = parse_event_attribute(attribute_node)
                        event[key] = value
                    result[case_id].append(event)

    return result


def dependency_graph_file(
    log: Dict[str, List[Dict[str, Union[int, str, datetime]]]]
) -> Dict[str, Dict[str, int]]:
    result: Dict[str, Dict[str, int]] = {}
    for case_id in log.keys():
        trace = log[case_id]
        for index in range(len(trace) - 1):
            pred_task = trace[index]["concept:name"]
            succ_task = trace[index + 1]["concept:name"]
            if pred_task not in result.keys():
                result[pred_task] = {succ_task: 1}
            else:
                if succ_task not in result[pred_task].keys():
                    result[pred_task][succ_task] = 1
                else:
                    result[pred_task][succ_task] += 1

    return result


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


def fitness_token_replay(
    log: Dict[str, List[Dict[str, Union[int, str, datetime]]]], model: PetriNet
) -> float:
    def get_remained_token_num(model: PetriNet) -> int:
        remained_token_num = 0

        for id in model.node_dic.keys():
            node = model.node_dic[id]
            if isinstance(node, Place):
                if node.token > 0:
                    remained_token_num += node.token

        return remained_token_num

    def consume_end_token(model: PetriNet) -> Tuple[int, int]:
        """
        return (new_m, new_c)
        """
        new_m = 0
        new_c = 0

        for id in model.node_dic.keys():
            node = model.node_dic[id]
            if isinstance(node, Place) and len(node.successor_id_set) == 0:
                new_c += 1
                if node.token <= 0:
                    new_m += 1
                else:
                    node.token -= 1

        return new_m, new_c

    m = 0.0
    r = 0.0
    c = 0.0
    p = 0.0

    for trace_id in log.keys():
        tmp_model = deepcopy(model)

        p += get_remained_token_num(tmp_model)

        for event in log[trace_id]:
            new_m, new_c, new_p = tmp_model.fire_transition(
                tmp_model.transition_name_to_id(event["concept:name"])
            )
            m += new_m
            c += new_c
            p += new_p

        new_m, new_c = consume_end_token(tmp_model)
        m += new_m
        c += new_c
        r += get_remained_token_num(tmp_model)

    # print(m, c, p, r)

    return 0.5 * (1 - m / c) + 0.5 * (1 - r / p)


class Painter:
    def __init__(self) -> None:
        self.dot_code = ""

    def generate_dot_code(self, petriNet: PetriNet) -> None:
        """
        generate the dot code
        save the code into './result.dot'
        """
        self.dot_code = "digraph SourceGra {\n"

        for id in petriNet.node_dic.keys():
            node = petriNet.node_dic[id]
            if isinstance(node, Place):
                self.dot_code += f'x{id} [shape = circle label=" "];\n'
            elif isinstance(node, Transition):
                self.dot_code += f'x{id} [shape = box label="{node.name}"];\n'
            else:
                raise Exception

        for id in petriNet.node_dic.keys():
            node = petriNet.node_dic[id]
            for succ_id in node.successor_id_set:
                self.dot_code += f"x{id} -> x{succ_id};\n"

        self.dot_code += "}"

        dot_file_path = "./result.dot"
        with open(dot_file_path, "w") as f:
            f.write(self.dot_code)


# test code
if __name__ == "__main__":
    # log = read_from_file("example-log.xes")
    # log = read_from_file("extension-log-4.xes")
    log = read_from_file("extension-log-noisy-4.xes")
    # log_noisy = read_from_file("extension-log-noisy-4.xes")
    mined_model = alpha(log)
    # print(round(fitness_token_replay(log, mined_model), 5))
    # print(round(fitness_token_replay(log_noisy, mined_model), 5))

    painter = Painter()
    painter.generate_dot_code(mined_model)

    # tmp_num = 0
    # for id in mined_model.node_dic.keys():
    #     node = mined_model.node_dic[id]
    #     if isinstance(node, Place) and len(node.successor_id_set) == 0:
    #         tmp_num += 1
    # print(tmp_num)
