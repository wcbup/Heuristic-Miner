from __future__ import annotations
from typing import List, Set, Dict, Union, Tuple
from datetime import datetime
import xml.etree.ElementTree as ET
from copy import copy, deepcopy
import json


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

    def generate_json(self) -> str:
        info_dict: List[Dict[str, List[str | List[str]]]] = []

        for node in self.node_dic.values():
            node_dict = {}
            if isinstance(node, Place):
                node_dict["type"] = "place"
                node_dict["name"] = str(node.id)
                node_dict["successor"] = [
                    self.node_dic[i].name for i in node.successor_id_set
                ]

            elif isinstance(node, Transition):
                node_dict["type"] = "transition"
                node_dict["name"] = node.name
                node_dict["successor"] = [
                    str(self.node_dic[i].id) for i in node.successor_id_set
                ]
            else:
                raise Exception
            info_dict.append(node_dict)
        return json.dumps(info_dict)


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
