from PetriNet import *

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