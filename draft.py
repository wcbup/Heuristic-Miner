print(["a"*2])

class Bulabula:
    def __init__(self, w) -> None:
        self.a = w
    

a = Bulabula(1)
b = Bulabula(2)
c = Bulabula(2)

a_set = set()
a_set.add(a)
a_set.add(b)
b_set = set()
b_set.add(b)
b_set.add(a)
print(a_set == b_set)

a = (set(), set())
print(a)
a[0].add(1)
print(a)