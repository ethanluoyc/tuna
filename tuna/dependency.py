import networkx as nx
from networkx.algorithms.dag import topological_sort
import collections

class Task(object):
    def __init__(self, id):
        self.id = id

    def name(self):
        return str(self.id)
    
    def __hash__(self):
        return hash(self.id)
    
    def __eq__(self, other):
        return self.id == other.id

    def __repr__(self):
        return "Task(id={})".format(self.id)

    def requires(self):
        return []

    def run(self):
        print("Hello {}".format(self.id))

    def output(self):
        pass

class Scheduler(object):
    def _deps(self, task):
        q = collections.deque()
        q.append(task)
        graph = nx.DiGraph()
        graph.add_node(task)
        while len(q) > 0:
            task = q.popleft()
            requires = task.requires()
            for r in requires:
                graph.add_node(r)
                graph.add_edge(task, r)
                q.append(r)
        return list(topological_sort(graph))[::-1]

    def schedule(self, task):
        return self._deps(task)

if __name__ == "__main__":
    class Task1(Task):
        def requires(self):
            return [Task(0)]
    scheduler = Scheduler()
    print(scheduler.schedule(Task1(1)))
