from constraints_abc import ConstraintsABC
import pyarrow.compute as pq

class Constraints(ConstraintsABC):



    def __init__(self):
        self.direct_types = {'greater': pq.greater,
                             'greater_equal': pq.greater_equal,
                             'less': pq.less,
                             'less_equal': pq.less_equal}

    def greater_equal(self, field, value):
        pass

    def greater(self, field, value):
        pass

    def less_equal(self, field, value):
        pass

    def less(self, field, value):
        pass
