from constraints_abc import ConstraintsABC
import pyarrow as pa
import pyarrow.compute as pc

class Constraints(ConstraintsABC):



    def __init__(self):
        self.direct_types = {'greater': pc.greater,
                             'greater_equal': pc.greater_equal,
                             'less': pc.less,
                             'less_equal': pc.less_equal,
                             'allowed_values': pc.is_in}

    def check_constraint(self, pa_column, constraint):
        if constraint.enabled != True:
            return None
        cons_name = constraint.name
        cons_func = self.direct_types(cons_name)
        for level, value in constraint.values.items():
            ret = cons_func(pa_column, value)
            # print(f'{cons_name} {level}, {len(ret)}')



direct_types = {'greater': pc.greater, 'greater_equal': pc.greater_equal, 'less': pc.less,
                'less_equal': pc.less_equal, 'allowed_values': pc.is_in}


def check_constraint(column, constraint):
    # print('constraint ', constraint)
    if constraint.enabled != True:
        # print('constraint disabled')
        return None
    cons_name = constraint.name
    cons_func = direct_types[cons_name]
    cons_level = 0
    for level, value in constraint.values.items():
        # print('type ', pa_column.type)
        if value in ['', None]:
            continue
        val = convert_test_value(column.type, value)
        match cons_name:
            case 'allowed_values':  # Case where function has different signature
                ret = allowed_values(cons_func, column, value)
            case _:
                ret = cons_func(column, val)
        ret = ret.value_counts().to_pylist()
        out = {'level': level, 'cons_name': cons_name, 'value': value}
        out.update(parse_results(ret, len(column)))
        if out['status'] == 'Fail':
            if level == 'warning':
                cons_level = max(cons_level, 1)
            if level == 'error':
                cons_level = max(cons_level, 2)
        # print(f'{level}: {cons_name}, {ret}')
        # print(out)
    return cons_level, out

def parse_results(value_counts, len_col):
    true, false, none = 0, 0, 0
    for r in value_counts:
        if r['values'] == True:
            true = r['counts']
        if r['values'] == False:
            false = r['counts']
        if r['values'] == None:
            none = r['counts']
    total = len_col - none
    if true == total:
        out = {'status': 'Pass'}
    else:
        out = {'status': 'Fail', 'count': false, 'percent': float(false) / float(total) * 100.}
    return out


def convert_test_value(col_type, value):
    # Convert json strings to the same type as the column to be checked
    match col_type:
        case col_type if pa.types.is_integer(col_type):
            val = pc.cast([value], col_type)[0]
        case col_type if pa.types.is_floating(col_type):
            val = pc.cast([value], col_type)[0]
        case col_type if pa.types.is_string(col_type):
            val = value
        case col_type if pa.types.is_timestamp(col_type):
            val = pc.strptime(value, format='%Y-%m-%dT%H:%M:%S', unit=col_type.unit)
        case _:
            value
    return val

#

def allowed_values(cons_func, pa_column, value):
    val = pa.array(value.split(','))
    return cons_func(pa_column, value_set=val)
