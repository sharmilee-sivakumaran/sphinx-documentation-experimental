from __future__ import absolute_import, division


def build_eval_include_func(filter_expression):
    expression = compile(filter_expression, "filter_expression.py", "eval")

    class Locals(object):
        def __init__(self, row):
            self._row = row

        def __getitem__(self, item):
            return self._row.cells_dict[item].value

    def include_func(row):
        return eval(expression, {}, Locals(row))

    return include_func


def build_filter_include_func(column_names, filter_value):
    def include_func(row):
        for cn in column_names:
            if filter_value.lower() in row.cells_dict[cn].value.lower():
                return True
        return False
    return include_func


def get_fields(default_fields, user_fields):
    fields = list(default_fields)
    if user_fields and "all" in user_fields:
        fields = None
    elif user_fields:
        arg_fields = []
        for f in user_fields:
            if "," in f:
                arg_fields.extend(f.split(","))
            else:
                arg_fields.append(f)
        for f in arg_fields:
            if not f.startswith("-") and not f.startswith("+"):
                fields = []
        for f in arg_fields:
            if not f.startswith("-") and not f.startswith("+"):
                fields.append(f)
        for f in arg_fields:
            if f.startswith("+"):
                fields.append(f[1:])
            if f.startswith("-"):
                fields = filter(lambda x: x != f[1:], fields)
    return fields
