from fields import fields_names


def generate(table):
    """
    Generates random row in table
    :param table:
    :return:
    """
    return tuple(getattr(table, name).get() for name in fields_names(table))


def generator(table):
    return [generate(table) for _ in range(getattr(table, "__cnt__").get())]


def paired_generator(table1, table2, pairing: dict):
    if len(pairing) <= 0:
        raise Exception(f"There are no keys for pairing two tables {table1} and {table2}!")
    if not all(map(lambda key: key in fields_names(table1), pairing.keys())):
        raise Exception(f"There is a key that is mentioned in config but couldn't be found"
                        f" in the first table {table1}!")
    if not all(map(lambda key: key in fields_names(table2), pairing.values())):
        raise Exception(f"There is a key that is mentioned in config but couldn't be found"
                        f" in the second table {table2}!")
    intersect_generators = dict()
    for key1, key2 in pairing.items():
        try:
            intersect_generator = getattr(table1, key1).intersect(getattr(table2, key2))
            if intersect_generator is None:
                raise Exception
        except Exception:
            raise Exception("Can't generate data that matches the fields of both tables. " +
                            f"({key1} in first table, {key2} in second table)")
        intersect_generators[(key1, key2)] = intersect_generator
    cnt = min(getattr(table1, "__cnt__").get(), getattr(table2, "__cnt__").get())
    data1, data2 = [], []
    for _ in range(cnt):
        intersect_values = dict()
        for key1, key2 in pairing.items():
            value = intersect_generators[(key1, key2)].get()
            intersect_values[key1] = intersect_values[key2] = value
        data1.append(tuple(intersect_values[name] if name in pairing.keys() else getattr(table1, name).get()
                           for name in fields_names(table1)))
        data2.append(tuple(intersect_values[name] if name in pairing.values() else getattr(table2, name).get()
                           for name in fields_names(table2)))
    return data1, data2
