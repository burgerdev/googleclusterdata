
import os
import csv
from collections import defaultdict

from config import config


_basedir = config.get("clusterdata", "root")

with open(os.path.join(_basedir, "schema.csv"), 'r') as _schemafile:
    _reader = csv.reader(_schemafile)
    _reader.next()
    _content = list(_reader)


_grouped = defaultdict(list)
for item in _content:
    key = item[0].split('/')[0]
    _grouped[key].append(item)

_table_names = _grouped.keys()


def get_table_names():
    return _table_names


def get_valid_tables():
    return filter(lambda t: t.valid, map(Table, _table_names))


class Table(object):
    """
    TODO doc
    """

    @property
    def name(self):
        return self._name

    @property
    def valid(self):
        return all(map(lambda c: c.valid, self._cols))

    def __init__(self, table_name):
        assert table_name in _table_names,\
            "unknown table {}".format(table_name)
        self._name = table_name
        self._cols = map(Column, _grouped[table_name])

    def get_columns(self):
        return self._cols

    def get_glob(self):
        assert len(self._cols) > 0,\
            "this table does not hold any columns"
        g = self._cols[0].get_glob()
        return os.path.join(_basedir, self._name, g)

    def prepare(self, row):
        """
        @param row iterable of strings
        @return iterable of the correct types for each column
        """
        out = [c.prepare(v) for c, v in zip(self._cols, row)]
        return out

    def __len__(self):
        return len(self._cols)

    def __getitem__(self, i):
        return self._cols[i]

    def __iter__(self):
        for c in self._cols:
            yield c


class Column(object):
    """
    TODO doc
    """

    @property
    def valid(self):
        try:
            self._extract_type(self._row)
        except ValueError:
            return False
        else:
            return True

    def __init__(self, row):
        self._row = row

    def describe(self):
        return self._describe_single_col(self._row)

    def get_glob(self):
        return self._row[0].split('/')[1]

    def prepare(self, v):
        if len(v) == 0:
            return None
        t = self._extract_type(self._row)
        if t == "BIGINT":
            return int(v)
        if t == "FLOAT":
            return float(v)
        if t == "BOOLEAN":
            return bool(v)
        return v

    @staticmethod
    def _type_map(t):
        if t == "STRING_HASH":
            return "VARCHAR(512)"
        if t == "STRING_HASH_OR_INTEGER":
            raise ValueError("Don't know how to interpret that")
        if t == "INTEGER":
            # some values in the cluster data are bigger than 2*32
            return "BIGINT"
        return t

    @staticmethod
    def _extract_index(row):
        return int(row[1])

    @staticmethod
    def _extract_type(row):
        return Column._type_map(row[3])

    @staticmethod
    def _extract_mandatory(row):
        return row[4] == "YES"

    @staticmethod
    def _extract_col_name(row):
        repl = [(' ', '_'),
                ("user", "username"),
                ("/", "")]
        s = row[2]
        for a, b in repl:
            s = s.replace(a, b)
        return s

    @staticmethod
    def _describe_single_col(row):
        d = dict()
        d["name"] = Column._extract_col_name(row)
        d["type"] = Column._extract_type(row)

        d["extras"] = ""

        if Column._extract_mandatory(row):
            d["extras"] += " NOT NULL"

        return "{name} {type}{extras}".format(**d)
