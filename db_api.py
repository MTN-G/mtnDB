from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Type
import json
import os

from dataclasses_json import dataclass_json

LOCAL_PATH = os.getcwd()
DB_ROOT = Path('db_files')
print(LOCAL_PATH)


@dataclass_json
@dataclass
class DBField:
    name: str
    type: Type


@dataclass_json
@dataclass
class SelectionCriteria:
    field_name: str
    operator: str
    value: Any


@dataclass_json
@dataclass
class DBTable:
    name: str
    fields: List[DBField]
    key_field_name: str
    path: str

    def count(self) -> int:
        data = json.load(open(self.path, "r"))
        return len(data)

    def insert_record(self, values: Dict[str, Any]) -> None:
        new_record = {}
        for field in self.fields:
            print(field)
            key = field["name"]
            new_record[key] = values[key]
        table = json.load(open(self.path, 'r'))
        try:
            new_record["PK"] = table[len(table) - 1]["PK"] + 1
        except:
            new_record["PK"] = 1
        table.append(new_record)
        json.dump(table, open(self.path, 'w'))

    def delete_record(self, key: Any) -> None:
        table = json.load(open(self.path, 'r'))

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        raise NotImplementedError

    def get_record(self, key: Any) -> Dict[str, Any]:
        raise NotImplementedError

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        raise NotImplementedError

    def query_table(self, criteria: List[SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        raise NotImplementedError

    def create_index(self, field_to_index: str) -> None:
        raise NotImplementedError


@dataclass_json
@dataclass
class DataBase():
    # name: str
    # tables: List[DBTable]
    # path: str

    def __init__(self, db_name):
        self.db_name = db_name
        self.tables = []
        self.path = f"{LOCAL_PATH}/{db_name}"
        try:
            if not os.path.exists(self.path):
                os.mkdir(self.path)
                self.tables = []
                json.dump([], open(f"{self.path}/config.json", 'w+'))
            else:
                config = json.load(open(f"{self.path}/config.json", 'r'))
                convert_config = map(lambda table: DBTable(
                    table["table_name"], table["fields"], table["key_field_name"], table["path"]), config)
                self.tables = list(convert_config)

        except OSError:
            print("Creation of the directory %s failed" % db_name)

    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:
        try:
            table_json = f"{self.path}/{table_name}.json"

            fields_config = []
            for field in fields:
                fields_config.append(
                    {"name": field.name, "type": str(field.type)})

            json.dump([], open(table_json, 'w+'))

            config = json.load(open(f"{self.path}/config.json", 'r'))
            config.append({"table_name": table_name, "fields": fields_config,
                           "key_field_name": key_field_name, "path": table_json})
            json.dump(config, open(f"{self.path}/config.json", 'w+'))

            new_table = DBTable(table_name, fields, key_field_name, table_json)
            self.tables.append(new_table)

            return new_table

        except Exception as e:
            print(e)

    def num_tables(self) -> int:
        return len(self.tables)

    def get_table(self, table_name: str) -> DBTable:
        for table in self.tables:
            if table.name == table_name:
                print(table)
                return table
        return f"{table_name} not exist"

    def delete_table(self, table_name: str) -> None:
        try:
            config = json.load(open(f"{self.path}/config.json", 'r'))
            for table in config:
                if table["table_name"] == table_name:
                    config.remove(table)
            json.dump(config, open(f"{self.path}/config.json", 'w+'))
            table_to_remove = self.get_table(table_name)
            self.tables.remove(table_to_remove)
        except Exception as e:
            print(e)

    def get_tables_names(self) -> List[Any]:
        tables_names = map(lambda x: x.name, self.tables)
        return list(tables_names)

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError


def test():
    db = DataBase("test")
    id = DBField("id", int)
    name = DBField("name", str)
    x = db.get_table('mentors')
    x.insert_record({"id": 3453, "name": 'mat2423ans'})
    # print(x.count())


test()
