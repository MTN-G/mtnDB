from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Type
import json
import os

from dataclasses_json import dataclass_json

LOCAL_PATH = os.getcwd()
DB_ROOT = Path("db_files")
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
            try:
                key = field["name"]
            except:
                key = field.name
            new_record[key] = values[key]
        table = json.load(open(self.path, "r"))
        all_keys = list(map(lambda row: row[self.key_field_name], table))
        if new_record[self.key_field_name] not in all_keys:
            table.append(new_record)
            json.dump(table, open(self.path, "w"))
        else:
            print("invalid primary key")

    def delete_record(self, key: Any) -> None:
        table = json.load(open(self.path, "r"))
        for row in table:
            if row[self.key_field_name] == key:
                table.remove(row)
                print(f"{row[self.key_field_name]} deleted")
                break
        json.dump(table, open(self.path, "w"))

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        table = json.load(open(self.path, "r"))
        row_to_delete = []
        for row in table:
            for crit in criteria:
                print(f"'{row[crit.field_name]}'{crit.operator}'{crit.value}'")
                if eval(f"'{row[crit.field_name]}'{crit.operator}'{crit.value}'"):
                    row_to_delete.append(row)
                    print(f"{row[self.key_field_name]} deleted")
                    break
        for row in row_to_delete:
            table.remove(row)
        json.dump(table, open(self.path, "w"))

    def get_record(self, key: Any) -> Dict[str, Any]:
        table = json.load(open(self.path, "r"))
        for row in table:
            if row[self.key_field_name] == key:
                print(row)
                return row

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        table = json.load(open(self.path, "r"))
        for row in table:
            if row[self.key_field_name] == key:
                for key, value in values.items():
                    row[key] = value
                    print(row)
                print(f"{row[self.key_field_name]} updated")
        json.dump(table, open(self.path, "w"))

    #  AND
    def query_table(self, criteria: List[SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        table = json.load(open(self.path, "r"))
        response = []
        for row in table:
            for crit in criteria:
                if not eval(f"'{row[crit.field_name]}'{crit.operator}'{crit.value}'"):
                    if row in response:
                        response.remove(row)
                    break
                else:
                    if row not in response:
                        response.append(row)
        print(response)
        return response

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
                json.dump([], open(f"{self.path}/config.json", "w+"))
            else:
                config = json.load(open(f"{self.path}/config.json", "r"))
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

            json.dump([], open(table_json, "w+"))

            config = json.load(open(f"{self.path}/config.json", "r"))
            config.append({"table_name": table_name, "fields": fields_config,
                           "key_field_name": key_field_name, "path": table_json})
            json.dump(config, open(f"{self.path}/config.json", "w+"))

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
            config = json.load(open(f"{self.path}/config.json", "r"))
            for table in config:
                if table["table_name"] == table_name:
                    config.remove(table)
            json.dump(config, open(f"{self.path}/config.json", "w+"))
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
    animal = DBField("animal", str)
    arms = DBField("arms", int)
    # db.create_table("mentors", [id, name, animal], "id")
    db.create_table("students", [id, name, arms], "id")
    x = db.get_table("mentors")
    x.insert_record({"id": 1, "name": "matan", "animal": "dog"})
    x.insert_record({"id": 2, "name": "omer", "animal": "panda"})
    x.insert_record({"id": 3, "name": "ran", "animal": "cat"})
    x.insert_record({"id": 4, "name": "stav", "animal": "monkey"})
    x.insert_record({"id": 5, "name": "itamar", "animal": "cat"})
    x.get_record(2)
    print(x.count())
    # x.delete_record(2)
    # print(x.count())
    # x.insert_record({"id": 2, "name": "omer", "animal": "panda"})
    hate_cat = SelectionCriteria("animal", "==", "cat")
    big = SelectionCriteria("id", ">", "3")
    # x.delete_records([omer_matan])
    # print(x.count())
    x.update_record(4, {"name": "ran", "animal": "cat"})
    x.query_table([hate_cat, big])

    # print(x.count())


test()
