import datetime
import os
from typing import List, Optional
from uuid import UUID

import psycopg as pg
from ssaw.models import Group, QuestionnaireDocument
from ssaw.utils import get_properties
import strawberry
from strawberry.schema.config import StrawberryConfig


VALUE_TYPES = {
    "TextQuestion": str,
    "TextListQuestion":List[str],
    "SingleQuestion": int,
    "NumericQuestion": int,
    "MultyOptionsQuestion": List[int],
    "DateTimeQuestion": datetime.date,
    "Variable_LONG": int,
    "Variable_STRING": str,
    "Variable_DOUBLE": float,
    "Variable_DATE": datetime.date,
    "Variable_BOOLEAN": bool,
}

CLASS_LOOKUP = {}

def parse_questionnaire(document) -> QuestionnaireDocument:
    Group.update_forward_refs()
    return QuestionnaireDocument.parse_obj(document)

def create_strawberry_types(resource_id, q):
    fields = {}
    groups = get_properties(q, groups=True, items=False)

    fields = {'interview__id': None}
    annotations = {'interview__id': UUID}
    gr_fields = {}
    gr_annotations = {}
    for name, var in get_properties(q).items():
        obj_type = var.obj_type
        if obj_type in ["StaticText",]:
            continue
        if obj_type == "Variable":
            obj_type = f"{obj_type}_{var.type.name}"

        key = var.parent_id
        if groups[key].is_roster:
            if key in gr_fields:
                gr_fields[key][name] = strawberry.field(default=None, description=getattr(var, "variable_label", None))
                gr_annotations[key][name] = Optional[VALUE_TYPES[obj_type]]
            else:
                gr_fields[key]={'roster__vector': None}
                gr_annotations[key]={'roster__vector': List[int]}
                gr_name = groups[key].variable_name
                fields[gr_name] = strawberry.field(default=None, description=groups[key].title)
        else:
            fields[name] = strawberry.field(default=None, description=getattr(var, "variable_label", None))
            annotations[name] = Optional[VALUE_TYPES[obj_type]]

    for id, gr in gr_fields.items():
        name = groups[id].variable_name
        gr_cls = type(name, (), gr)
        gr_cls.__annotations__ = gr_annotations[id]
        CLASS_LOOKUP[name] = gr_cls
        annotations[name] = Optional[List[strawberry.type(gr_cls)]]

    cls = type(resource_id, (), fields)
    cls.__annotations__ = annotations
    CLASS_LOOKUP[resource_id] = cls
    return strawberry.type(cls)


def create_strawberry_query(fields):
    class_fields = {name: strawberry.field(resolver=get_interviews) for name in fields}
    class_annotations = {name: List[field_type] for name, field_type in fields.items()}
    cls = type("Query", (), class_fields)
    cls.__annotations__ = class_annotations
    return strawberry.type(cls)


def row_to_obj(row: dict):
    return {
        key: [
            CLASS_LOOKUP[key](**el)
            for el in val
        ] if key in CLASS_LOOKUP else val
        for key, val in row.items()
    }


def get_interviews(info, limit:int=10):
    main = info.selected_fields[0]
    table_name = db_tables[info.python_name]

    db_fields = [f"'{fld.name}',{fld.name.lower()}" for fld in main.selections if not fld.selections]

    nested_fields = {
        fld.name: [
            f"'{nested_fld.name}',{nested_fld.name.lower()}"
            for nested_fld in fld.selections
            if fld.selections
        ]
        for fld in main.selections
        if fld.selections
    }

    sql_from = f"FROM {table_name}"
    sql_with = []
    for child, flds in nested_fields.items():
        sql_with.append(f'{child} AS (SELECT interview__id, json_agg(json_build_object({",".join(flds)})) AS {child} FROM {table_name[:-1]}_{child}" {child} GROUP BY interview__id)')
        sql_from += f" LEFT JOIN {child} ON {table_name}.interview__id = {child}.interview__id"
        db_fields.append(f"'{child}', coalesce({child}.{child}, '[]'::json)")

    sql = f'WITH {",".join(sql_with)}' if sql_with else ''
    sql = f'{sql} SELECT json_build_object({",".join(db_fields)}) {sql_from} LIMIT {limit}'
    print(sql)

    with conn.cursor() as cur:
        cur.execute(sql)

        res = cur.fetchall()
        print([row[0] for row in res])
        return [CLASS_LOOKUP[info.python_name](**row_to_obj(row[0])) for row in res]


conn_str = os.getenv("CONNECTION_STRING", "host=127.0.0.1 dbname=headquarters user=postgres password=")
conn = pg.connect(conn_str, autocommit=True)
cur = conn.cursor()

cur.execute("SELECT name FROM workspaces.workspaces WHERE disabled_at_utc is NULL")
workspaces = cur.fetchall()

fields = {}
db_tables = {}
for ws in workspaces:
    ws_name = ws[0]
    cur.execute(f"SELECT value from ws_{ws_name}.appsettings WHERE id='ExportService.ApiKey'")
    export_key = cur.fetchone()[0]["Key"]
    prefix = "default_" if ws[0] == "primary" else f"default_{ws_name}_"
    schema = f"{prefix}{export_key}"
    cur.execute(f"SELECT * FROM ws_{ws_name}.questionnairedocuments")
    for quest_row in cur.fetchall():
        q=parse_questionnaire(quest_row[1])
        version = quest_row[0].split('$')[1]
        resource_id = f"{ws[0]}_{q.variable_name}_{version}"
        table_name = f"{q.variable_name}${version}"
        cur.execute(f"SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_schema = '{schema}' AND table_name = '{table_name}')")
        if cur.fetchone()[0] is False:
            continue
        interview_data = create_strawberry_types(resource_id, q)
        fields[resource_id] = interview_data
        db_tables[resource_id] = f'"{schema}"."{table_name}"'

cur.close()
schema = strawberry.Schema(query=create_strawberry_query(fields), config=StrawberryConfig(auto_camel_case=False))
