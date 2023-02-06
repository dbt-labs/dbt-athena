from dataclasses import dataclass, field
from typing import Dict, Optional, Set

from dbt.adapters.base.relation import BaseRelation, InformationSchema, Policy


@dataclass
class AthenaIncludePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class AthenaRelation(BaseRelation):
    quote_character: str = ""
    include_policy: Policy = field(default_factory=lambda: AthenaIncludePolicy())


class AthenaSchemaSearchMap(Dict[InformationSchema, Dict[str, Set[Optional[str]]]]):
    """A utility class to keep track of what information_schema tables to
    search for what schemas and relations. The schema and relation values are all
    lowercased to avoid duplication.
    """

    def add(self, relation: AthenaRelation):
        key = relation.information_schema_only()
        if key not in self:
            self[key] = {}
        schema: Optional[str] = None
        if relation.schema is not None:
            schema = relation.schema.lower()
            relation_name = relation.name.lower()
            if schema not in self[key]:
                self[key][schema] = set()
            self[key][schema].add(relation_name)
