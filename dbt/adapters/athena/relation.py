from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation, Policy


@dataclass
class AthenaIncludePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class AthenaRelation(BaseRelation):
    quote_character: str = ""
    include_policy: Policy = AthenaIncludePolicy()
