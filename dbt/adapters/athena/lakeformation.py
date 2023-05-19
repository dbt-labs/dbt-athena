from typing import Dict, List, Optional, Union

from mypy_boto3_lakeformation import LakeFormationClient
from mypy_boto3_lakeformation.type_defs import (
    AddLFTagsToResourceResponseTypeDef,
    GetResourceLFTagsResponseTypeDef,
    RemoveLFTagsFromResourceResponseTypeDef,
    ResourceTypeDef,
)
from pydantic import BaseModel

from dbt.adapters.athena.relation import AthenaRelation
from dbt.events import AdapterLogger
from dbt.exceptions import DbtRuntimeError

logger = AdapterLogger("AthenaLakeFormation")


class LfTagsConfig(BaseModel):
    enabled: bool = False
    tags: Optional[Dict[str, str]] = None
    tags_columns: Optional[Dict[str, Dict[str, List[str]]]] = None


class LfTagsManager:
    def __init__(self, lf_client: LakeFormationClient, relation: AthenaRelation, lf_tags_config: LfTagsConfig):
        self.lf_client = lf_client
        self.database = relation.schema
        self.table = relation.identifier
        self.lf_tags = lf_tags_config.tags
        self.lf_tags_columns = lf_tags_config.tags_columns

    def process_lf_tags(self) -> None:
        table_resource = {"Table": {"DatabaseName": self.database, "Name": self.table}}
        existing_lf_tags = self.lf_client.get_resource_lf_tags(Resource=table_resource)
        self._remove_lf_tags_columns(existing_lf_tags)
        self._apply_lf_tags_table(table_resource, existing_lf_tags)
        self._apply_lf_tags_columns()

    def _remove_lf_tags_columns(self, existing_lf_tags: GetResourceLFTagsResponseTypeDef) -> None:
        lf_tags_columns = existing_lf_tags.get("LFTagsOnColumns", [])
        logger.debug(f"COLUMNS: {lf_tags_columns}")
        if lf_tags_columns:
            to_remove = {}
            for column in lf_tags_columns:
                for tag in column["LFTags"]:
                    tag_key = tag["TagKey"]
                    tag_value = tag["TagValues"][0]
                    if tag_key not in to_remove:
                        to_remove[tag_key] = {tag_value: [column["Name"]]}
                    elif tag_value not in to_remove[tag_key]:
                        to_remove[tag_key][tag_value] = [column["Name"]]
                    else:
                        to_remove[tag_key][tag_value].append(column["Name"])
            logger.debug(f"TO REMOVE: {to_remove}")
            for tag_key, tag_config in to_remove.items():
                for tag_value, columns in tag_config.items():
                    resource = {
                        "TableWithColumns": {"DatabaseName": self.database, "Name": self.table, "ColumnNames": columns}
                    }
                    response = self.lf_client.remove_lf_tags_from_resource(
                        Resource=resource, LFTags=[{"TagKey": tag_key, "TagValues": [tag_value]}]
                    )
                    logger.debug(self._parse_lf_response(response, columns, {tag_key: tag_value}, "remove"))

    def _apply_lf_tags_table(
        self, table_resource: ResourceTypeDef, existing_lf_tags: GetResourceLFTagsResponseTypeDef
    ) -> None:
        lf_tags_table = existing_lf_tags.get("LFTagsOnTable", [])
        logger.debug(f"EXISTING TABLE TAGS: {lf_tags_table}")
        logger.debug(f"CONFIG TAGS: {self.lf_tags}")

        to_remove = {tag["TagKey"]: tag["TagValues"] for tag in lf_tags_table if tag["TagKey"] not in self.lf_tags}
        logger.debug(f"TAGS TO REMOVE: {to_remove}")
        if to_remove:
            response = self.lf_client.remove_lf_tags_from_resource(
                Resource=table_resource, LFTags=[{"TagKey": k, "TagValues": v} for k, v in to_remove.items()]
            )
            logger.debug(self._parse_lf_response(response, None, self.lf_tags, "remove"))

        if self.lf_tags:
            response = self.lf_client.add_lf_tags_to_resource(
                Resource=table_resource, LFTags=[{"TagKey": k, "TagValues": [v]} for k, v in self.lf_tags.items()]
            )
            logger.debug(self._parse_lf_response(response, None, self.lf_tags))

    def _apply_lf_tags_columns(self) -> None:
        if self.lf_tags_columns:
            for tag_key, tag_config in self.lf_tags_columns.items():
                for tag_value, columns in tag_config.items():
                    resource = {
                        "TableWithColumns": {"DatabaseName": self.database, "Name": self.table, "ColumnNames": columns}
                    }
                    response = self.lf_client.add_lf_tags_to_resource(
                        Resource=resource,
                        LFTags=[{"TagKey": tag_key, "TagValues": [tag_value]}],
                    )
                    logger.debug(self._parse_lf_response(response, columns, {tag_key: tag_value}))

    def _parse_lf_response(
        self,
        response: Union[AddLFTagsToResourceResponseTypeDef, RemoveLFTagsFromResourceResponseTypeDef],
        columns: Optional[List[str]] = None,
        lf_tags: Dict[str, str] = None,
        verb: str = "add",
    ) -> str:
        failures = response.get("Failures", [])
        columns_appendix = f" for columns {columns}" if columns else ""
        if failures:
            base_msg = f"Failed to {verb} LF tags: {lf_tags} to {self.database}.{self.table}" + columns_appendix
            for failure in failures:
                tag = failure.get("LFTag", {}).get("TagKey")
                error = failure.get("Error", {}).get("ErrorMessage")
                logger.error(f"Failed to {verb} {tag} for {self.database}.{self.table}" + f" - {error}")
            raise DbtRuntimeError(base_msg)
        return f"Success: {verb} LF tags: {lf_tags} to {self.database}.{self.table}" + columns_appendix
