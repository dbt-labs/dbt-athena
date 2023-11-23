"""AWS Lakeformation permissions management helper utilities."""

from typing import Dict, List, Optional, Sequence, Set, Union

from mypy_boto3_lakeformation import LakeFormationClient
from mypy_boto3_lakeformation.type_defs import (
    AddLFTagsToResourceResponseTypeDef,
    BatchPermissionsRequestEntryTypeDef,
    ColumnLFTagTypeDef,
    DataCellsFilterTypeDef,
    GetResourceLFTagsResponseTypeDef,
    LFTagPairTypeDef,
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
    inherited_tags: Optional[List[str]] = None


class LfTagsManager:
    def __init__(self, lf_client: LakeFormationClient, relation: AthenaRelation, lf_tags_config: LfTagsConfig):
        self.lf_client = lf_client
        self.database = relation.schema
        self.table = relation.identifier
        self.lf_tags = lf_tags_config.tags
        self.lf_tags_columns = lf_tags_config.tags_columns
        self.lf_inherited_tags = set(lf_tags_config.inherited_tags) if lf_tags_config.inherited_tags else set()

    def process_lf_tags_database(self) -> None:
        if self.lf_tags:
            database_resource = {"Database": {"Name": self.database}}
            response = self.lf_client.add_lf_tags_to_resource(
                Resource=database_resource, LFTags=[{"TagKey": k, "TagValues": [v]} for k, v in self.lf_tags.items()]
            )
            self._parse_and_log_lf_response(response, None, self.lf_tags)

    def process_lf_tags(self) -> None:
        table_resource = {"Table": {"DatabaseName": self.database, "Name": self.table}}
        existing_lf_tags = self.lf_client.get_resource_lf_tags(Resource=table_resource)
        self._remove_lf_tags_columns(existing_lf_tags)
        self._apply_lf_tags_table(table_resource, existing_lf_tags)
        self._apply_lf_tags_columns()

    @staticmethod
    def _column_tags_to_remove(
        lf_tags_columns: List[ColumnLFTagTypeDef], lf_inherited_tags: Set[str]
    ) -> Dict[str, Dict[str, List[str]]]:
        to_remove = {}

        for column in lf_tags_columns:
            non_inherited_tags = [tag for tag in column["LFTags"] if not tag["TagKey"] in lf_inherited_tags]
            for tag in non_inherited_tags:
                tag_key = tag["TagKey"]
                tag_value = tag["TagValues"][0]
                if tag_key not in to_remove:
                    to_remove[tag_key] = {tag_value: [column["Name"]]}
                elif tag_value not in to_remove[tag_key]:
                    to_remove[tag_key][tag_value] = [column["Name"]]
                else:
                    to_remove[tag_key][tag_value].append(column["Name"])

        return to_remove

    def _remove_lf_tags_columns(self, existing_lf_tags: GetResourceLFTagsResponseTypeDef) -> None:
        lf_tags_columns = existing_lf_tags.get("LFTagsOnColumns", [])
        logger.debug(f"COLUMNS: {lf_tags_columns}")
        if lf_tags_columns:
            to_remove = LfTagsManager._column_tags_to_remove(lf_tags_columns, self.lf_inherited_tags)
            logger.debug(f"TO REMOVE: {to_remove}")
            for tag_key, tag_config in to_remove.items():
                for tag_value, columns in tag_config.items():
                    resource = {
                        "TableWithColumns": {"DatabaseName": self.database, "Name": self.table, "ColumnNames": columns}
                    }
                    response = self.lf_client.remove_lf_tags_from_resource(
                        Resource=resource, LFTags=[{"TagKey": tag_key, "TagValues": [tag_value]}]
                    )
                    self._parse_and_log_lf_response(response, columns, {tag_key: tag_value}, "remove")

    @staticmethod
    def _table_tags_to_remove(
        lf_tags_table: List[LFTagPairTypeDef], lf_tags: Optional[Dict[str, str]], lf_inherited_tags: Set[str]
    ) -> Dict[str, Sequence[str]]:
        return {
            tag["TagKey"]: tag["TagValues"]
            for tag in lf_tags_table
            if tag["TagKey"] not in (lf_tags or {})
            if tag["TagKey"] not in lf_inherited_tags
        }

    def _apply_lf_tags_table(
        self, table_resource: ResourceTypeDef, existing_lf_tags: GetResourceLFTagsResponseTypeDef
    ) -> None:
        lf_tags_table = existing_lf_tags.get("LFTagsOnTable", [])
        logger.debug(f"EXISTING TABLE TAGS: {lf_tags_table}")
        logger.debug(f"CONFIG TAGS: {self.lf_tags}")

        to_remove = LfTagsManager._table_tags_to_remove(lf_tags_table, self.lf_tags, self.lf_inherited_tags)

        logger.debug(f"TAGS TO REMOVE: {to_remove}")
        if to_remove:
            response = self.lf_client.remove_lf_tags_from_resource(
                Resource=table_resource, LFTags=[{"TagKey": k, "TagValues": v} for k, v in to_remove.items()]
            )
            self._parse_and_log_lf_response(response, None, self.lf_tags, "remove")

        if self.lf_tags:
            response = self.lf_client.add_lf_tags_to_resource(
                Resource=table_resource, LFTags=[{"TagKey": k, "TagValues": [v]} for k, v in self.lf_tags.items()]
            )
            self._parse_and_log_lf_response(response, None, self.lf_tags)

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
                    self._parse_and_log_lf_response(response, columns, {tag_key: tag_value})

    def _parse_and_log_lf_response(
        self,
        response: Union[AddLFTagsToResourceResponseTypeDef, RemoveLFTagsFromResourceResponseTypeDef],
        columns: Optional[List[str]] = None,
        lf_tags: Optional[Dict[str, str]] = None,
        verb: str = "add",
    ) -> None:
        table_appendix = f".{self.table}" if self.table else ""
        columns_appendix = f" for columns {columns}" if columns else ""
        resource_msg = self.database + table_appendix + columns_appendix
        if failures := response.get("Failures", []):
            base_msg = f"Failed to {verb} LF tags: {lf_tags} to " + resource_msg
            for failure in failures:
                tag = failure.get("LFTag", {}).get("TagKey")
                error = failure.get("Error", {}).get("ErrorMessage")
                logger.error(f"Failed to {verb} {tag} for " + resource_msg + f" - {error}")
            raise DbtRuntimeError(base_msg)
        logger.debug(f"Success: {verb} LF tags {lf_tags} to " + resource_msg)


class FilterConfig(BaseModel):
    row_filter: str
    column_names: List[str] = []
    principals: List[str] = []

    def to_api_repr(self, catalog_id: str, database: str, table: str, name: str) -> DataCellsFilterTypeDef:
        return {
            "TableCatalogId": catalog_id,
            "DatabaseName": database,
            "TableName": table,
            "Name": name,
            "RowFilter": {"FilterExpression": self.row_filter},
            "ColumnNames": self.column_names,
            "ColumnWildcard": {"ExcludedColumnNames": []},
        }

    def to_update(self, existing: DataCellsFilterTypeDef) -> bool:
        return self.row_filter != existing["RowFilter"]["FilterExpression"] or set(self.column_names) != set(
            existing["ColumnNames"]
        )


class DataCellFiltersConfig(BaseModel):
    enabled: bool = False
    filters: Dict[str, FilterConfig]


class LfGrantsConfig(BaseModel):
    data_cell_filters: DataCellFiltersConfig


class LfPermissions:
    def __init__(self, catalog_id: str, relation: AthenaRelation, lf_client: LakeFormationClient) -> None:
        self.catalog_id = catalog_id
        self.relation = relation
        self.database: str = relation.schema
        self.table: str = relation.identifier
        self.lf_client = lf_client

    def get_filters(self) -> Dict[str, DataCellsFilterTypeDef]:
        table_resource = {"CatalogId": self.catalog_id, "DatabaseName": self.database, "Name": self.table}
        return {f["Name"]: f for f in self.lf_client.list_data_cells_filter(Table=table_resource)["DataCellsFilters"]}

    def process_filters(self, config: LfGrantsConfig) -> None:
        current_filters = self.get_filters()
        logger.debug(f"CURRENT FILTERS: {current_filters}")

        to_drop = [f for name, f in current_filters.items() if name not in config.data_cell_filters.filters]
        logger.debug(f"FILTERS TO DROP: {to_drop}")
        for f in to_drop:
            self.lf_client.delete_data_cells_filter(
                TableCatalogId=f["TableCatalogId"],
                DatabaseName=f["DatabaseName"],
                TableName=f["TableName"],
                Name=f["Name"],
            )

        to_add = [
            f.to_api_repr(self.catalog_id, self.database, self.table, name)
            for name, f in config.data_cell_filters.filters.items()
            if name not in current_filters
        ]
        logger.debug(f"FILTERS TO ADD: {to_add}")
        for f in to_add:
            self.lf_client.create_data_cells_filter(TableData=f)

        to_update = [
            f.to_api_repr(self.catalog_id, self.database, self.table, name)
            for name, f in config.data_cell_filters.filters.items()
            if name in current_filters and f.to_update(current_filters[name])
        ]
        logger.debug(f"FILTERS TO UPDATE: {to_update}")
        for f in to_update:
            self.lf_client.update_data_cells_filter(TableData=f)

    def process_permissions(self, config: LfGrantsConfig) -> None:
        for name, f in config.data_cell_filters.filters.items():
            logger.debug(f"Start processing permissions for filter: {name}")
            current_permissions = self.lf_client.list_permissions(
                Resource={
                    "DataCellsFilter": {
                        "TableCatalogId": self.catalog_id,
                        "DatabaseName": self.database,
                        "TableName": self.table,
                        "Name": name,
                    }
                }
            )["PrincipalResourcePermissions"]

            current_principals = {p["Principal"]["DataLakePrincipalIdentifier"] for p in current_permissions}

            to_revoke = {p for p in current_principals if p not in f.principals}
            if to_revoke:
                self.lf_client.batch_revoke_permissions(
                    CatalogId=self.catalog_id,
                    Entries=[self._permission_entry(name, principal, idx) for idx, principal in enumerate(to_revoke)],
                )
                revoke_principals_msg = "\n".join(to_revoke)
                logger.debug(f"Revoked permissions for filter {name} from principals:\n{revoke_principals_msg}")
            else:
                logger.debug(f"No redundant permissions found for filter: {name}")

            to_add = {p for p in f.principals if p not in current_principals}
            if to_add:
                self.lf_client.batch_grant_permissions(
                    CatalogId=self.catalog_id,
                    Entries=[self._permission_entry(name, principal, idx) for idx, principal in enumerate(to_add)],
                )
                add_principals_msg = "\n".join(to_add)
                logger.debug(f"Granted permissions for filter {name} to principals:\n{add_principals_msg}")
            else:
                logger.debug(f"No new permissions added for filter {name}")

            logger.debug(f"Permissions are set to be consistent with config for filter: {name}")

    def _permission_entry(self, filter_name: str, principal: str, idx: int) -> BatchPermissionsRequestEntryTypeDef:
        return {
            "Id": str(idx),
            "Principal": {"DataLakePrincipalIdentifier": principal},
            "Resource": {
                "DataCellsFilter": {
                    "TableCatalogId": self.catalog_id,
                    "DatabaseName": self.database,
                    "TableName": self.table,
                    "Name": filter_name,
                }
            },
            "Permissions": ["SELECT"],
            "PermissionsWithGrantOption": [],
        }
