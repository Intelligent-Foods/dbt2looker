from enum import Enum
from typing import Union, Dict, List, Optional
try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal
from pydantic import BaseModel, Field, validator


# dbt2looker utility types
class UnsupportedDbtAdapterError(ValueError):
    def __init__(self, wrong_value: str):
        msg = f'{wrong_value} is not a supported dbt adapter'
        super().__init__(msg)


class SupportedDbtAdapters(str, Enum):
    bigquery = 'bigquery'
    postgres = 'postgres'
    redshift = 'redshift'
    snowflake = 'snowflake'
    spark = 'spark'


# Lookml types
class LookerMeasureType(str, Enum):
    number = 'number'
    string = 'string'
    average = 'average'
    average_distinct = 'average_distinct'
    count = 'count'
    count_distinct = 'count_distinct'
    list = 'list'
    max = 'max'
    median = 'median'
    median_distinct = 'median_distinct'
    min = 'min'
    # percentile = 'percentile'
    # percentile_distinct = 'percentile_distinct'
    sum = 'sum'
    sum_distinct = 'sum_distinct'


class LookerJoinType(str, Enum):
    left_outer = 'left_outer'
    full_outer = 'full_outer'
    inner = 'inner'
    cross = 'cross'


class LookerJoinRelationship(str, Enum):
    many_to_one = 'many_to_one'
    many_to_many = 'many_to_many'
    one_to_many = 'one_to_many'
    one_to_one = 'one_to_one'


class LookerValueFormatName(str, Enum):
    decimal_0 = 'decimal_0'
    decimal_1 = 'decimal_1'
    decimal_2 = 'decimal_2'
    decimal_3 = 'decimal_3'
    decimal_4 = 'decimal_4'
    usd_0 = 'usd_0'
    usd = 'usd'
    gbp_0 = 'gbp_0'
    gbp = 'gbp'
    eur_0 = 'eur_0'
    eur = 'eur'
    id = 'id'
    percent_0 = 'percent_0'
    percent_1 = 'percent_1'
    percent_2 = 'percent_2'
    percent_3 = 'percent_3'
    percent_4 = 'percent_4'


class LookerHiddenType(str, Enum):
    yes = 'yes'
    no = 'no'
    
class LookerConvertTimezoneType(str, Enum):
    yes = 'yes'
    no = 'no'


class Dbt2LookerMeasure(BaseModel):
    type: LookerMeasureType
    filters: Optional[List[Dict[str, str]]] = []
    description: Optional[str] = ''
    sql: Optional[str] = None
    value_format_name: Optional[LookerValueFormatName] = None
    group_label: Optional[str] = None
    view_label: Optional[str] = None
    label: Optional[str] = None
    hidden: Optional[LookerHiddenType] = None
    drill_fields: Optional[List[str]] = None
    list_field: Optional[str] = None
    sql_distinct_key: Optional[str] = None
    value_format: Optional[str] = None

    @validator('filters')
    def filters_are_singular_dicts(cls, v: List[Dict[str, str]]):
        if v is not None:
            for f in v:
                if len(f) != 1:
                    raise ValueError('Multiple filter names provided for a single filter in measure block')
        return v


class Dbt2LookerDimension(BaseModel):
    enabled: Optional[bool] = True
    hidden: Optional[LookerHiddenType] = None
    name: Optional[str] = None
    sql: Optional[str] = None
    description: Optional[str] = ''
    value_format_name: Optional[LookerValueFormatName] = None
    group_label: Optional[str] = None
    view_label: Optional[str] = None
    label: Optional[str] = None
    # similar to data_type, will become type for looker dimensions defined 
    # at the model level
    type: Optional[str] = None
    convert_tz: Optional[LookerConvertTimezoneType] = None
    timeframes: Optional[List[str]] = None
    suggestions: Optional[List[str]] = None
    required_access_grants: Optional[List[str]] = None
    group_item_label: Optional[str] = None
    primary_key: Optional[str] = None
    value_format: Optional[str] = None


class Dbt2LookerMeta(BaseModel):
    measures: Optional[Dict[str, Dbt2LookerMeasure]] = {}
    measure: Optional[Dict[str, Dbt2LookerMeasure]] = {}
    metrics: Optional[Dict[str, Dbt2LookerMeasure]] = {}
    metric: Optional[Dict[str, Dbt2LookerMeasure]] = {}
    dimension: Optional[Dbt2LookerDimension] = Dbt2LookerDimension()


# Looker file types
class LookViewFile(BaseModel):
    filename: str
    contents: str


class LookModelFile(BaseModel):
    filename: str
    contents: str


# dbt config types
class DbtProjectConfig(BaseModel):
    name: str


class DbtModelColumnMeta(Dbt2LookerMeta):
    pass


class DbtModelColumn(BaseModel):
    name: str
    description: Optional[str] = ''
    data_type: Optional[str] = None
    meta: DbtModelColumnMeta


class DbtNode(BaseModel):
    unique_id: str
    resource_type: str


class Dbt2LookerExploreJoin(BaseModel):
    join: str
    type: Optional[LookerJoinType] = LookerJoinType.left_outer
    relationship: Optional[LookerJoinRelationship] = LookerJoinRelationship.many_to_one
    sql_on: Optional[str] = None
    foreign_key: Optional[str] = None
    view_label: Optional[str] = None


class Dbt2LookerModelMeta(BaseModel):
    joins: Optional[List[Dbt2LookerExploreJoin]] = []
    view_name: Optional[str] = None
    label: Optional[str] = None
    view_label: Optional[str] = None
    dimensions: Optional[List[Dbt2LookerDimension]] = []


class DbtModelMeta(Dbt2LookerModelMeta):
    pass

class DbtModelConfig(BaseModel):
    meta: Optional[DbtModelMeta]

class DbtModel(DbtNode):
    resource_type: Literal['model']
    relation_name: Optional[str] = None
    db_schema: str = Field(..., alias='schema')
    name: str
    description: Optional[str] = ''
    columns: Dict[str, DbtModelColumn]
    tags: List[str]
    config: Optional[DbtModelConfig] = None

    @validator('columns')
    def case_insensitive_column_names(cls, v: Dict[str, DbtModelColumn]):
        return {
            name.lower(): column.copy(update={'name': column.name.lower()})
            for name, column in v.items()
        }


class DbtManifestMetadata(BaseModel):
    adapter_type: str

    @validator('adapter_type')
    def adapter_must_be_supported(cls, v):
        try:
            SupportedDbtAdapters(v)
        except ValueError:
            raise UnsupportedDbtAdapterError(wrong_value=v)
        return v


class DbtManifest(BaseModel):
    nodes: Dict[str, Union[DbtModel, DbtNode]]
    metadata: DbtManifestMetadata


class DbtCatalogNodeMetadata(BaseModel):
    type: str
    db_schema: str = Field(..., alias='schema')
    name: str
    comment: Optional[str]
    owner: Optional[str]


class DbtCatalogNodeColumn(BaseModel):
    type: str
    comment: Optional[str]
    index: int
    name: str


class DbtCatalogNode(BaseModel):
    metadata: DbtCatalogNodeMetadata
    columns: Dict[str, DbtCatalogNodeColumn]

    @validator('columns')
    def case_insensitive_column_names(cls, v: Dict[str, DbtCatalogNodeColumn]):
        return {
            name.lower(): column.copy(update={'name': column.name.lower()})
            for name, column in v.items()
        }


class DbtCatalog(BaseModel):
    nodes: Dict[str, DbtCatalogNode]
