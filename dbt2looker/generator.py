import logging
import re

import lkml

from . import models

LOOKER_DTYPE_MAP = {
    'bigquery': {
        'INT64':     'number',
        'INTEGER':   'number',
        'FLOAT':     'number',
        'FLOAT64':   'number',
        'NUMERIC':   'number',
        'BIGNUMERIC': 'number',
        'BOOLEAN':   'yesno',
        'STRING':    'string',
        'TIMESTAMP': 'timestamp',
        'DATETIME':  'datetime',
        'DATE':      'date',
        'TIME':      'string',    # Can time-only be handled better in looker?
        'BOOL':      'yesno',
        'ARRAY':     'string',
        'GEOGRAPHY': 'string',
        'BYTES': 'string',
    },
    'snowflake': {
        'NUMBER': 'number',
        'DECIMAL': 'number',
        'NUMERIC': 'number',
        'INT': 'number',
        'INTEGER': 'number',
        'BIGINT': 'number',
        'SMALLINT': 'number',
        'FLOAT': 'number',
        'FLOAT4': 'number',
        'FLOAT8': 'number',
        'DOUBLE': 'number',
        'DOUBLE PRECISION': 'number',
        'REAL': 'number',
        'VARCHAR': 'string',
        'CHAR': 'string',
        'CHARACTER': 'string',
        'STRING': 'string',
        'TEXT': 'string',
        'BINARY': 'string',
        'VARBINARY': 'string',
        'BOOLEAN': 'yesno',
        'DATE': 'date',
        'DATETIME': 'datetime',
        'TIME': 'string',        # can we support time?
        'TIMESTAMP': 'timestamp',
        'TIMESTAMP_NTZ': 'timestamp',
        # TIMESTAMP_LTZ not supported (see https://docs.looker.com/reference/field-params/dimension_group)
        # TIMESTAMP_TZ not supported (see https://docs.looker.com/reference/field-params/dimension_group)
        'VARIANT': 'string',
        'OBJECT': 'string',
        'ARRAY': 'string',
        'GEOGRAPHY': 'string',
    },
    'redshift': {
        'SMALLINT': 'number',
        'INT2': 'number',
        'INTEGER': 'number',
        'INT': 'number',
        'INT4': 'number',
        'BIGINT': 'number',
        'INT8': 'number',
        'DECIMAL': 'number',
        'NUMERIC': 'number',
        'REAL': 'number',
        'FLOAT4': 'number',
        'DOUBLE PRECISION': 'number',
        'FLOAT8': 'number',
        'FLOAT': 'number',
        'BOOLEAN': 'yesno',
        'BOOL': 'yesno',
        'CHAR': 'string',
        'CHARACTER': 'string',
        'NCHAR': 'string',
        'BPCHAR': 'string',
        'VARCHAR': 'string',
        'CHARACTER VARYING': 'string',
        'NVARCHAR': 'string',
        'TEXT': 'string',
        'DATE': 'date',
        'TIMESTAMP': 'timestamp',
        'TIMESTAMP WITHOUT TIME ZONE': 'timestamp',
        # TIMESTAMPTZ not supported
        # TIMESTAMP WITH TIME ZONE not supported
        'GEOMETRY': 'string',
        # HLLSKETCH not supported
        'TIME': 'string',
        'TIME WITHOUT TIME ZONE': 'string',
        # TIMETZ not supported
        # TIME WITH TIME ZONE not supported
    },
    'postgres': {
        # BIT, BIT VARYING, VARBIT not supported
        # BOX not supported
        # BYTEA not supported
        # CIRCLE not supported
        # INTERVAL not supported
        # LINE not supported
        # LSEG not supported
        # PATH not supported
        # POINT not supported
        # POLYGON not supported
        # TSQUERY, TSVECTOR not supported
        'XML': 'string',
        'UUID': 'string',
        'PG_LSN': 'string',
        'MACADDR': 'string',
        'JSON': 'string',
        'JSONB': 'string',
        'CIDR': 'string',
        'INET': 'string',
        'MONEY': 'number',
        'SMALLINT': 'number',
        'INT2': 'number',
        'SMALLSERIAL': 'number',
        'SERIAL2': 'number',
        'INTEGER': 'number',
        'INT': 'number',
        'INT4': 'number',
        'SERIAL': 'number',
        'SERIAL4': 'number',
        'BIGINT': 'number',
        'INT8': 'number',
        'BIGSERIAL': 'number',
        'SERIAL8': 'number',
        'DECIMAL': 'number',
        'NUMERIC': 'number',
        'REAL': 'number',
        'FLOAT4': 'number',
        'DOUBLE PRECISION': 'number',
        'FLOAT8': 'number',
        'FLOAT': 'number',
        'BOOLEAN': 'yesno',
        'BOOL': 'yesno',
        'CHAR': 'string',
        'CHARACTER': 'string',
        'NCHAR': 'string',
        'BPCHAR': 'string',
        'VARCHAR': 'string',
        'CHARACTER VARYING': 'string',
        'NVARCHAR': 'string',
        'TEXT': 'string',
        'DATE': 'date',
        'TIMESTAMP': 'timestamp',
        'TIMESTAMP WITHOUT TIME ZONE': 'timestamp',
        # TIMESTAMPTZ not supported
        # TIMESTAMP WITH TIME ZONE not supported
        'GEOMETRY': 'string',
        # HLLSKETCH not supported
        'TIME': 'string',
        'TIME WITHOUT TIME ZONE': 'string',
        'STRING': 'string',
        # TIMETZ not supported
        # TIME WITH TIME ZONE not supported
    },
    'spark': {
        'BYTE':        'number',
        'SHORT':       'number',
        'INTEGER':     'number',
        'LONG':        'number',
        'FLOAT':       'number',
        'DOUBLE':      'number',
        'DECIMAL':     'number',
        'STRING':      'string',
        'VARCHAR':     'string',
        'CHAR':        'string',
        'BOOLEAN':     'yesno',
        'TIMESTAMP':   'timestamp',
        'DATE':        'datetime',
    }
}

looker_date_time_types = ['datetime', 'timestamp']
looker_date_types = ['date']
looker_scalar_types = ['number', 'yesno', 'string']

looker_timeframes = [
    'raw',
    'time',
    'hour',
    'date',
    'day_of_week',
    'day_of_year',
    'week',
    'week_of_year',
    'month',
    'month_num',
    'month_name',
    'quarter',
    'quarter_of_year',
    'year'
]


def normalise_spark_types(column_type: str) -> str:
    return re.match(r'^[^\(]*', column_type).group(0)

# A dimension/measure description will start indented at 4 spaces, so subsequent
# lines should start indented at 4 + 2 spaces.
def indent_multiline_description(description: str, space_count = 6) -> str:
    return description.replace('\n', '\n' + ' ' * space_count)


def map_adapter_type_to_looker(adapter_type: models.SupportedDbtAdapters, column_type: str):
    normalised_column_type = (normalise_spark_types(column_type) if adapter_type == models.SupportedDbtAdapters.spark.value else column_type).upper()
    looker_type = LOOKER_DTYPE_MAP[adapter_type].get(normalised_column_type)
    if (column_type is not None) and (looker_type is None):
        logging.warning(f'Column type {column_type} not supported for conversion from {adapter_type} to looker. No dimension will be created.')
    return looker_type


def lookml_date_time_dimension_group(column: models.DbtModelColumn, adapter_type: models.SupportedDbtAdapters):
    description = column.meta.dimension.description or column.description
    return {
        'name': column.meta.dimension.name or column.name,
        'type': 'time',
        **(
            {'label': column.meta.dimension.label}
            if (column.meta.dimension.label)
            else {}
        ),
        'sql': column.meta.dimension.sql or f'${{TABLE}}.{column.name}',
        'description': indent_multiline_description(description),
        'datatype': map_adapter_type_to_looker(adapter_type, column.data_type),
        'timeframes': column.meta.dimension.timeframes or looker_timeframes,
        **(
            {'view_label': column.meta.dimension.view_label}
            if (column.meta.dimension.view_label)
            else {}
        ),
        # convert_tz is yes if not specified by default
        **(
            {'convert_tz': 'no'}
            if (column.meta.dimension.convert_tz == 'no')
            else {}
        )
    }


def lookml_date_dimension_group(column: models.DbtModelColumn, adapter_type: models.SupportedDbtAdapters):
    description = column.meta.dimension.description or column.description
    return {
        'name': column.meta.dimension.name or column.name,
        'type': 'time',
        **(
            {'label': column.meta.dimension.label}
            if (column.meta.dimension.label)
            else {}
        ),
        'sql': column.meta.dimension.sql or f'${{TABLE}}.{column.name}',
        'description': indent_multiline_description(description),
        'datatype': map_adapter_type_to_looker(adapter_type, column.data_type),
        'timeframes': column.meta.dimension.timeframes or looker_timeframes,
        **(
            {'view_label': column.meta.dimension.view_label}
            if (column.meta.dimension.view_label)
            else {}
        ),
        **(
            {'convert_tz': 'no'}
            if (column.meta.dimension.convert_tz == 'no')
            else {}
        )
    }


def lookml_dimension_groups_from_model(model: models.DbtModel, adapter_type: models.SupportedDbtAdapters):
    date_times = [
        lookml_date_time_dimension_group(column, adapter_type)
        for column in model.columns.values()
        if map_adapter_type_to_looker(adapter_type, column.data_type) in looker_date_time_types
    ]
    dates = [
        lookml_date_dimension_group(column, adapter_type)
        for column in model.columns.values()
        if column.meta.dimension.enabled
        and map_adapter_type_to_looker(adapter_type, column.data_type) in looker_date_types
    ]
    return date_times + dates

def get_optional_dimension_fields_dict(props, looker_type):
    return {
        **(
            {'hidden': props.hidden.value}
            if (props.hidden)
            else {}
        ),
        **(
            {'value_format_name': props.value_format_name.value}
            if (props.value_format_name and looker_type == 'number')
            else {}
        ),
        **(
            {'label': props.label}
            if (props.label)
            else {}
        ),
        **(
            {'group_label': props.group_label}
            if (props.group_label)
            else {}
        ),
        **(
            {'view_label': props.view_label}
            if (props.view_label)
            else {}
        ),
        **(
            {'suggestions': props.suggestions}
            if (props.suggestions)
            else {}
        ),
        **(
            {'required_access_grants': props.required_access_grants}
            if (props.required_access_grants)
            else {}
        )
    }

def lookml_dimensions_from_model(model: models.DbtModel, adapter_type: models.SupportedDbtAdapters):
    column_dimensions = [
        {
            'name': column.meta.dimension.name or column.name,
            'type': map_adapter_type_to_looker(adapter_type, column.data_type),
            'sql': column.meta.dimension.sql or f'${{TABLE}}.{column.name}',
            'description': indent_multiline_description(column.meta.dimension.description or column.description),
            **(get_optional_dimension_fields_dict(
                column.meta.dimension,
                map_adapter_type_to_looker(adapter_type, column.data_type)
            ))
        }
        for column in model.columns.values()
        if column.meta.dimension.enabled
        and map_adapter_type_to_looker(adapter_type, column.data_type) in looker_scalar_types
    ]
    # dimensions defined at the model level, useful for dimensions derived by
    # an SQL formula based on multiple columns
    model_dimensions = [
        {
            'name': dim.name,
            'type': map_adapter_type_to_looker(adapter_type, dim.type),
            'sql': dim.sql,
            'description': indent_multiline_description(dim.description),
            **(get_optional_dimension_fields_dict(
                dim,
                map_adapter_type_to_looker(adapter_type, dim.type)
            ))
        }
        for dim in model.config.meta.dimensions
        if map_adapter_type_to_looker(adapter_type, dim.type) in looker_scalar_types
    ]
    return column_dimensions + model_dimensions


def lookml_measure_filters(measure: models.Dbt2LookerMeasure, model: models.DbtModel):
    # This check is (temporarily) disabled as it cannot handle the case when a filter
    # references a dimension defined as a derived dimension, not a column
    
    # try:
    #     columns = {
    #         column_name: model.columns[column_name]
    #         for f in measure.filters
    #         for column_name in f
    #     }
    # except KeyError as e:
    #     raise ValueError(
    #         f'Model {model.unique_id} contains a measure that references a non_existent column: {e}\n'
    #         f'Ensure that dbt model {model.unique_id} contains a column: {e}'
    #     ) from e
    return [{
        (column_name): fexpr
        for column_name, fexpr in f.items()
    } for f in measure.filters]


def lookml_measures_from_model(model: models.DbtModel):
    return [
        lookml_measure(measure_name, column, measure, model)
        for column in model.columns.values()
        for measure_name, measure in {
            **column.meta.measures, **column.meta.measure, **column.meta.metrics, **column.meta.metric
        }.items()
    ]


def lookml_measure(measure_name: str, column: models.DbtModelColumn, measure: models.Dbt2LookerMeasure, model: models.DbtModel):
    measure_description = measure.description or column.description or f'{measure.type.value.capitalize()} of {column.name}'

    m = {
        'name': measure_name,
        'type': measure.type.value,
        'sql': measure.sql or f'${{TABLE}}.{column.name}',
        'description': indent_multiline_description(measure_description),
    }
    if measure.filters:
        m['filters'] = lookml_measure_filters(measure, model)
    if measure.value_format_name:
        m['value_format_name'] = measure.value_format_name.value
    if measure.group_label:
        m['group_label'] = measure.group_label
    if measure.view_label:
        m['view_label'] = measure.view_label
    if measure.label:
        m['label'] = measure.label
    if measure.hidden:
        m['hidden'] = measure.hidden.value
    if measure.drill_fields:
        m['drill_fields'] = measure.drill_fields
    return m

def lookml_set_of_dimensions(dimensions, dimension_groups):
    dimension_group_fields = [
        dg['name'] + '_' + dg['timeframes'][0]
        for dg in dimension_groups
    ]
    dimension_fields = [
        d['name']
        for d in dimensions if 'hidden' not in d.keys()
    ]
    return { 
        "name": "details",
        "fields": dimension_group_fields + dimension_fields
    }


def lookml_view_from_dbt_model(model: models.DbtModel, adapter_type: models.SupportedDbtAdapters):
    view_name = model.config.meta.view_name or model.name

    dimensions = lookml_dimensions_from_model(model, adapter_type)
    dimension_groups = lookml_dimension_groups_from_model(model, adapter_type)

    lookml = {
        'view': {
            'name': view_name,
            'sql_table_name': model.relation_name,
            'drill_fields': '[details*]',
            'dimension_groups': dimension_groups,
            'dimensions': dimensions,
            'measures': lookml_measures_from_model(model),
            'set': lookml_set_of_dimensions(dimensions, dimension_groups)
        }
    }
    logging.debug(
        f'Created view from model %s with %d measures, %d dimensions',
        view_name,
        len(lookml['view']['measures']),
        len(lookml['view']['dimensions']),
    )
    contents = lkml.dump(lookml)
    filename = f'{view_name}.view.lkml'
    return models.LookViewFile(filename=filename, contents=contents)


def lookml_model_from_dbt_model(model: models.DbtModel, connection_name: str):
    # Note: assumes view names = model names
    #       and models are unique across dbt packages in project
    view_name = model.config.meta.view_name or model.name
    lookml = {
        'connection': connection_name,
        'include': '/views/*',
        'explore': {
            'name': view_name
        }
    }
    if model.config.meta.label:
        lookml['explore']['label'] = model.config.meta.label
    if model.config.meta.view_label:
        lookml['explore']['view_label'] = model.config.meta.view_label

    # An explore description will start indented at 2 spaces, so subsequent
    # lines should start indented at 2 + 2 spaces.
    if model.description:
        lookml['explore']['description'] = indent_multiline_description(model.description, 4)
    lookml['explore']['joins'] = [
        {
            'name': join.join,
            'type': join.type.value,
            'relationship': join.relationship.value,
            'sql_on': join.sql_on,
            'foreign_key': join.foreig_key,
            'view_label': join.view_label,
        }
        for join in model.config.meta.joins
    ]

    contents = lkml.dump(lookml)
    filename = f'{view_name}.model.lkml'
    return models.LookModelFile(filename=filename, contents=contents)
