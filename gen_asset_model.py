import logging as log
import json
import sys
import os
import argparse
import time

from itertools import chain
from pathlib import Path

from dataclasses import dataclass

from typing import List

from adlinktech.datariver import DataRiver, JSonTagGroupRegistry, JSonThingClassRegistry, ThingClass, InvalidArgumentError
from adlinktech.datariver import IotType

DATA_RIVER_CONFIG_ENV_VAR = 'ADLINK_DATARIVER_URI'

# ------------------------------------------------------------------------
# ---------------------- Configure logging -------------------------------
# ------------------------------------------------------------------------

log.basicConfig(format='[ %(levelname)s ] %(message)s', level=log.DEBUG, stream=sys.stdout)

# ------------------------------------------------------------------------
# ---------------------- Argument parser ---------------------------------
# ------------------------------------------------------------------------
def create_argparser():
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument('-tg', '--tag_groups', type=str, required=False,
                        help='Path to a directory of tag groups to load.', default=None)
    parser.add_argument('-tc', '--thing_classes', type=str, required=False,
                        help='Path to a directory of tag groups to load.', default=None)
    return parser.parse_args()


# ------------------------------------------------------------------------
# ---------------------- AWS Sitewise classes and functions --------------
# ------------------------------------------------------------------------
@dataclass
class AwsModelProperty:
    name : str
    data_type : str
    unit: str

    def _type_dict(self):
        return {}

    def to_dict(self):
        result = {'name' : self.name, 'dataType' : self.data_type, 'type' : self._type_dict()}
        if self.unit is not None:
            result['unit'] = self.unit
        return result

@dataclass
class AwsModelAttribute(AwsModelProperty):
    default_value: str

    def _type_dict(self):
        return {'attribute' : {'defaultValue' : self.default_value}}

@dataclass
class AwsModelMeasurement(AwsModelProperty):

    def _type_dict(self):
        return {'measurement' : {}}


@dataclass
class AwsModelVariable:
    name : str
    property_id : str
    hierarchy_id : str

    def to_dict(self):
        result = {'name' : self.name}
        value = {}
        if self.property_id is not None:
            value['propertyId'] = self.property_id
        if self.hierarchy_id is not None:
            value['hierarchyId'] = self.hierarchy_id
        result['value'] = value
        return result


@dataclass
class AwsModelTransform(AwsModelProperty):
    expression : str
    variables : List[AwsModelVariable]

    def _type_dict(self):
        return {'transform' : {
            'expression' : self.expression,
            'variables' : [v.to_dict() for v in self.variables]
        }}


@dataclass
class AwsModelMetricWindow:
    interval : str

    def to_dict(self):
        return {'tumbling' : {'interval' : self.interval}}


@dataclass
class AwsModelMetric(AwsModelProperty):
    expression : str
    variables : List[AwsModelVariable]
    window : AwsModelMetricWindow


    def _type_dict(self):
        result = {'metric':{
            'expression' : self.expression,
            'variables' : [v.to_dict() for v in self.variables],
            'window' : self.window.to_dict()
        }}

@dataclass
class AwsModelAssertHierarchy:
    name : str
    child_id : str

    def to_dict(self):
        return {
            'name' : self.name,
            'childAssetModelId' : self.child_id
        }


@dataclass
class AwsAssetModel:
    name : str
    description : str
    properties : List[AwsModelProperty]
    hierarchies : List[AwsModelAssertHierarchy]


    def to_dict(self):
        return {
            'assetModelName': self.name,
            'assetModelDescription' : self.description,
            'assetModelProperties' : [p.to_dict() for p in self.properties],
            'assetModelHiearchies' : [h.to_dict() for h in self.hierarchies]
        }

# ------------------------------------------------------------------------
# ---------------------- Model loading functions -------------------------
# ------------------------------------------------------------------------
def get_river_config_uri() -> str:
    '''
    Get the Data River config URI from the Environment. The config should available in the ADLINK_DATARIVER_URI
    environment variable.

    If the environment variable is not set then the default configuration provided with the Edge SDK will be used. The
    default configuration can be found at $EDGE_SDK_HOME/etc/config/default_datariver_config_v1.2.xml.

    :return: The URI for the Data River configuration to be used in the application.
    '''
    river_conf_env = os.getenv(DATA_RIVER_CONFIG_ENV_VAR)
    if river_conf_env is None:
        river_conf_env = os.environ[DATA_RIVER_CONFIG_ENV_VAR] = \
            'file://{}/etc/config/default_datariver_config_v1.2.xml'.format(os.environ['EDGE_SDK_HOME'])
        log.info(f'Environment variable {DATA_RIVER_CONFIG_ENV_VAR} not set. Defaulting to: {str(river_conf_env)}')

    return river_conf_env


def load_thing_classes(dr : DataRiver, thing_class_dir : str):
    tcr = JSonThingClassRegistry()
    for entry in os.scandir(thing_class_dir):
        if entry.is_file() and entry.path.endswith('.json'):
            log.info(f'Loading thing class: {entry.path}')
            try:
                tcr.register_thing_classes_from_uri(f'file://{entry.path}')
            except Exception as e:
                log.error(f'Unable to load thing class: {entry.path} : {e}')
                pass
    dr.add_thing_class_registry(tcr)


def load_tag_groups(dr : DataRiver, tag_group_dir : str):
    tgr = JSonTagGroupRegistry()

    for entry in os.scandir(tag_group_dir):
        if entry.is_file() and entry.path.endswith('.json'):
            log.info(f'Loading tag group: {entry.path}')
            try:
                tgr.register_tag_groups_from_uri(f'file://{entry.path}')
            except Exception as e:
                log.error(f'Unable to load tag group: {entry.path} : {e}')
                pass
    dr.add_tag_group_registry(tgr)


# ------------------------------------------------------------------------
# ---------------------- Model mapping functions -------------------------
# ------------------------------------------------------------------------
def find_tag_group(tgr, tag_group_name):
    retry_count = 50

    for i in range(0, 50):
        try:
            return True, tgr.find_tag_group(tag_group_name)
        except InvalidArgumentError:
            pass
        time.sleep(100.0 / 1000.0)

    return False, None

type_mapping = {
    IotType.TYPE_BYTE : 'INTEGER',
    IotType.TYPE_UINT16 : 'INTEGER',
    IotType.TYPE_UINT32 : 'INTEGER',
    IotType.TYPE_UINT64 : None,
    IotType.TYPE_INT8 : 'INTEGER',
    IotType.TYPE_INT16 : 'INTEGER',
    IotType.TYPE_INT32 : 'INTEGER',
    IotType.TYPE_INT64 : 'INTEGER',
    IotType.TYPE_FLOAT32 : 'DOUBLE',
    IotType.TYPE_FLOAT64 : 'DOUBLE',
    IotType.TYPE_BOOLEAN : 'BOOLEAN',
    IotType.TYPE_STRING : 'STRING',
    IotType.TYPE_CHAR : None,
    IotType.TYPE_BYTE_SEQ : None,
    IotType.TYPE_UINT16_SEQ : None,
    IotType.TYPE_UINT32_SEQ : None,
    IotType.TYPE_UINT64_SEQ : None,
    IotType.TYPE_INT8_SEQ : None,
    IotType.TYPE_INT16_SEQ : None,
    IotType.TYPE_INT32_SEQ : None,
    IotType.TYPE_INT64_SEQ : None,
    IotType.TYPE_FLOAT32_SEQ : None,
    IotType.TYPE_FLOAT64_SEQ : None,
    IotType.TYPE_BOOLEAN_SEQ : None,
    IotType.TYPE_STRING_SEQ : None,
    IotType.TYPE_CHAR_SEQ : None,

}

def map_tag(prefix, tag):
    type = type_mapping.get(tag.kind, None)
    return AwsModelMeasurement(f'{prefix}{tag.name}', type, tag.unit)


def map_output_tag_group(tgr, tg):
    properties = []
    name = tg.name
    tg_id = tg.output_tag_group
    res, tg_definition = find_tag_group(tgr, tg_id)
    if res:
        type = tg_definition.top_level_type
        for tag in type.tags:
            aws = map_tag(f'{name}_', tag)
            if aws is not None:
                properties.append(aws)

    return properties

def map_thing_class(tc : ThingClass, tgr):
    name = tc.name
    description = tc.description
    hierarchies = []
    properties = []
    properties.append(AwsModelAttribute('contextId', 'STRING', '', ''))
    properties.append(AwsModelAttribute('description', 'STRING', '', ''))
    mapped_outputs = [map_output_tag_group(tgr, tg) for tg in tc.output_tag_groups]
    if(len(mapped_outputs) > 0):
        properties.extend(list(chain.from_iterable(mapped_outputs)))


    model = AwsAssetModel(name, description, properties, hierarchies)

    return model


# ------------------------------------------------------------------------
# ---------------------- Script entry point ------------------------------
# ------------------------------------------------------------------------
def main():
    params = vars(create_argparser())

    data_river = DataRiver.get_instance(get_river_config_uri())

    if params['tag_groups'] is not None:
        load_tag_groups(data_river, params['tag_groups'])

    if params['thing_classes'] is not None:
        load_thing_classes(data_river, params['thing_classes'])

    tgr = data_river.discovered_tag_group_registry

    tcr = data_river.discovered_thing_class_registry
    print('Thing Classes')
    print('_____________')
    for tc in tcr.thing_classes:
        print(f'    {tc.name}')
        model = map_thing_class(tc, tgr)
        Path(tc.context).mkdir(parents=True, exist_ok=True)
        with open(os.path.join(tc.context, f'{tc.name}.json'), 'w') as outfile:
            json.dump(model.to_dict(), outfile)


if __name__ == '__main__':
    sys.exit(main())