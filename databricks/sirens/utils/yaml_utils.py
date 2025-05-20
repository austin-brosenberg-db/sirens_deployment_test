import yaml
from collections import OrderedDict


class literal(str):
    pass


def literal_presenter(dumper, data):
    # pyyaml's Emitter won't honor the multiline style if there are newlines
    # preceded by whitespace
    data = '\n'.join([line.strip() for line in data.strip().splitlines()])
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")


yaml.add_representer(literal, literal_presenter)


def represent_ordereddict(dumper, data):
    value = []

    for item_key, item_value in data.items():
        node_key = dumper.represent_data(item_key)
        node_value = dumper.represent_data(item_value)
        value.append((node_key, node_value))
    return yaml.nodes.MappingNode(u"tag:yaml.org,2002:map", value)


yaml.add_representer(OrderedDict, represent_ordereddict)
