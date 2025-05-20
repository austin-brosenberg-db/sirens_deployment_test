from yaml import Loader

from databricks.sirens.detection.rule.rule_entity import *

from databricks.sirens.utils.yaml_utils import *
from databricks.sirens.utils.config_utils import *


class DetectionRuleset:
    """rule collection set"""

    def __init__(self, *rules):
        self.rules = {}
        self.add_rules(*rules)

    def as_list(self):
        return [RuleEntity(name=self.rules[name]) for name in self.rules]

    @staticmethod
    def _check_duplicates(a, b):
        for rule_name in b.rules:
            if rule_name in a.rules:
                raise SirensDetectionException('Duplicate rule name found')

    def __add__(self, other):
        """+ operator, return combination of two rulesets"""
        DetectionRuleset._check_duplicates(self, other)
        return self.__or__(other)

    def __iadd__(self, other):
        """+= operator, add ruleset to self in place"""
        DetectionRuleset._check_duplicates(self, other)
        for name in other.rules:
            self.rules[name] = other.rules[name]
        return self

    def __or__(self, other):
        """| operator, add RHS ruleset to LHS ruleset, replacing rules with duplicate names"""
        combined = DetectionRuleset()
        for name in self.rules:
            combined.rules[name] = self.rules[name]
        for name in other.rules:
            if name in combined.rules:
                print(f"Replacing rule with duplicate name {name}")
            combined.rules[name] = other.rules[name]
        return combined

    def __ior__(self, other):
        """|= operator, add new rules and overwrite rules with duplicate names"""
        for name in other.rules:
            if name in self.rules:
                print(f"Replacing rule with duplicate name {name}")
            self.rules[name] = other.rules[name]
        return self

    def add_rules(self, *rules):
        if len(rules) == 0:
            return

        for rule in rules:
            if rule.name in self.rules:
                raise SirensDetectionException('Cannot use the same name for multiple rules.')
            self.rules[rule.name] = rule

    def disable_rule(self, *rule_names: str):
        for k in rule_names:
            removed_key = self.rules.pop(k, None)
            if not removed_key:
                print(f"{removed_key} rule is removed")
        return self

    @staticmethod
    def from_yaml(yaml_str):
        rule_dict = yaml.load(yaml_str, Loader=Loader)

        return DetectionRuleset(
            RuleEntity(
                name=rule_dict.get('name'),
                summary=rule_dict.get('summary'),
                severity=rule_dict.get('severity'),
                source=rule_dict.get('source'),
                sourceDetails=rule_dict.get('sourceDetails'),
                alertClass=rule_dict.get('alertClass'),
                ruleVersion=rule_dict.get('ruleVersion'),
                eventTime=rule_dict.get('eventTime'),
                alertedTime=rule_dict.get('alertedTime'),
                rawTime=rule_dict.get('rawTime'),
                actor=rule_dict.get('actor'),
                target=rule_dict.get('target'),
                attacks=rule_dict.get('attacks'),
                observables=rule_dict.get('observables'),
                sourceUuids=rule_dict.get('sourceUuids'),
                risk=rule_dict.get('risk'),
                riskScale=rule_dict.get('riskScale'),
                context=rule_dict.get('context'),
                uuid=rule_dict.get('uuid'),
                filter=rule_dict.get('filter'),
                time_window=rule_dict.get('time_window'),
                event_window=rule_dict.get('event_window'),
                compound_logic=rule_dict.get('compound_logic'),
                collect=rule_dict.get('collect')
            )
        )

    @staticmethod
    def load_yaml(glob_or_list, alertClass=None, schema_fields=None, env=''):
        if isinstance(glob_or_list, list):
            files = glob_or_list
        else:
            files = glob.glob(glob_or_list)

        rules = []
        for file in files:
            with open(file, 'rt') as yaml_file:
                rule_dicts = yaml.load(yaml_file, Loader=Loader)
                config_utils.sub_yaml_vars(rule_dicts, env)
                if isinstance(rule_dicts, dict):
                    rule_dicts = [rule_dicts]
                for rule_dict in rule_dicts:
                    rules.append(
                        RuleEntity(
                            schema_fields=rule_dict.get('schema_fields', schema_fields),
                            name=rule_dict.get('name'),
                            summary=rule_dict.get('summary'),
                            severity=rule_dict.get('severity'),
                            source=rule_dict.get('source'),
                            sourceDetails=rule_dict.get('sourceDetails'),
                            alertClass=rule_dict.get('alertClass', alertClass),
                            ruleVersion=rule_dict.get('ruleVersion'),
                            eventTime=rule_dict.get('eventTime'),
                            alertedTime=rule_dict.get('alertedTime'),
                            rawTime=rule_dict.get('rawTime'),
                            actor=rule_dict.get('actor'),
                            target=rule_dict.get('target'),
                            attacks=rule_dict.get('attacks'),
                            observables=rule_dict.get('observables'),
                            sourceUuids=rule_dict.get('sourceUuids'),
                            risk=rule_dict.get('risk'),
                            riskScale=rule_dict.get('riskScale'),
                            context=rule_dict.get('context'),
                            uuid=rule_dict.get('uuid'),
                            filter=rule_dict.get('filter'),
                            time_window=rule_dict.get('time_window'),
                            event_window=rule_dict.get('event_window'),
                            compound_logic=rule_dict.get('compound_logic'),
                            collect=rule_dict.get('collect')
                        )
                    )
        return DetectionRuleset(*rules)

    @staticmethod
    def to_yaml(ruleset):
        rule_dicts = []
        for name, rule in ruleset.rules.items():
            rec = OrderedDict([
                ('name', name),
                ('summary', rule.original_fields.get('summary')),
                ('severity', rule.original_fields.get('severity')),
                ('source', rule.original_fields.get('source')),
                ('sourceDetails', rule.original_fields.get('sourceDetails')),
                ('alertClass', rule.original_fields.get('alertClass')),
                ('ruleVersion', rule.original_fields.get('ruleVersion')),
                ('eventTime', rule.original_fields.get('eventTime')),
                ('alertedTime', rule.original_fields.get('alertedTime')),
                ('rawTime', rule.original_fields.get('rawTime')),
                ('actor', rule.original_fields.get('actor')),
                ('target', rule.original_fields.get('target')),
                ('attacks', rule.original_fields.get('attacks')),
                ('observables', rule.original_fields.get('observables')),
                ('sourceUuids', rule.original_fields.get('sourceUuids')),
                ('risk', rule.original_fields.get('risk')),
                ('riskScale', rule.original_fields.get('riskScale')),
                ('context', rule.original_fields.get('context')),
                ('uuid', rule.original_fields.get('uuid')),
                ('filter', rule.filter),
                ('time_window', rule.original_fields.get('time_window')),
                ('event_window', rule.original_fields.get('event_window')),
                ('compound_logic', rule.original_fields.get('compound_logic')),
                ('collect', rule.original_fields.get('collect'))
            ])
            rule_dicts.append(rec)
        return yaml.dump(rule_dicts)

    @staticmethod
    def dump_yaml(ruleset, outfile):
        with open(outfile, 'wt') as outf:
            outf.write(DetectionRuleset.to_yaml(ruleset))
