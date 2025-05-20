import re
from typing import Dict, List
from enum import IntEnum
from datetime import datetime, timedelta
from bisect import bisect_right
from pyspark.sql.functions import lit, concat, create_map, array, when, struct, current_timestamp, expr, sha1, \
    concat_ws, udf
from pyspark.sql.types import *
from databricks.sirens.detection.alert.sirens_alert import SirensAlert, AlertSchema
from databricks.sirens.exceptions import *


class RuleEntity:
    @staticmethod
    def __check_kwargs_length(kwargs, length):
        if len([x for x in kwargs.values() if x is not None]) > length:
            raise SirensDetectionException(f'Rule {kwargs["name"]} may take up to {length} fields, but received {len(kwargs)}')

    @staticmethod
    def __check_kwargs_inputs(kwargs, types):
        for key in types:
            if key not in kwargs.keys() or kwargs[key] is None:
                raise SirensDetectionException(f'{key} must be filled in for rule {kwargs["name"]}')

    @staticmethod
    def __enforce_schema(kwargs):
        mandatory_kwargs_keys = {'name', 'summary', 'severity', 'source', 'alertClass', 'ruleVersion', 'filter'}

        if kwargs.get('alertClass') == 'CORRELATION':
            max_kwargs_len = 25
            mandatory_kwargs_keys.add('time_window')
        else:
            max_kwargs_len = 23
            mandatory_kwargs_keys.add('eventTime')

        RuleEntity.__check_kwargs_length(kwargs, max_kwargs_len)
        RuleEntity.__check_kwargs_inputs(kwargs, mandatory_kwargs_keys)
        if kwargs['severity'].lower() not in [s.name.lower() for s in Serverity] \
                and kwargs['severity'][0] != '<' and kwargs['severity'][-1] != '>':
            raise SirensDetectionException(
                f'severity must be {[sev.name for sev in Serverity]} or a column value. severity given: {type(kwargs["severity"])}'
            )
        if kwargs['alertClass'].lower() not in [alertClass.name.lower() for alertClass in AlertClass]:
            raise SirensDetectionException(
                f'alertClass must be on of the following: {[alertClass.name for alertClass in AlertClass]}. alertClass given: {type(kwargs["alertClass"])}'
            )

    @staticmethod
    def parse_sevent_sequence(event_sequence, seen_events):
        # Initialize an empty list to hold the parsed segments
        parsed_segments = []

        # Define a pattern for the keyword
        keyword_pattern = r'\s*then\s*(?:within\s*\d+\s*(?:minute|minutes|second|seconds|hour|hours|day|days|week|weeks)\b)?'

        # Find all matches of the keyword pattern in the string
        matches = list(re.finditer(keyword_pattern, event_sequence, flags=re.IGNORECASE))

        # Initialize the start index for the first eventId
        start_index = 0
        # Iterate over the matches
        for match in matches:
            # The eventId is the part of the string from start_index to the start of the match
            event_id = event_sequence[start_index:match.start()].strip()
            # The keyword is the matched string
            keyword = event_sequence[match.start():match.end()].strip()
            # Update the start index for the next eventId
            start_index = match.end()

            # Append the eventId and keyword to the parsed_segments
            parsed_segments.append({'type': 'event_id', 'value': event_id})
            parsed_segments.append({'type': 'keyword', 'value': re.sub(' +', ' ', keyword).upper()})

        # Append the last eventId to the parsed_segments
        parsed_segments.append({'type': 'event_id', 'value': event_sequence[start_index:].strip()})

        last_type = None
        for i, x in enumerate(parsed_segments):
            if last_type is None:
                if x['type'] != 'event_id':
                    raise SirensDetectionException(f'event_sequence must start with an event_id. Sequence started with {x["value"]}.')
                if x['value'] not in seen_events:
                    raise SirensDetectionException(f'{x["value"]} is not a recognized event_id. The event_ids recognized are: {seen_events}.')
                last_type = 'event_id'
            elif last_type == 'event_id':
                if x['type'] != 'keyword':
                    raise SirensDetectionException(f'A keyword must follow an event_id. {x["value"]} at position {i} is invalid in {event_sequence}.')
                last_type = 'keyword'
            else:
                if x['type'] != 'event_id':
                    raise SirensDetectionException(f'An event_id must follow a keyword. {x["value"]} at position {i} is invalid in {event_sequence}.')
                if x['value'] not in seen_events:
                    raise SirensDetectionException(f'{x["value"]} is not a recognized event_id. The event_ids recognized are: {seen_events}.')
                last_type = 'event_id'

        if last_type != 'event_id':
            raise SirensDetectionException(f'event_sequence must end with an event_id. {parsed_segments[-1]["value"]} at position {len(parsed_segments)-1} is invalid.')

        return parsed_segments

    @staticmethod
    @udf(returnType=BooleanType())
    def sequential_filtering(sequences, event_id):
        event = sequences[event_id]
        event_sequence = event['__event_sequence']

        # sort all timestamps for each event's condition and reset_on timestamps
        seen_events = set()
        for key in event:
            if isinstance(key, str):
                continue

            if key['event_id'] is None:
                raise SirensDetectionException(f'event_id must be specified for each element in a sequence.')
            if key['event_id'] in seen_events:
                raise SirensDetectionException(f"An event_id can't be used twice. Duplicate ID: {key['event_id']}")
            seen_events.add(key['event_id'])

            key['condition'].sort()
            if key['reset_on']:
                key['reset_on'].sort()

        # parse the event_sequence string
        event_sequence = RuleEntity.parse_sevent_sequence(event_sequence, seen_events)

        def dfs(last_time, idx, time_constraint, reset_on_times, current_times):
            # If not an event, check if the next event has a time constraint to the previous event. If so, parse it into seconds
            if event_sequence[idx]['type'] == 'keyword':
                if "WITHIN" in event_sequence[idx]['value']:
                    x = event_sequence[idx]['value'].split()
                    time, unit = int(x[-2]), x[-1]
                    if unit in ["SECOND", "SECONDS"]:
                        time *= 1
                    elif unit in ["MINUTE", "MINUTES"]:
                        time *= 60
                    elif unit in ["HOUR", "HOURS"]:
                        time *= 3600
                    elif unit in ["DAY", "DAYS"]:
                        time *= 86400
                    elif unit in ["WEEK", "WEEKS"]:
                        time *= 604800
                    else:
                        raise SirensDetectionException(
                            f"""
                            {unit} is not a valid time unit in {event_sequence}.
                            Valid units are: SECOND, SECONDS, MINUTE, MINUTES, HOUR, HOURS, DAY, DAYS, WEEK, WEEKS
                            """
                        )

                    time_constraint = time
                # Move to the next event after processing the keyword linking events
                dfs(last_time, idx + 1, time_constraint, reset_on_times, current_times)
            else:
                # get the index of the first time greater than the last event's timestamp
                min_time_idx = bisect_right(event[event_sequence[idx]['value']]['condition'], last_time)
                # if there is no time greater, it will be the end of the list. return since this sequence is invalid
                if min_time_idx == len(event[event_sequence[idx]['value']]['condition']):
                    return

                # iterate through each timestamp that may still be valid
                for time in event[event_sequence[idx]['value']]['condition'][min_time_idx:]:
                    # if there is a time constraint and the time does not fall within range of the last event's time + constraint
                    # stop iterating since this sequence is invalid and all following sequences will be as well
                    if time_constraint != 0 and time > last_time + timedelta(seconds=time_constraint):
                        break

                    # if the event has a reset on condition, find the first valid time which can reset the sequence.
                    # append all times from there onwards into an array of timestamps which can invalidate the sequence
                    can_reset = []
                    if event[event_sequence[idx]['value']]['reset_on'] is not None:
                        can_reset_idx = bisect_right(event[event_sequence[idx]['value']]['reset_on'], time)
                        if can_reset_idx != len(event[event_sequence[idx]['value']]['reset_on']):
                            can_reset = event[event_sequence[idx]['value']]['reset_on'][can_reset_idx:]

                    # if this is the last event in the sequence, the sequence is valid through this stage. add it to the array of candidates
                    if idx == len(event_sequence) - 1:
                        candidates.append([current_times, reset_on_times])
                    else:
                        # move on if there are more events
                        dfs(time, idx + 1, 0, reset_on_times.copy() + can_reset.copy(), current_times.copy() + [time])

        # get all sequences which uphold with respect to order of conditions and time constraints
        candidates = []
        dfs(datetime(year=1996, month=9, day=7), 0, 0, [], [])

        # check each candidate against the associated times that may invalidate the sequence
        for candidate in candidates:
            reset = False
            for reset_time in candidate[1]:
                # for each time that can reset the candidate sequence, check if it falls within the sequence
                # if yes, the sequence is invalid
                if bisect_right(candidate[0], reset_time) != len(candidate[0]):
                    reset = True
                    break

            # if there is a candidate which does not have an invalidating event, there exists a sequence which upholds all conditions. return true
            if not reset:
                return True

        # if no candidate upholds to all conditions. return false
        return False

    @staticmethod
    def _field_expr_sub(field, string, arr_threshold='> 0', depth=0):
        if depth == 0:
            first_depth = ''
            second_depth = '2'
        else:
            first_depth = str(depth+1)
            second_depth = str(depth+2)

        subbed_quotes = []
        for x in re.finditer(r'\'(.*?)\'|\"(.*?)\"', string):
            y = x.groups()[0] if x.groups()[0] is not None else x.groups()[1]
            if field in y:
                subbed_quotes.append((y, '_'*len(y)))
                string = string[:x.span()[0]+1] + '_'*len(y) + string[x.span()[1]-1:]

        valid_prefixes = '(^| |\\(|=|<|>|)'
        valid_postfixes = '($| |\\)|=|<|>|!|\\[|\\.)'

        if '[]' in field:
            field = field.replace('[]', '\\[\\]')
            field_capture_group = f'({field}[\\w|\\.|( |!|=|<|>)]+)'
            re_match = re.finditer(f'{valid_prefixes}{field_capture_group}{valid_postfixes}', string)
            for match in re_match:
                x = match.groups()[1]
                array_field, struct_field = x.split('[]')
                sub_window = string[match.span()[0]:]
                sub_window = sub_window[sub_window.find(x) + len(x):].strip()
                sub_window = sub_window[:re.search('AND|OR|$', sub_window, re.IGNORECASE).span()[0]].strip()
                condition = ' ' + sub_window
                x = f'SIZE(FILTER(x{first_depth}.{array_field}, x{second_depth} -> x{second_depth}{struct_field} {condition})) {arr_threshold}'
                string = re.sub(f'{valid_prefixes}{field_capture_group}{valid_postfixes}*{condition}', fr'\1{x}\3',
                                string)
        else:
            string = re.sub(f'{valid_prefixes}{field}{valid_postfixes}', fr'\1x{first_depth}.{field}\2', string)

        for x, y in subbed_quotes:
            string = string.replace(y, x, 1)

        return re.sub(' {2,}', ' ', string).strip()

    def _get_event_window(self, rule):
        time_window = rule['time_window']
        event_window = rule.get('event_window')

        for x in self._schema_fields:
            if event_window is not None:
                event_window = RuleEntity._field_expr_sub(x, event_window)
            time_window = RuleEntity._field_expr_sub(x, time_window)

        if event_window is not None:
            window = f"{time_window} AND {event_window}"
        else:
            window = time_window

        return window
    

    @staticmethod
    def _collect_events(field, condition="TRUE", array_column='alerts'):
        if "[]" in field:
            field = field.split('[].')

            array_field_union = f"""
                AGGREGATE(
                    x.{field[0]},
                    ARRAY(''),
                    (acc2, x2) -> ARRAY_UNION(acc2, IF(x2.{field[1]} IS NULL, ARRAY(''), ARRAY(STRING(x2.{field[1]}))))
                    )
                """

            field_filter = f"(CASE WHEN {condition} THEN {array_field_union} ELSE ARRAY('') END)"

        else:
            field_filter = f"(CASE WHEN {condition} THEN ARRAY(IF(x.{field} IS NULL, '', STRING(x.{field}))) ELSE ARRAY('') END)"

        clause = f"""
            ARRAY_REMOVE(
            AGGREGATE(
                {array_column}, 
                ARRAY(''), 
                (acc, x) -> ARRAY_UNION(
                acc, {field_filter}
                )
            ), ''
            )
            """
        return clause

    def _get_clause(self, type, field, condition, threshold, event_id=None, array_column='alerts'):
        if condition is not None:
            for x in self._schema_fields:
                condition = RuleEntity._field_expr_sub(x, condition, '> 0')
        else:
            condition = "TRUE"

        if type == 'count':
            if condition == "TRUE":
                raise Exception(f'condition must be specified for {type} filters.')

            if threshold is None:
                threshold = '> 0'

            clause = f"""
                SIZE(
                  FILTER(
                    {array_column}, 
                    x -> {condition}
                    )
                  ) {threshold}
                """
        elif type == 'cardinality':
            if field is None:
                raise Exception(f'field must be specified for {type} filters.')

            if threshold is None:
                raise Exception(f'threshold must be specified for {type} filters.')
            
            clause = f"""
                SIZE(
                {RuleEntity._collect_events(field, condition, array_column)}
                ) {threshold}
            """
        elif type in {'sum', 'product', 'difference', 'belief'}:
            if field is None:
                raise Exception(f'field must be specified for {type} filters.')

            if threshold is None:
                raise Exception(f'threshold must be specified for {type} filters.')

            val_type = 'DOUBLE'

            if type == 'sum':
                math_expr = f'acc + {val_type}(x.{field})'
                starting_val = 0
            elif type == 'product':
                math_expr = f'acc * {val_type}(x.{field})'
                starting_val = 1
            elif type == 'difference':
                math_expr = f'acc - {val_type}(x.{field})'
                starting_val = 0
            else:
                # belief
                math_expr = f'''
                IF(
                    {val_type}(x.{field}) < 0, acc + acc * {val_type}(x.{field}),
                     acc + {val_type}(x.{field}) - acc * {val_type}(x.{field})
                )
                '''
                starting_val = 0

            field_condition = f"{val_type}({starting_val}), (acc, x) -> (CASE WHEN {condition} THEN {math_expr} ELSE acc END)"

            clause = f"""
                AGGREGATE(
                  {array_column}, {field_condition}
                ) {threshold}
                """
            if type == 'belief':
                clause = f"""
                BROUND(
                    {clause.replace(f') {threshold}', f'), 3')}
                ) {threshold}
                """
        elif type == 'sequence':
            clause = f"sequential_filtering(sequences, '{event_id}')"
        else:
            raise Exception(f'Unsupported type: {type} given.')

        return clause

    def _get_timestamps(self, type, condition, array_column='alerts', timestamp_column="eventTime"):
        if condition is None:
            return f"NULL AS {type}"

        for x in self._schema_fields:
            condition = RuleEntity._field_expr_sub(x, condition, '> 0')
        clause = f"""
        TRANSFORM(
            FILTER(
                {array_column}, 
                x -> {condition}
            ),
        x -> x.{timestamp_column}) AS {type}
        """

        return clause

    def _get_event_sequences(self, sequences):
        if len(sequences) == 0:
            return lit(None)

        column = "STRUCT(\n"
        for sequence in sequences:
            column += f"""STRUCT(\n'{sequence["event_sequence"]}' AS __event_sequence,"""
            events = sequence['events']
            array_column = sequence.get('array_column', 'alerts')
            timestamp_column = sequence.get('timestamp_column', "eventTime")

            for event in events:
                condition = event['condition']
                reset_on = event.get('reset_on')

                column += f"""
                STRUCT(
                    {self._get_timestamps('condition', condition, array_column, timestamp_column)},
                    {self._get_timestamps('reset_on', reset_on, array_column, timestamp_column)},
                    "{event['event_id']}" AS event_id
                ) AS {event['event_id']},"""
            column = column[:-1]
            column += f') AS {sequence["event_id"]},'

        column = column[:-1]
        column += ')'

        return expr(column)

    def get_collect_fields(self, rule):
        if 'collect' not in rule:
            return

        for key, val in rule['collect'].items():
            self.collect_fields[key] = RuleEntity._collect_events(val)

    def _compound_clauses(self, rule):
        clauses = []
        filter = rule['filter']

        seen = set()
        event_ids = []
        sequences = []
        for x in filter:
            type = x['type']
            if type == 'sequence':
                sequences.append(x)
            field = x.get('field')
            condition = x.get('condition', "TRUE")
            threshold = x.get('threshold')
            array_column = x.get('array_column', 'alerts')

            event_id = x.get('event_id')
            if event_id is None:
                raise SirensDetectionException(f'event_id must be specified for each element of array filters.')
            if event_id in seen:
                raise SirensDetectionException(f"An event_id can't be used twice. Duplicate ID: {event_id}")
            event_ids.append(event_id)
            seen.add(event_id)

            clauses.append(
                self._get_clause(
                    type=type, field=field, condition=condition, threshold=threshold,
                    event_id=event_id, array_column=array_column)
            )

        self.sequences = self._get_event_sequences(sequences)
        compound_logic = rule.get('compound_logic')
        if compound_logic is None:
            compound_logic = ""
            for i in range(1, len(filter)):
                compound_logic += str(event_ids[i-1]) + " AND "
            compound_logic += str(event_ids[-1])

        for i in range(1, len(clauses) + 1):
            compound_logic = compound_logic.replace(str(event_ids[i-1]), clauses[i - 1])

        return compound_logic

    @staticmethod
    def __check_unfilled(kwargs, key):
        if kwargs.get(key) is None or (isinstance(kwargs[key], str) and kwargs[key].strip() == ''):
            return True
        else:
            return False

    @staticmethod
    def _convert_dict(dict):
        pyspark_map = []
        for key, val in dict.items():
            if isinstance(key, str):
                pyspark_map.append(RuleEntity._convert_string(key).cast('string'))
            else:
                pyspark_map.append(lit(str(key)))

            if isinstance(val, str):
                pyspark_map.append(RuleEntity._convert_string(val).cast('string'))
            else:
                pyspark_map.append(lit(str(val)))

        return create_map(*pyspark_map)

    @staticmethod
    def _convert_string(message):
        pyspark_message = []

        is_col = False
        for x in re.split(r'(\<(.*?)\>)', message):
            if len(x) == 0:
                continue
            if x[0] == '<':
                is_col = True
                continue

            if is_col:
                pyspark_message.append(expr(x))
                is_col = False
            else:
                pyspark_message.append(lit(x))

        if len(pyspark_message) > 1:
            return concat_ws("", *pyspark_message)
        elif len(pyspark_message) == 1:
            return pyspark_message[0]
        else:
            return lit(None)

    @staticmethod
    def convert_mutable(val):
        try:
            return array([RuleEntity.convert_val(x) for x in val])
        except:
            return array([lit(str(x)) for x in val])

    @staticmethod
    def convert_val(val):
        if val is None:
            return lit(None)
        elif isinstance(val, Dict):
            return RuleEntity._convert_dict(val)
        elif isinstance(val, str):
            return RuleEntity._convert_string(val)
        elif isinstance(val, list) or isinstance(val, tuple) or isinstance(val, set):
            return RuleEntity.convert_mutable(val)
        elif isinstance(val, int):
            return lit(val)
        elif isinstance(val, float):
            return lit(val).cast('float')
        else:
            return lit(str(val))

    @staticmethod
    def convert_struct(vals):
        return struct(
            [RuleEntity.convert_val(vals[x]).alias(x) for x in vals]
        )

    def __init__(self, **kwargs):
        if kwargs.get('schema_fields') is None:
            self._schema_fields = SirensAlert.schema_field_set
            self._schema_fields.discard('rawJson')
        else:
            self._schema_fields = set(kwargs['schema_fields'])

        if 'observables' in self._schema_fields:
            [self._schema_fields.add(x) for x in [
                'observables.ipAddresses[]',
                'observables.domains[]',
                'observables.fileHashes[]',
                'observables.filePaths[]',
                'observables.urls[]',
                'observables.processes[]'
            ]]
            self._schema_fields.discard('observables')
        if 'attacks' in self._schema_fields:
            self._schema_fields.add('attacks[]')
            self._schema_fields.discard('attacks')
        if 'sourceUuids' in self._schema_fields:
            self._schema_fields.add('sourceUuids[]')
            self._schema_fields.discard('sourceUuids')

        self.__enforce_schema(kwargs)

        self.name = None
        self.filter = None
        self.original_fields = {}
        self.pyspark_fields = {}
        self.sequences = lit(None)
        self.collect_fields = {}

        if RuleEntity.__check_unfilled(kwargs, 'alertedTime'):
            kwargs['alertedTime'] = "<CURRENT_TIMESTAMP>"

        if kwargs['alertClass'] == 'CORRELATION':
            if RuleEntity.__check_unfilled(kwargs, 'eventTime'):
                kwargs['eventTime'] = '<__last_seen>'
            if RuleEntity.__check_unfilled(kwargs, 'sourceUuids'):
                kwargs['sourceUuids'] = '<__uuids>'

            default_context = {
                'key': '<key>',
                'first_seen': '<__first_seen>',
                'last_seen': '<__last_seen>'
            }
            if RuleEntity.__check_unfilled(kwargs, 'context'):
                kwargs['context'] = default_context
            else:
                kwargs['context'] = {**kwargs['context'], **default_context}

            if RuleEntity.__check_unfilled(kwargs, 'observables'):
                kwargs['observables'] = '<__observables>'

            if RuleEntity.__check_unfilled(kwargs, 'uuid'):
                kwargs['uuid'] = "<sha1(concat_ws(' ', key, __first_seen, __last_seen, __uuids, window, filter))>"
        else:
            if RuleEntity.__check_unfilled(kwargs, 'uuid'):
                kwargs['uuid'] = "<uuid()>"

        for key, val in kwargs.items():
            key = str(key)
            self.original_fields[key] = val

            if key == 'name':
                if not isinstance(val, str):
                    raise SirensDetectionException(f"name must be a string. Type given: {type(val)}")
                self.name = val

            if key == 'filter':
                if kwargs['alertClass'] == 'CORRELATION':
                    if isinstance(kwargs['filter'], dict):
                        kwargs['filter'] = [kwargs['filter']]
                    self.filter = self._compound_clauses(kwargs)
                else:
                    if isinstance(kwargs['filter'], dict):
                        kwargs['filter'] = [kwargs['filter']]
                        self.filter = self._compound_clauses(kwargs)
                    elif isinstance(kwargs['filter'], list):
                        self.filter = self._compound_clauses(kwargs)
                    else:
                        self.filter = str(val)
            elif val is None:
                self.pyspark_fields[key] = lit(None)
            elif val == f'<{key}>':
                self.pyspark_fields[key] = expr(key)
            elif key in {'actor', 'target', 'observables'} and isinstance(val, dict):
                def __relative_key_order(_key):
                    return [_x.split(':')[0] for _x in AlertSchema[_key].dataType.simpleString().split('struct<')[1][:-1].split(',')]
                val = {k: val.get(k) for k in __relative_key_order(key)}
                self.pyspark_fields[key] = RuleEntity.convert_struct(val)
            elif key == 'attacks':
                self.pyspark_fields[key] = array([RuleEntity.convert_struct(x) for x in val])
            elif key == 'time_window':
                self.pyspark_fields['window'] = self._get_event_window(kwargs)
            elif key == 'collect':
                self.get_collect_fields(kwargs)
            elif key == 'event_window':
                continue
            else:
                self.pyspark_fields[key] = RuleEntity.convert_val(val)


class AlertClass(IntEnum):
    UNDEFINED = 0
    ALERT = 1         # A single alert received from an alerting data source.
    CORRELATION = 2   # Several alerts combined into a correlation event.
    HEARTBEAT = 3     # Not an alert at all, just a message to indicate the alert source is alive.
    INFO = 4          # An informational event, should not be an alert but provides context

class Serverity(IntEnum):
    UNDEFINED = 0
    INFO = 1
    LOW = 2
    MEDIUM = 3
    HIGH = 4
    CRITICAL = 5

class EntityType(IntEnum):
    UNDEFINED = 0
    USER = 1
    HOST = 2
    APPLICATION = 3
    USER_GROUP = 4
    INFRASTRUCTURE = 5


class KillchainStage(IntEnum):
    UNDEFINED = 0
    RECONNAISSANCE = 1
    WEAPONIZATION = 2
    DELIVERY = 3
    EXPLOITATION = 4
    INSTALLATION = 5
    COMMAND_AND_CONTROL = 6
    ACTIONS_ON_OBJECTIVES = 7

class MitreTaxonomyType(IntEnum):
    ENTERPRISE = 0
    MOBILE = 1
    ICS = 2

class MitreEnterpriseTactic(IntEnum):
    # The values deliberately mirror those allocated by MITRE
    # https://attack.mitre.org/tactics/enterprise/
    UNDEFINED = 0
    RECONNAISSANCE = 43
    RESOURCE_DEVELOPMENT = 42
    INITIAL_ACCESS = 1
    EXECUTION = 2
    PERSISTENCE = 3
    PRIVILEGE_ESCALATION = 4
    DEFENSE_EVASION = 5
    CREDENTIAL_ACCESS = 6
    DISCOVERY = 7
    LATERAL_MOVEMENT = 8
    COLLECTION = 9
    COMMAND_AND_CONTROL = 11
    EXFILTRATION = 10
    IMPACT = 40


class MitreMobileTactic(IntEnum):
    # The values deliberately mirror those allocated by MITRE
    # https://attack.mitre.org/tactics/mobile/
    # Note that there is no overlap in numbering (except for "UNDEFINED") with MitreEnterpriseTactic
    UNDEFINED = 0
    INITIAL_ACCESS = 27
    EXECUTION = 41
    PERSISTENCE = 28
    PRIVILEGE_ESCALATION = 29
    DEFENSE_EVASION = 30
    CREDENTIAL_ACCESS = 31
    DISCOVERY = 32
    LATERAL_MOVEMENT = 33
    COLLECTION = 35
    COMMAND_AND_CONTROL = 37
    EXFILTRATION = 36
    IMPACT = 34
    NETWORK_EFFECTS = 38
    REMOTE_SERVICE_EFFECTS = 39


class MitreICSTactic(IntEnum):
    # The values deliberately mirror those allocated by MITRE
    # https://attack.mitre.org/tactics/ics/
    # Note that there is no overlap in numbering (except for "UNDEFINED") with MitreEnterpriseTactic
    UNDEFINED = 0
    INITIAL_ACCESS = 108
    EXECUTION = 104
    PERSISTENCE = 110
    PRIVILEGE_ESCALATION = 111
    EVASION = 103
    DISCOVERY = 102
    LATERAL_MOVEMENT = 109
    COLLECTION = 100
    COMMAND_AND_CONTROL = 101
    INHIBIT_RESPONSE_FUNCTION = 107
    IMPAIR_PROCESS_CONTROL = 106
    IMPACT = 105