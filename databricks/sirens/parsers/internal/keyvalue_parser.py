from typing import List

import pandas as pd
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType, MapType


class GenericKVParser:
    def __init__(self, delimiter=" ", value_sep="="):
        self.delimitor = delimiter
        self.value_sep = value_sep

    def _extract_value(self, text, start_position):
        """
        Extracts a string from the given text starting from the specified position.
        The string ends when an end quote is encountered (if quoted) or a white space is found (if not quoted).
        """
        # Initialize variables
        result = ""
        in_quotes = False
        quote_char = None

        # Iterate through the text starting from the specified position
        for i in range(start_position, len(text)):
            char = text[i]

            if char in ('"', "'"):
                # Toggle in_quotes flag when encountering a quote character
                if i == start_position and not in_quotes:
                    in_quotes = True
                    quote_char = char
                elif in_quotes and char == quote_char:
                    # End the string if the matching end quote is found
                    return result, i + 1
            elif char in self.delimitor and not in_quotes:
                break
            else:
                # Append the character to the result
                result += char
        if i == (len(text) - 1):
            i += 1
        return result, i

    def _skip(self, cur: int, text: str, chars=" "):
        while cur < len(text) and text[cur] in chars:
            cur += 1
        return cur

    def _find_fieldname(self, cur, text, fieldNames, keyValueSep="="):
        if fieldNames:
            for k in fieldNames:
                if text[cur:].startswith(k + keyValueSep):
                    return k.replace("-", "_"), cur + len(k) + 1
            print(f"Cannot find any field name from {text[cur:]}")
        else:
            sep_pos = text.find(keyValueSep, cur)
            if sep_pos >= 0:
                fn = text[cur:sep_pos].strip()
                return fn.replace("-", "_"), sep_pos + len(keyValueSep)
            else:
                print(f"Cannot find any field name from {text[cur:]}")
        return "", cur

    # option to parse key-value pairs without field names
    # unhex proctitle
    def parse_kv(self, text: str, fieldNames=[]):
        i = 0
        dct = dict()  # init_dct(field_names)
        encoded_bytes = text.encode("utf-8", errors="ignore")
        text = encoded_bytes.decode("utf-8").replace('\x1d', ' ')
        while i < len(text):
            i = self._skip(i, text)
            if i >= len(text): break
            field_name, i = self._find_fieldname(i, text, fieldNames, keyValueSep=self.value_sep)
            if not field_name:
                break
            field_value, i = self._extract_value(text, i)
            dct[field_name] = field_value
        return dct

    def get_all_fields(self, fieldNames):
        all_fields = []
        cleaned_fields = []
        for fn in fieldNames:
            cleaned_fn = fn.replace("-", "_")
            cleaned_fields.append(cleaned_fn)
            all_fields.append(cleaned_fn)
        return all_fields

    def parse_kv_udf(self, colname, fieldNames):
        @pandas_udf(returnType=MapType(StringType(), StringType()))
        def parse_kvp_lines(s: pd.Series) -> pd.Series:
            results = []
            for line in s:
                results.append(self.parse_kv(line, fieldNames))
            return pd.Series(results)

        return parse_kvp_lines(colname)

    def parse(self, src_df: DataFrame, rawCol: str = "value", recordCol: str = "record", fieldNames: List[str] = []):
        return src_df.withColumn(recordCol, self.parse_kv_udf(rawCol, fieldNames))

    def expand_fields(self, df: DataFrame, fieldNames: List[str], recordCol="record"):
        all_fields = self.get_all_fields(fieldNames)
        cols = [f"{recordCol}.{field}" for field in all_fields]
        return df.selectExpr(recordCol, *cols)

    def post_process(self, df: DataFrame):
        return df
