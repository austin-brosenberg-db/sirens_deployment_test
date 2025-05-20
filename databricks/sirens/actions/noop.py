import json
from . import plugins
from typing import List, Optional, Dict, Union

from pyspark.sql import DataFrame

from databricks.sirens.internal.baseaction import BaseAction, ActionResult
from databricks.sirens.logging import get_logger

logger = get_logger(__name__)


@plugins.register
class Action(BaseAction):
    def __init__(self, actionConfigObj: dict = {}):
        super().__init__(actionConfigObj)
        pass

    @plugins.register
    def do_action(self, requested_action: str, params: Optional[Dict],
                  body: Optional[Union[List[Dict], DataFrame]]) -> ActionResult:
        """return noop for testing purposes.

        :param df: _description_
        :type df: DataFrame
        """

        return ActionResult(True, "noop complete")
