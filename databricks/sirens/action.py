"""Implements the API for the actions framework. Specifically executes actions a
multiprocessor pool with a user defined time out.

Returns status as bool, response/errormessage as str

Authors:
    Derek King 11/22/22

Classes:
    Handler

Functions:
    execute(func, params, body) -> Tuple[bool, str]
    df_to_dict(spark, df) -> dict
"""
from multiprocessing.context import TimeoutError
from multiprocessing.pool import Pool
from typing import List, Optional, Dict, Tuple

from pyspark.sql import DataFrame

from databricks.sirens.actions import Action
from databricks.sirens.config_reader import ActionReader
from databricks.sirens.exceptions import SirensActionException
from databricks.sirens.internal.baseaction import ActionResult
from databricks.sirens.logging import get_logger

logger = get_logger(__name__)


class Handler:
    """Action handler class - implements API between main notebooks and action plugins.
    """

    def __init__(self, module: str, timeout: Optional[int] = 60, debug: Optional[bool] = False):
        self.send_to = module
        self.timeout = timeout
        self.debug = debug
        self.timed_out = False
        if not isinstance(self.timeout, int):
            raise SirensActionException('specify timeout as type int')
        # read the action.yaml config for third party integration
        self.actionConfigObj = ActionReader().read(module)
        self.body = None
        logger.debug(f'read action config as: {self.actionConfigObj}')

    @staticmethod
    def _result_callback(result):
        logger.debug(f"Action Callback Set: {result}")
        pass

    # task executed in a worker process
    def _task(self, plugin: object, params: Optional[Dict],
              body: Optional[List[Dict]], requested_action: str) -> ActionResult:
        """Executes the given action in the defined action module

        :param plugin: action plugin to use
        :type plugin: object
        :param params: any parameters required by the action module
        :type params: Optional[Dict]
        :param body: data to send
        :type body: Optional[List[Dict]]
        :param requested_action: function to call in the action module
        :type requested_action: str
        :return: Status as bool, API return string or error message
        :rtype: Tuple[bool, str]
        """
        logger.debug(f'instantiating plugin {plugin}')
        action_obj = Action(plugin=plugin, actionConfigObj=self.actionConfigObj)
        logger.debug(f"executing action: {requested_action}'")
        result = action_obj.do_action(requested_action=requested_action, params=params, body=body)
        logger.debug(f"finished executing. status: {result.status}, result: {result.data}")

        # verify third party integration returns the coorect format
        if not isinstance(result, ActionResult):
            raise SirensActionException("Invalid response. Please return ActionResult")

        logger.debug(f'Task {plugin} done')

        return ActionResult(result.status, result.data)

    @staticmethod
    def df_to_dict(df: DataFrame):
        """Convert a dataframe to dictionary to pass to action

        :param df: dataframe to pass to action handler
        :type df: DataFrame
        """
        return [row.asDict() for row in df.collect()]

    def execute(self, action_request: str, params: Optional[Dict] = None,
                body: Optional[List[Dict]] = None) -> ActionResult:
        """Execute action

        :param action_request: action to execute on the handler
        :type action_request: str
        :param params: parameter information needed for the action, defaults to None
        :type header: Optional[[Dict], optional
        :param body: data to be sent to the action handler, defaults to None
        :type body: Optional[List[Dict]], optional
        :return: Status as bool, API return string or error message
        :rtype: ActionResult
        """
        self.action_request = action_request
        self.body = body
        self.params = params

        # check list or dict passed - convert if not
        if self.body:
            if not isinstance(self.body, list):
                self.body = [self.body]

        if self.params:
            if not isinstance(self.params, dict):
                return ActionResult(False, "invalid param type - please use dict type")

        logger.debug("starting processor pool with pool of 1.")
        # create daemon process pool of 1.
        with Pool(1) as pool:

            # issue tasks to the process pool
            logger.debug(f"starting tasK {self._task}, with data: {self.body}")
            result = pool.apply_async(self._task, args=(self.send_to, self.params, self.body, action_request),
                                      callback=self._result_callback)
            try:
                status, task_result = result.get(timeout=self.timeout)
                logger.debug(f"getting result entries: status: {status}, task_result: {task_result}")
            except ValueError as exc:
                raise SirensActionException(f"Invalid return from {self.send_to} API. Please return 'status' "
                                            f"'response/error' as Tuple(bool, str)")
            except TimeoutError as exc:
                pool.terminate()
                self.timed_out = True
                status = False
                task_result = f"Action timed out before hearing back from {self.send_to}"

            # close the process pool
            pool.terminate()
            # wait for the child processes to close
            pool.join()

        return ActionResult(status, task_result)
