"""Slack API Module.

do_action() calls the functions available here.

Author:
    Derek King. 22-Nov-22

Version: 1.0

Classes:
    Action

Functions:
    do_action(action, actionConfig, params, body)

Actions:
    post_message()

Params:
    {"template":"<name of template in action conf dir>, "channel":<channel to send to>}
Body:
    {"Data to send - which must match the template file you use"}
"""

import json
from typing import List, Optional, Dict, Tuple, Union

from pyspark.sql import DataFrame


from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from jinja2 import Template, Environment, FileSystemLoader, TemplateNotFound

from . import plugins
from databricks.sirens.logging import get_logger
from databricks.sirens.internal.baseaction import BaseAction, ActionResult
from ..global_config import GlobalConfig

logger = get_logger(__name__)


@plugins.register
class Action(BaseAction):
    """Slack Actions Handler
    """

    def __init__(self, actionConfigObj: dict) -> None:
        super().__init__(actionConfigObj)

    @plugins.register
    def do_action(self, requested_action: str, params: Optional[Dict],
                  body: Optional[Union[List[Dict], DataFrame]]) -> ActionResult:
        """Entry point for supported actions

        :param requested_action: name of a function that implements the action (ex. post_message)
        :type requested_action: str
        :param params: any params required to setup the action
        :type params: Optional[Dict]
        :param body: data to send using the action
        :type body: Optional[List[Dict]]
        :raises SirensActionException: _description_
        :return: ActionResult
        :rtype: Object
        """

        # if post message called.
        if 'post_message' in requested_action:
            status, response = self._post_message(params, body)
            if not status:
                return ActionResult(False, str(response))
            else:
                return ActionResult(True, str(response))
        else:
            return ActionResult(False, f"No action {requested_action} available")

    def _format_message(self, template: str, body: Dict) -> object:
        try:
            file_loader = FileSystemLoader([GlobalConfig.get_top_level_directory() + "/conf/actions/slack/"])
            env = Environment(loader=file_loader)
            template = env.get_template(template + '.template.jinja')
            context_text = json.dumps(json.dumps(body.get('context'), sort_keys=True, indent=4))
            output = template.render(body=body, context=context_text)
        except Exception as exc:
            return ActionResult(False, str({exc}))

        return ActionResult(True, output)

    def _post_message(self, params: Optional[Dict], body: Optional[List[Dict]]) -> ActionResult:
        """Post a message to a Slack channel

        :param params: parameter information required to make the call
        :type header: str
        :param body: data to send
        :type body: dict
        :return: response string
        :rtype: list
        """

        CHANNEL = params.get("channel")
        TEMPLATE = params.get("template")

        if not CHANNEL:
            return ActionResult(False, "No slack channel passed")
        if not TEMPLATE:
            return ActionResult(False, "No template file passed")

        try:
            # create client object
            client = WebClient(token=self.auth_token)
        except Exception as exc:
            return ActionResult(False, str(exc))

        # apply jinja template to data passed in
        status, response = self._format_message(TEMPLATE, body[0])
        if not status:
            return ActionResult(False, str(response))
        else:
            formatted_message = response

        try:
            # send to slack
            response = client.chat_postMessage(channel="#" + CHANNEL, text=formatted_message, blocks=formatted_message)
            return ActionResult(True, str(response))
        except SlackApiError as exc:
            return ActionResult(False, str(exc))
