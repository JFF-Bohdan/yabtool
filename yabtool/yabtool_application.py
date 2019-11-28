import argparse
import datetime
import os
import shutil
import socket
import sys

import boto3
from botocore.exceptions import ClientError
import loguru
from loguru._defaults import LOGURU_FORMAT
from yabtool.version import __version__

from .supported_steps.base import pretty_time_delta, time_interval
from .yabtool_flow_orchestrator import YabtoolFlowOrchestrator


def get_cli_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--version",
        "-v",
        action="version",
        version="%(prog)s {}".format(__version__)
    )

    parser.add_argument(
        "--log-level",
        "-l",
        action="store",
        default="INFO",
        help="Specify log level (DEBUG, INFO, ...)"
    )

    parser.add_argument(
        "--disable-voting",
        "-k",
        action="store_true",
        help="Allow voting for flow execution skipping"
    )

    parser.add_argument(
        "--secrets",
        "-s",
        required=True,
        action="store",
        help="Path to file with secrets"
    )

    parser.add_argument(
        "--config",
        "-c",
        action="store",
        help="Path to main configuration file"
    )

    parser.add_argument(
        "--dry-run",
        "-y",
        action="store_true",
        default=None,
        help="Perform dry run only"
    )

    parser.add_argument(
        "--target",
        "-d",
        action="store",
        help="Specify target in secret file (overrides default in secrets file if specified)"
    )

    parser.add_argument(
        "--temporary-folder",
        "-t",
        action="store",
        help="Path to temporary folder that will be used by yabtool"
    )

    parser.add_argument(
        "--flow",
        "-f",
        action="store",
        help="Required flow name"
    )

    parser.add_argument(
        "--add-main-log",
        "-m",
        action="store_true",
        default=False,
        help="Add main log to logs folder"
    )

    parser.add_argument(
        "--add-session-log",
        "-p",
        action="store_true",
        default=False,
        help="Add session log file"
    )

    return parser.parse_known_args()


class EmailNotifier(object):
    def __init__(self, logger):
        self.logger = logger
        self.flow_orchestrator = None
        self.notification_data = None
        self.succeeded = None
        self.exception = None
        self.seconds_spent = None
        self.only_dry_run = None

    def run(self):
        charset = "utf-8"

        connection = self.notification_data.get("connection")
        assert connection

        region = connection.get("region")
        assert region

        connection_data = self.notification_data.get("connection")
        assert connection_data

        aws_access_key_id = connection_data.get("aws_access_key_id")
        assert aws_access_key_id

        aws_secret_access_key = connection_data.get("aws_secret_access_key")
        assert aws_secret_access_key

        client = boto3.client(
            "ses",
            region_name=region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

        timestamp_end = datetime.datetime.utcnow()
        self.seconds_spent = time_interval(self.flow_orchestrator.backup_start_timestamp, timestamp_end)

        sender = self.notification_data.get("sender")
        assert sender

        to_addresses = self.notification_data.get("to")
        assert to_addresses
        self.logger.info("sending email to: {}".format(to_addresses))

        subject_template = self.notification_data.get("subject")
        assert subject_template

        plain_body_template = self.notification_data.get("body")
        assert plain_body_template

        subject, plain_body = self._render_plain_text_body(plain_body_template, subject_template)
        self.logger.info("plain_body:\n{}".format(plain_body))

        body_html = """<html>
            <head></head>
            <body>
              <pre>{}</pre>
            </body>
            </html>
            """.format(plain_body)

        try:
            self.logger.info("sending email to {}".format(to_addresses))
            response = client.send_email(
                Destination={
                    "ToAddresses": [
                        "; ".join(to_addresses),
                    ],
                },
                Message={
                    "Body": {
                        "Html": {
                            "Charset": charset,
                            "Data": body_html,
                        },
                        "Text": {
                            "Charset": charset,
                            "Data": plain_body,
                        },
                    },
                    "Subject": {
                        "Charset": charset,
                        "Data": subject,
                    },
                },
                Source=sender
                # If you are not using a configuration set, comment or delete the
                # following line
                # ConfigurationSetName=CONFIGURATION_SET,
            )
        # Display an error if something goes wrong.
        except ClientError as e:
            self.logger.error("Error sending email: {}".format(e.response["Error"]["Message"]))
        except Exception as e:
            self.logger.exception("Error sending email: {}".format(e))
        else:
            self.logger.info("Email sent! Message ID: {}".format(response["MessageId"]))

    def _render_plain_text_body(self, plain_body_template, subject_template):
        rendering_context = self.flow_orchestrator.rendering_context

        dry_run_stat = self.flow_orchestrator.produce_exeuction_stat(self.flow_orchestrator.dry_run_statistics)
        active_run_stat = self.flow_orchestrator.produce_exeuction_stat(self.flow_orchestrator.active_run_statistics)

        dry_run_metrics_data_list = self.flow_orchestrator.produce_execution_metrics(
            self.flow_orchestrator.dry_run_statistics
        )
        dry_run_metrics = ""
        for step_name, metrics_data_item in dry_run_metrics_data_list:
            dry_run_metrics += "\nMetrics for '{}':\n{}".format(step_name, metrics_data_item)
        dry_run_metrics = str(dry_run_metrics).strip()

        active_run_metrics_data_list = self.flow_orchestrator.produce_execution_metrics(
            self.flow_orchestrator.active_run_statistics
        )
        active_run_metrics = ""
        for step_name, metrics_data_item in active_run_metrics_data_list:
            active_run_metrics += "\n\nMetrics for '{}':\n{}".format(step_name, metrics_data_item)
        active_run_metrics = str(active_run_metrics).strip()

        context = {
            "backup_start_timestamp": self.flow_orchestrator.backup_start_timestamp,
            "flow_name": rendering_context.flow_name,
            "host_name": socket.gethostname(),
            "flow_execution_succeeded": self.succeeded,
            "str_flow_execution_status": ("succeeded" if self.succeeded else "failed"),
            "time_spent": pretty_time_delta(self.seconds_spent),
            "flow_exception": str(self.exception),

            "dry_run_stat": dry_run_stat,
            "active_run_stat": active_run_stat,
            "only_dry_run": self.only_dry_run,

            "dry_run_metrics": dry_run_metrics,
            "active_run_metrics": active_run_metrics
        }

        rendering_environment = YabtoolFlowOrchestrator.create_rendering_environment()

        body_jinja2_template = rendering_environment.from_string(plain_body_template)
        subject_jinja2_template = rendering_environment.from_string(subject_template)

        plain_body = body_jinja2_template.render(**context)
        plain_subject = subject_jinja2_template.render(**context)

        return plain_subject, plain_body


class YabtoolApplication(object):
    def __init__(self):
        self.logger = None
        self.rendering_context = None

    def run(self):
        args, unknown_args = get_cli_args()
        self._initialize_logger(args)
        self.logger.debug("unknown command line arguments: {}".format(unknown_args))

        flow_orchestrator = YabtoolFlowOrchestrator(self.logger)

        if args.disable_voting:
            flow_orchestrator.skip_voting_enabled = False

        only_dry_run = None
        folder_name = None
        try:
            flow_orchestrator.initialize(args, unknown_args)

            folder_name = flow_orchestrator.rendering_context.temporary_folder
            root_temporary_folder = flow_orchestrator.rendering_context.root_temporary_folder
            flow_name = flow_orchestrator.flow_name

            if args.add_main_log:
                main_logs_folder = os.path.join(root_temporary_folder, "logs", "main")
                self._add_main_log(main_logs_folder, args)

            if args.add_session_log:
                session_logs_folder = os.path.join(root_temporary_folder, "logs", "session")
                self._add_session_log(session_logs_folder, flow_orchestrator.backup_start_timestamp, args)

            flow_orchestrator.dry_run()
            self.logger.info("dry run for flow '{}' performed".format(flow_name))

            only_dry_run = True
            if not args.dry_run:
                self.logger.info("flow '{}' started".format(flow_name))
                flow_orchestrator.run()
                only_dry_run = False

            flow_orchestrator.print_stat()
            self._send_notifications(flow_orchestrator, only_dry_run=only_dry_run)

        except BaseException as e:
            self.logger.exception("Error performing flow: {}".format(e))
            self._send_notifications(flow_orchestrator, succeeded=False, exception=e, only_dry_run=only_dry_run)

        finally:
            if flow_orchestrator.rendering_context.remove_temporary_folder and folder_name:
                if folder_name and os.path.exists(folder_name) and os.path.isdir(folder_name):
                    self.logger.info("going to remove temporary folder: {}".format(folder_name))
                    self._remove_temporary_folder(folder_name)

            else:
                self.logger.info("output folder removal disabled. folder name: '{}'".format(folder_name))

        return True

    def _send_notifications(self, flow_orchestrator, succeeded=True, exception=None, only_dry_run=False):
        enabled_notifications = self._get_enabled_notifications(flow_orchestrator)
        if not enabled_notifications:
            self.logger.info("no enabled notifications")
            return

        self.logger.debug("sending notifications for rules: {}".format(enabled_notifications))
        for notification_type, notification_data in enabled_notifications:
            if notification_type == "email":
                email_notifier = EmailNotifier(self.logger)

                email_notifier.flow_orchestrator = flow_orchestrator
                email_notifier.notification_data = notification_data
                email_notifier.succeeded = succeeded
                email_notifier.exception = exception
                email_notifier.only_dry_run = only_dry_run

                email_notifier.run()
            else:
                self.logger.error("unsupported notification type: '{}'".format(notification_type))

    def _get_enabled_notifications(self, flow_orchestrator):
        res = []

        target_data = flow_orchestrator.secrets_context["targets"][flow_orchestrator.rendering_context.target_name]
        notifications = target_data.get("notifications")
        if not notifications:
            return res

        for notification_type, notification in notifications.items():
            self.logger.debug("validating notification type for '{}'".format(notification_type))
            if not self._is_known_notification_type(notification_type):
                self.logger.warning("unknown notification type for '{}'".format(notification_type))
                continue

            if notification.get("enabled", False):
                notification_item = (notification_type, notification)
                res.append(notification_item)

        return res

    def _is_known_notification_type(self, notification_type):
        return notification_type in ["email"]

    def _initialize_logger(self, args):
        self.logger = loguru.logger
        self.logger.remove()
        self.logger.add(sys.stdout, format=LOGURU_FORMAT, level=args.log_level)

    def _add_main_log(self, logs_folder, args):
        if not os.path.exists(logs_folder):
            os.makedirs(logs_folder)

        path = os.path.join(logs_folder, "main_log.log")
        self.logger.add(path, rotation="10 Mb", retention="60 days", compression="zip", level=args.log_level)

    def _add_session_log(self, logs_folder, timestamp_begin, args):
        if not os.path.exists(logs_folder):
            os.makedirs(logs_folder)

        session_log_suffix = timestamp_begin.strftime("%Y-%m-%dT%H%M%S")
        path = os.path.join(logs_folder, "session_{}.log".format(session_log_suffix))
        self.logger.add(path, level=args.log_level)

    def _remove_temporary_folder(self, folder_name):
        try:
            shutil.rmtree(folder_name)
        except Exception as e:
            self.logger.error("error removing folder '{}': {}".format(folder_name, e))
