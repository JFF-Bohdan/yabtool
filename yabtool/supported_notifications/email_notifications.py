import datetime
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import mimetypes
import os
import socket

import boto3
from botocore.exceptions import ClientError
from yabtool.shared.base import AttrsToStringMixin
from yabtool.shared.jinja2_helpers import create_rendering_environment
from yabtool.supported_steps.base import pretty_time_delta, time_interval
from yabtool.version import __version__


class DataForEmailSending(AttrsToStringMixin):
    def __init__(self):
        self.subject = None
        self.sender = None
        self.to_recipients = None
        self.cc_recipients = None
        self.bcc_recipients = None
        self.plain_text_body = None
        self.html_body = None
        self.charset = None
        self.attachments = []


class EmailSender(object):

    def __init__(self, logger, notification_data):
        self.logger = logger
        self.notification_data = notification_data

    def send(self, data_for_sending: DataForEmailSending):
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

        msg = MIMEMultipart()
        msg["Subject"] = data_for_sending.subject
        msg["From"] = data_for_sending.sender
        msg["To"] = "; ".join(data_for_sending.to_recipients)

        for attachment_file_path in data_for_sending.attachments:
            part = MIMEApplication(self._get_file_content(attachment_file_path))
            part.add_header("Content-Disposition", "attachment", filename=os.path.basename(attachment_file_path))
            mimetype = mimetypes.guess_type(attachment_file_path)
            part.add_header("Content-Type", "{}; charset=UTF-8".format(mimetype))

            msg.attach(part)

        if data_for_sending.html_body:
            part_html = MIMEText(data_for_sending.html_body, "html")
            msg.attach(part_html)
        else:
            part_plain = MIMEText(data_for_sending.plain_text_body, "plain")
            msg.attach(part_plain)

        try:
            self.logger.info("sending email to {}".format(data_for_sending.to_recipients))

            response = client.send_raw_email(
                RawMessage={"Data": msg.as_string()},
                Source=data_for_sending.sender,
                Destinations=data_for_sending.to_recipients
            )

        # Display an error if something goes wrong.
        except ClientError as e:
            self.logger.error("Error sending email: {}".format(e.response["Error"]["Message"]))
        except Exception as e:
            self.logger.exception("Error sending email: {}".format(e))
        else:
            self.logger.info("Email sent! Message ID: {}".format(response["MessageId"]))

    def _get_file_content(self, file_name):
        with open(file_name, "rb") as input_file:
            return input_file.read()


class EmailRenderer(object):
    DEFAULT_CHARSET = "utf-8"

    def __init__(self, logger):
        self.logger = logger
        self.flow_orchestrator = None
        self.notification_data = None
        self.succeeded = None
        self.exception = None
        # self.seconds_spent = None
        self.only_dry_run = None

        self.additional_variables = {}

        self.charset = EmailRenderer.DEFAULT_CHARSET

    def render(self):
        res = DataForEmailSending()
        res.charset = self.charset

        timestamp_end = datetime.datetime.utcnow()
        seconds_spent = time_interval(self.flow_orchestrator.backup_start_timestamp, timestamp_end)

        flow_context = self.flow_orchestrator.rendering_context

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

        rendering_context = flow_context.to_context()

        new_values = {
            "backup_start_timestamp": self.flow_orchestrator.backup_start_timestamp,
            "flow_name": flow_context.flow_name,
            "host_name": socket.gethostname(),
            "flow_execution_succeeded": self.succeeded,
            "str_flow_execution_status": ("succeeded" if self.succeeded else "failed"),
            "time_spent": pretty_time_delta(seconds_spent),
            "flow_exception": str(self.exception),

            "dry_run_stat": dry_run_stat,
            "active_run_stat": active_run_stat,
            "only_dry_run": self.only_dry_run,

            "dry_run_metrics": dry_run_metrics,
            "active_run_metrics": active_run_metrics,
            "yabtool_version": __version__
        }

        rendering_context = {**rendering_context, **new_values}
        rendering_context = {**rendering_context, **self.additional_variables}

        res.sender = self.notification_data.get("sender")
        assert res.sender

        res.to_recipients = self.notification_data.get("to")
        assert res.to_recipients

        subject_template = self.notification_data.get("subject")
        assert subject_template

        plain_body_template = self.notification_data.get("body")
        assert plain_body_template

        res.subject, res.plain_text_body = self._render_plain_text_body(
            rendering_context,
            plain_body_template,
            subject_template
        )
        self.logger.info("plain_body:\n{}".format(res.plain_text_body))

        res.html_body = self._render_html_body(res.plain_text_body)

        return res

    def _render_plain_text_body(self, context, plain_body_template, subject_template):
        rendering_environment = create_rendering_environment()

        body_jinja2_template = rendering_environment.from_string(plain_body_template)
        subject_jinja2_template = rendering_environment.from_string(subject_template)

        plain_body = body_jinja2_template.render(**context)
        plain_subject = subject_jinja2_template.render(**context)

        return plain_subject, plain_body

    def _render_html_body(self, plain_text):
        return """<html>
            <head></head>
            <body>
              <pre>{}</pre>
            </body>
            </html>
        """.format(plain_text)
