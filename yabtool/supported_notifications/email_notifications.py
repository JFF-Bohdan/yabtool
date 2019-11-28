class EmailNotificationRenderingContext(object):
    def __init__(self):
        self.flow_name = None
        self.host_name = None
        self.str_flow_execution_status = None
        self.time_spent = None
        self.flow_execution_succeeded = None
        self.flow_exception = None
        self.steps_execution_statistics = None
        self.steps_execution_metrics = None


class SendEmailNotification(object):
    def __init__(self, aws_access_key_id, aws_secret_access_key, rendering_context=None):
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key

        self.rendering_context = rendering_context if rendering_context is not None \
            else EmailNotificationRenderingContext()

    def render_plain_text(self):
        pass

    def render_html(self):
        pass
