class BaseFlowStep(object):
    def __init__(
        self,
        logger,
        rendering_context,
        step_context,
        secret_context,
        rendering_environment,
    ):
        self.logger = logger
        self.rendering_context = rendering_context
        self.step_context = step_context
        self.rendering_environment = rendering_environment
        self.secret_context = secret_context

    @classmethod
    def step_name(cls):
        pass

    def run(self, dry_run=False):
        return dict()

    def _render_parameter(self, parameter_name):
        template = self.step_context[parameter_name]
        self.logger.debug("'{}'@template: '{}'".format(parameter_name, template))

        res = self._render_result(template)
        self.logger.debug("'{}'@value: '{}'".format(parameter_name, res))

        return res

    def _render_result(self, template, additional_context=None):
        mixed_context = self.rendering_context.to_context()
        mixed_context = {**mixed_context, **self._get_step_context()}

        if additional_context:
            mixed_context = {**mixed_context, **additional_context}

        jinja2_template = self.rendering_environment.from_string(template)
        return jinja2_template.render(**mixed_context)

    def _get_step_context(self):
        return {**self.step_context, **self.secret_context}


class StepContextData(object):
    def __init__(self):
        self.name = None
        self.description = None
