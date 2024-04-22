from airflow.models import BaseOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.utils.decorators import apply_defaults

class CustomSlackOperator(BaseOperator):
    """
    Custom Slack Operator to send a message with a hidden URL.
    """

    template_fields = ('text',)

    @apply_defaults
    def __init__(
            self,
            text,
            channel='#dv-team-familie-varslinger',
            username='Airflow',
            icon_emoji=':ghost:',
            webhook_token=None,
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.text = text
        self.channel = channel
        self.username = username
        self.icon_emoji = icon_emoji
        self.webhook_token = webhook_token

    def execute(self, context):
        slack_hook = SlackWebhookHook(
            http_conn_id=self.webhook_token,
            message=self.text,
            channel=self.channel,
            username=self.username,
            icon_emoji=self.icon_emoji
        )
        slack_hook.execute()
