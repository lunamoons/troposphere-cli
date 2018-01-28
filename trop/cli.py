import os
import sys
import click
import boto3
import time

from datetime import (
    datetime,
    timedelta,
)
from pytz import utc
from botocore.exceptions import ClientError

from functools import update_wrapper
from troposphere import Template


@click.group()
@click.option(
    '--region',
    '-r',
    envvar='AWS_REGION',
    required=True,
    help="The `AWS` region.",
)
@click.pass_context
def cli(context, region):
    """ Troposphere CLI.
    """
    context.obj = {
        'cloudformation': boto3.client('cloudformation', region_name=region),
        's3': boto3.client('s3', region_name=region),
    }


STATUSES = [
    'CREATE_IN_PROGRESS',
    'CREATE_FAILED',
    'CREATE_COMPLETE',
    'ROLLBACK_IN_PROGRESS',
    'ROLLBACK_FAILED',
    'ROLLBACK_COMPLETE',
    'DELETE_IN_PROGRESS',
    'DELETE_FAILED',
    'DELETE_COMPLETE',
    'UPDATE_IN_PROGRESS',
    'UPDATE_COMPLETE_CLEANUP_IN_PROGRESS',
    'UPDATE_COMPLETE',
    'UPDATE_ROLLBACK_IN_PROGRESS',
    'UPDATE_ROLLBACK_FAILED',
    'UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS',
    'UPDATE_ROLLBACK_COMPLETE',
]


@cli.command('list')
@click.option('--all', is_flag=True)
@click.pass_obj
def list_stacks(clients, show_all):
    """ List stacks.
    """
    statuses = STATUSES.copy()
    if not show_all:
        statuses.remove('DELETE_COMPLETE')

    stacks = clients['cloudformation'].list_stacks(
        StackStatusFilter=statuses,
    )
    for stack in stacks.get('StackSummaries'):
        click.echo(
            "%(StackName)-15s %(StackStatus)s" % stack
        )


def _events(clients, name):
    now = utc.localize(datetime.utcnow())
    seen = set()

    while True:
        status = (
            clients['cloudformation'].describe_stacks(StackName=name)
            .get('Stacks')[0]
            .get('StackStatus')
        )

        events = clients['cloudformation'].describe_stack_events(StackName=name)

        for event in reversed(events.get('StackEvents')):
            event_id = event.get('EventId')
            timestamp = event.get('Timestamp')

            if timestamp < now - timedelta(seconds=2):
                continue

            if event_id not in seen:
                message = (
                    "%(Timestamp)s [ %(LogicalResourceId)-20s ] "
                    % event +
                    "%(ResourceStatus)s"
                    % event
                )
                reason = event.get('ResourceStatusReason')

                if reason is not None:
                    message += " - %s" % reason

                click.echo(message)

            seen.add(event_id)

        if status.endswith('COMPLETE'):
            break

        time.sleep(1)


@cli.command('events')
@click.argument('name')
@click.pass_obj
def stack_events(*args, **kwargs):
    """ Display stack events.
    """
    return _events(*args, **kwargs)


@cli.command()
@click.argument('name')
@click.option(
    '--key',
    '-k',
    help="Output the value for given key.",
)
@click.pass_obj
def outputs(clients, name, key=None):
    """ Show stack output values.
    """
    for output in (
        clients['cloudformation'].describe_stacks(StackName=name)
        .get('Stacks')[0]
        .get('Outputs')
    ):
        if key is not None:
            if output.get('OutputKey') == key:
                click.echo(output.get('OutputValue'))
                break

            continue

        click.echo(
            "%(OutputKey)-35s: %(OutputValue)-30s" % output
        )


def _parameters(clients, name):
    stack = clients['cloudformation'].describe_stacks(StackName=name).get('Stacks')[0]
    assert stack.get('StackStatus') not in (
        'DELETE_COMPLETE',
    )

    return {
        parameter['ParameterKey']: parameter['ParameterValue']
        for parameter in stack.get('Parameters')
    }


@cli.command()
@click.argument('name')
@click.pass_obj
def parameters(clients, name):
    """ Show templates parameter values.
    """
    try:
        for key, value in _parameters(clients, name).items():
            click.echo(
                "%(key)-35s: %(value)-30s" % dict(key=key, value=value)
            )

    except (ClientError, AssertionError):
        click.echo(
            "Stack `%s` does not exist." % name
        )


def _template(path):
    if not isinstance(path, Template):
        sys.path.append(os.getcwd())
        paths = path.split('.')

        path = __import__('.'.join(paths[:-1]))

        for attr in paths[1:]:
            path = getattr(path, attr)

    return path


@cli.command('template')
@click.argument('template', envvar='STACK_TEMPLATE')
def template_to_json(template):
    """ Show template as json.
    """
    template = _template(template)
    click.echo(template.to_json())


def manage(command):
    @click.option(
        '--template',
        '-t',
        envvar='STACK_TEMPLATE',
        required=True,
        help="Path to stack template.",
    )
    @click.option(
        '--parameter',
        '-p',
        type=(str, str),
        multiple=True,
        help="Stack parameter as `<key> <value>`.",
    )
    @click.option(
        '--iam',
        'capability',
        flag_value='CAPABILITY_IAM',
        help="Enable `CAPABILITY_IAM`.",
    )
    @click.option(
        '--named-iam',
        'capability',
        flag_value='CAPABILITY_NAMED_IAM',
        help="Enable `CAPABILITY_NAMED_IAM`.",
    )
    @click.option(
        '--tail',
        is_flag=True,
        help="Show stack events.",
    )
    @click.option(
        '--bucket',
        '-b',
        envvar='AWS_TEMPLATE_BUCKET',
        required=True,
        help="Name of the template bucket.",
    )
    @click.argument('name')
    @click.pass_obj
    def f(*args, **kwargs):
        tail = kwargs.pop('tail')
        command(*args, **kwargs)

        if tail:
            _events(args[0], kwargs['name'])

    return update_wrapper(f, command)


def update_params(template, params, previous):
    parameters_dict = {
        key: None
        for key in template.parameters
    }
    parameters_dict.update(
        {key: value for key, value in params},
    )

    params = []

    for key, value in parameters_dict.items():
        if value is None and key not in previous:
            continue

        param = dict(
            ParameterKey=key,
            UsePreviousValue=(
                value is None and
                key in previous
            ),
        )

        if value is not None:
            param.update(dict(ParameterValue=value))

        params.append(param)

    return params


def upload_template_to_s3(s3_client, stack_name, template, template_bucket):
    template_key = f'{stack_name}.template'
    template = template.encode('utf-8')
    s3_client.put_object(Body=template, Bucket=template_bucket, Key=template_key)

    bucket_location = s3_client.get_bucket_location(Bucket=template_bucket)
    bucket_location = bucket_location['LocationConstraint']

    return f'https://s3-{bucket_location}.amazonaws.com/{template_bucket}/{template_key}'


def get_stack_definition(clients, template, name, parameter, capability, bucket):
    template = _template(template)
    template_url = upload_template_to_s3(
        clients['s3'], name, template.to_json(indent=None), bucket,
    )

    try:
        previous_params = _parameters(clients, name)

    except (ClientError, AssertionError):
        previous_params = dict()

    definition = dict(
        StackName=name,
        TemplateURL=template_url,
        Parameters=update_params(template, parameter, previous_params),
    )

    if capability is not None:
        definition.update(dict(Capabilities=[capability]))

    return definition


@cli.command()
@manage
def create(clients, *args, **kwargs):
    """ Create a new stack.
    """
    clients['cloudformation'].create_stack(**get_stack_definition(clients, *args, **kwargs))


@cli.command()
@manage
def update(clients, *args, **kwargs):
    """ Update an existing stack.
    """
    stack_definition = get_stack_definition(clients, *args, **kwargs)
    clients['cloudformation'].update_stack(**stack_definition)


if __name__ == '__main__':
    cli.main()
