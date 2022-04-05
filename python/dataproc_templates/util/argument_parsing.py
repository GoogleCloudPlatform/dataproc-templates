from typing import TypedDict, Dict
import argparse

from .default_properties import DEFAULT_PROPERTIES


class DataprocTemplateArguments(TypedDict):
    template_name: str
    properties: Dict[str, str]


class _ParseKeyValue(argparse.Action):
    """
    Argparse action for storing key-value pairs in a dictionary.
    
    Each key-value pair should use the format "key=value".
    """

    def __call__(self, parser, namespace, values, option_string=None):

        parsed_elements: Dict[str, str] = getattr(namespace, self.dest, dict())

        for value in values:
            key, value = value.split('=', 1)
            parsed_elements[key] = value
        
        setattr(namespace, self.dest, parsed_elements)


def parse_args() -> DataprocTemplateArguments:
    """
    Parses command line options for Dataproc Templates.

    Returns:
        DataprocTemplateArguments: Typed dictionary containing
        the template name and the template properties.
    """

    parser: argparse.ArgumentParser = argparse.ArgumentParser()

    parser.add_argument(
        '--template',
        dest='template_name',
        type=str,
        help='The name of the template to run',
        required=True
    )
    parser.add_argument(
        '--templateProperty',
        '--template_property',
        dest='template_properties',
        nargs='*',
        action=_ParseKeyValue,
        help='Key and value for a template property',
        default={}
    )

    args: argparse.Namespace = parser.parse_args()

    return {
        'template_name': args.template_name,
        'properties': {
            **DEFAULT_PROPERTIES,
            **args.template_properties
        }
    }
