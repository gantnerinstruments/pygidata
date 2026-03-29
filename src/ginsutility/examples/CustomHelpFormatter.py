################################################
# This file is for removing clutter from the   #
# output of -h/--help.                         #
################################################

import argparse


class CustomHelpFormatter(argparse.HelpFormatter):
    ''' Custom formatter class to remove clutter from the output of -h/--help'''

    def _format_action_invocation(self, action):
        if not action.option_strings:
            default = self._get_default_metavar_for_positional(action)
            return default

        parts = []
        # Check if metavar is provided, otherwise use empty string
        if action.metavar:
            metavar = self._format_args(action, action.metavar)
        else:
            metavar = ''

        for option_string in action.option_strings:
            parts.append(option_string)

        return ', '.join(parts) + ' ' + metavar
