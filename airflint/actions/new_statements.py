# Credits go to GitHub user @isidentical who provided most of the solution.
import ast
from dataclasses import dataclass

from refactor import Action, Context, common
from refactor.ast import split_lines


@dataclass
class NewStatementsAction(Action):
    """Add new statements after the node's line."""

    statements: list[ast.stmt]

    def apply(self, context: Context, source: str) -> str:
        # We need a custom action that defines a custom apply()
        # method since this use case is not natively supported.

        # What apply() does is very basic, it splits the whole
        # source into smaller pieces (lines) and inserts the
        # newly built source code into the appropriate place.
        lines = split_lines(source)

        # First we need to find where the initial anchor starts,
        # and we'll extract the indentation from there. So all
        # the new nodes will align with this indentation level.
        start_line = lines[self.node.lineno - 1]
        indentation, start_prefix = common.find_indent(
            start_line[: self.node.col_offset],
        )

        # Then we'll build the actual source code from scratch.
        new_source = ""
        for statement in self.statements:
            new_source += context.unparse(statement) + "\n"

        # And split it into lines as well.
        replacement = split_lines(new_source)

        # For each line in this code, we'll apply *THE SAME* indentation level
        # as the anchor node.
        replacement.apply_indentation(indentation, start_prefix=start_prefix)

        # And insert these lines just on top of the actual anchor node.
        anchor = self.node.lineno - 1
        for line in reversed(replacement):
            lines.insert(anchor, line)

        # Finally we'll merge everything together!
        return lines.join()
