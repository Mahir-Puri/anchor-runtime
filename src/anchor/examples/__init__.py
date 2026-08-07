"""Example workflows.

Importing this package registers them, which is why the worker and the API both
import it at startup. In a real deployment you would point ANCHOR_WORKFLOWS at
your own module instead.
"""

from anchor.examples import refund_agent  # noqa: F401  (import registers)

__all__ = ["refund_agent"]
