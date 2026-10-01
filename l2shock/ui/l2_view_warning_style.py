# l2shock/ui/l2_view_warning_style.py
"""Presentation style for detector-free Analysis outage regions.

This module contains no outage detection, price loading, database access,
or candidate annotation logic.
"""

from typing import Final

WARNING_REGION_COLOR: Final[str] = "rgba(220, 38, 38, 0.14)"


__all__ = ["WARNING_REGION_COLOR"]
