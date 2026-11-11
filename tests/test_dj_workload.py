"""Per-cell event count behind the DJ workload section."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dashboard import _matrix_cell_events  # noqa: E402


def test_booked_forms():
    assert _matrix_cell_events("BOOKED") == 1
    assert _matrix_cell_events("booked, backup") == 1
    assert _matrix_cell_events("BOOKED x 2") == 2
    assert _matrix_cell_events("WEDFAIRE") == 1


def test_not_events():
    for v in ("", "OUT", "BACKUP", "OK", "MAXED"):
        assert _matrix_cell_events(v) == 0


def test_aag_reservation_counts():
    # RESERVED + AAG couple names = reserved for an AAG wedding (Paul, 2026-11-11)
    assert _matrix_cell_events("RESERVED", "Kelly and Ryan") == 1


def test_plain_reserved_hold_does_not_count():
    assert _matrix_cell_events("RESERVED") == 0
    assert _matrix_cell_events("RESERVED", "   ") == 0
