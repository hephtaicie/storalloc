""" Tests for storalloc.resources
"""

import pytest

from storalloc import resources as rs


def test_disk():
    """Test for Disk dataclass"""

    with pytest.raises(TypeError):
        rs.Disk()
