# tests/test_dummy.py
import pytest

def add(a, b):
    return a + b


class TestDummy:
    def test_addition(self):
        assert add(2, 3) == 5

    def test_addition_negative(self):
        assert add(-1, -2) == -3

    @pytest.mark.parametrize("a,b,expected", [
        (1, 2, 3),
        (0, 0, 0),
        (-1, 1, 0),
    ])
    def test_parametrized(self, a, b, expected):
        assert add(a, b) == expected


def test_exception():
    with pytest.raises(ZeroDivisionError):
        _ = 1 / 0
