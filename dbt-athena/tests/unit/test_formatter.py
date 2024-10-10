import textwrap
from datetime import date, datetime
from decimal import Decimal

import agate
import pytest
from pyathena.error import ProgrammingError

from dbt.adapters.athena.connections import AthenaParameterFormatter


class TestAthenaParameterFormatter:
    formatter = AthenaParameterFormatter()

    @pytest.mark.parametrize(
        "sql",
        [
            "",
            """

    """,
        ],
    )
    def test_query_none_or_empty(self, sql):
        with pytest.raises(ProgrammingError) as exc:
            self.formatter.format(sql)
        assert exc.value.__str__() == "Query is none or empty."

    def test_query_none_parameters(self):
        sql = self.formatter.format(
            """

            ALTER TABLE table ADD partition (dt = '2022-01-01')
        """
        )
        assert sql == "ALTER TABLE table ADD partition (dt = '2022-01-01')"

    def test_query_parameters_not_list(self):
        with pytest.raises(ProgrammingError) as exc:
            self.formatter.format(
                """
                SELECT *
                  FROM table
                 WHERE country = %(country)s
            """,
                {"country": "FR"},
            )
        assert exc.value.__str__() == "Unsupported parameter (Support for list only): {'country': 'FR'}"

    def test_query_parameters_unknown_formatter(self):
        with pytest.raises(TypeError) as exc:
            self.formatter.format(
                """
                SELECT *
                  FROM table
                 WHERE country = %s
            """,
                [agate.Table(rows=[("a", 1), ("b", 2)], column_names=["str", "int"])],
            )
        assert exc.value.__str__() == "<class 'agate.table.Table'> is not defined formatter."

    def test_query_parameters_list(self):
        res = self.formatter.format(
            textwrap.dedent(
                """
                        SELECT *
                          FROM table
                         WHERE nullable_field = %s
                           AND dt = %s
                           AND dti = %s
                           AND int_field > %s
                           AND float_field > %.2f
                           AND fake_decimal_field < %s
                           AND str_field = %s
                           AND str_list_field IN %s
                           AND int_list_field IN %s
                           AND float_list_field IN %s
                           AND dt_list_field IN %s
                           AND dti_list_field IN %s
                           AND bool_field = %s
                    """
            ),
            [
                None,
                date(2022, 1, 1),
                datetime(2022, 1, 1, 0, 0, 0),
                1,
                1.23,
                Decimal(2),
                "test",
                ["a", "b"],
                [1, 2, 3, 4],
                (1.1, 1.2, 1.3),
                (date(2022, 2, 1), date(2022, 3, 1)),
                (datetime(2022, 2, 1, 1, 2, 3), datetime(2022, 3, 1, 4, 5, 6)),
                True,
            ],
        )
        expected = textwrap.dedent(
            """
            SELECT *
              FROM table
             WHERE nullable_field = null
               AND dt = DATE '2022-01-01'
               AND dti = TIMESTAMP '2022-01-01 00:00:00.000'
               AND int_field > 1
               AND float_field > 1.23
               AND fake_decimal_field < 2
               AND str_field = 'test'
               AND str_list_field IN ('a', 'b')
               AND int_list_field IN (1, 2, 3, 4)
               AND float_list_field IN (1.100000, 1.200000, 1.300000)
               AND dt_list_field IN (DATE '2022-02-01', DATE '2022-03-01')
               AND dti_list_field IN (TIMESTAMP '2022-02-01 01:02:03.000', TIMESTAMP '2022-03-01 04:05:06.000')
               AND bool_field = True
            """
        ).strip()
        assert res == expected
