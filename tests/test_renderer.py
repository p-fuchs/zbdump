from __future__ import annotations

from io import StringIO

from zbdump import DumpFileRenderer, TableColumnInfo, TableIndexInfo, TableInfo


def _build_table_info() -> TableInfo:
    return TableInfo(
        table_name="dump_fixture",
        columns=[
            TableColumnInfo(
                ordinal_position=1,
                column_name="id",
                rendered_data_type="integer",
                is_nullable=False,
                column_default=None,
                is_identity=False,
                identity_generation=None,
            )
        ],
        indices=[],
        primary_key_columns=["id"],
    )


def test_render_table_indexes_skips_output_when_only_primary_key_exists() -> None:
    buffer = StringIO()
    renderer = DumpFileRenderer(buffer)

    renderer.render_table_indexes(_build_table_info())

    assert buffer.getvalue() == ""


def test_render_table_indexes_skips_primary_key_backing_index_with_custom_name() -> (
    None
):
    buffer = StringIO()
    renderer = DumpFileRenderer(buffer)
    table_info = TableInfo(
        table_name="dump_fixture",
        columns=_build_table_info().columns,
        indices=[
            TableIndexInfo(
                index_name="custom_named_pk",
                index_def="CREATE UNIQUE INDEX custom_named_pk ON public.dump_fixture USING btree (id)",
                is_primary_key_backing=True,
            )
        ],
        primary_key_columns=["id"],
    )

    renderer.render_table_indexes(table_info)

    assert buffer.getvalue() == ""


def test_render_table_data_skips_output_for_empty_row_iterator() -> None:
    buffer = StringIO()
    renderer = DumpFileRenderer(buffer)

    renderer.render_table_data(
        _build_table_info(),
        iter(()),
        include_column_names=True,
    )

    assert buffer.getvalue() == ""
