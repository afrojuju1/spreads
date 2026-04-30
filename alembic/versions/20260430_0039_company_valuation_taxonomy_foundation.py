"""company valuation taxonomy foundation

Revision ID: 20260430_0039
Revises: 20260429_0038
Create Date: 2026-04-30 12:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260430_0039"
down_revision = "20260429_0038"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "taxonomy_nodes",
        sa.Column("taxonomy_node_id", sa.Text(), nullable=False),
        sa.Column("taxonomy_version", sa.Text(), nullable=False),
        sa.Column("taxonomy_level", sa.Text(), nullable=False),
        sa.Column("taxonomy_code", sa.Text(), nullable=False),
        sa.Column("taxonomy_name", sa.Text(), nullable=False),
        sa.Column("parent_taxonomy_node_id", sa.Text(), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.ForeignKeyConstraint(
            ["parent_taxonomy_node_id"],
            ["taxonomy_nodes.taxonomy_node_id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("taxonomy_node_id"),
        sa.UniqueConstraint(
            "taxonomy_version",
            "taxonomy_level",
            "taxonomy_code",
            name="ux_taxonomy_nodes_version_level_code",
        ),
    )
    op.create_index(
        "idx_taxonomy_nodes_level",
        "taxonomy_nodes",
        ["taxonomy_level"],
        unique=False,
    )
    op.create_index(
        "idx_taxonomy_nodes_parent",
        "taxonomy_nodes",
        ["parent_taxonomy_node_id"],
        unique=False,
    )

    op.create_table(
        "taxonomy_mappings",
        sa.Column("mapping_id", sa.Text(), nullable=False),
        sa.Column("mapping_version", sa.Text(), nullable=False),
        sa.Column("source_standard", sa.Text(), nullable=False),
        sa.Column("source_code", sa.Text(), nullable=False),
        sa.Column("source_title", sa.Text(), nullable=True),
        sa.Column("match_mode", sa.Text(), nullable=False),
        sa.Column("canonical_sector_id", sa.Text(), nullable=True),
        sa.Column("canonical_industry_group_id", sa.Text(), nullable=True),
        sa.Column("canonical_industry_id", sa.Text(), nullable=True),
        sa.Column("canonical_subindustry_id", sa.Text(), nullable=True),
        sa.Column("priority", sa.Integer(), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["canonical_sector_id"],
            ["taxonomy_nodes.taxonomy_node_id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["canonical_industry_group_id"],
            ["taxonomy_nodes.taxonomy_node_id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["canonical_industry_id"],
            ["taxonomy_nodes.taxonomy_node_id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["canonical_subindustry_id"],
            ["taxonomy_nodes.taxonomy_node_id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("mapping_id"),
    )
    op.create_index(
        "idx_taxonomy_mappings_standard_code",
        "taxonomy_mappings",
        ["source_standard", "source_code"],
        unique=False,
    )
    op.create_index(
        "idx_taxonomy_mappings_subindustry",
        "taxonomy_mappings",
        ["canonical_subindustry_id"],
        unique=False,
    )

    op.create_table(
        "valuation_template_mappings",
        sa.Column("mapping_id", sa.Text(), nullable=False),
        sa.Column("mapping_version", sa.Text(), nullable=False),
        sa.Column("taxonomy_node_id", sa.Text(), nullable=False),
        sa.Column("taxonomy_level", sa.Text(), nullable=False),
        sa.Column("template_id", sa.Text(), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["taxonomy_node_id"],
            ["taxonomy_nodes.taxonomy_node_id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("mapping_id"),
        sa.UniqueConstraint(
            "mapping_version",
            "taxonomy_node_id",
            name="ux_valuation_template_mappings_version_node",
        ),
    )
    op.create_index(
        "idx_valuation_template_mappings_template",
        "valuation_template_mappings",
        ["template_id"],
        unique=False,
    )

    op.create_table(
        "issuer_classifications",
        sa.Column("issuer_id", sa.Text(), nullable=False),
        sa.Column("taxonomy_version", sa.Text(), nullable=False),
        sa.Column("canonical_sector_id", sa.Text(), nullable=True),
        sa.Column("canonical_industry_group_id", sa.Text(), nullable=True),
        sa.Column("canonical_industry_id", sa.Text(), nullable=True),
        sa.Column("canonical_subindustry_id", sa.Text(), nullable=True),
        sa.Column("classification_source", sa.Text(), nullable=False),
        sa.Column("classification_confidence", sa.Float(), nullable=False),
        sa.Column("taxonomy_mapping_id", sa.Text(), nullable=True),
        sa.Column("valuation_template_mapping_id", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["issuer_id"], ["issuers.issuer_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["canonical_sector_id"],
            ["taxonomy_nodes.taxonomy_node_id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["canonical_industry_group_id"],
            ["taxonomy_nodes.taxonomy_node_id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["canonical_industry_id"],
            ["taxonomy_nodes.taxonomy_node_id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["canonical_subindustry_id"],
            ["taxonomy_nodes.taxonomy_node_id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["taxonomy_mapping_id"],
            ["taxonomy_mappings.mapping_id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["valuation_template_mapping_id"],
            ["valuation_template_mappings.mapping_id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("issuer_id"),
    )
    op.create_index(
        "idx_issuer_classifications_source",
        "issuer_classifications",
        ["classification_source"],
        unique=False,
    )
    op.create_index(
        "idx_issuer_classifications_subindustry",
        "issuer_classifications",
        ["canonical_subindustry_id"],
        unique=False,
    )

    op.create_table(
        "issuer_overlay_flags",
        sa.Column("issuer_overlay_flag_id", sa.Text(), nullable=False),
        sa.Column("issuer_id", sa.Text(), nullable=False),
        sa.Column("flag_key", sa.Text(), nullable=False),
        sa.Column("flag_value", sa.Boolean(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["issuer_id"], ["issuers.issuer_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("issuer_overlay_flag_id"),
        sa.UniqueConstraint(
            "issuer_id",
            "flag_key",
            name="ux_issuer_overlay_flags_issuer_flag",
        ),
    )
    op.create_index(
        "idx_issuer_overlay_flags_flag_key",
        "issuer_overlay_flags",
        ["flag_key"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_issuer_overlay_flags_flag_key", table_name="issuer_overlay_flags")
    op.drop_table("issuer_overlay_flags")

    op.drop_index("idx_issuer_classifications_subindustry", table_name="issuer_classifications")
    op.drop_index("idx_issuer_classifications_source", table_name="issuer_classifications")
    op.drop_table("issuer_classifications")

    op.drop_index(
        "idx_valuation_template_mappings_template",
        table_name="valuation_template_mappings",
    )
    op.drop_table("valuation_template_mappings")

    op.drop_index("idx_taxonomy_mappings_subindustry", table_name="taxonomy_mappings")
    op.drop_index("idx_taxonomy_mappings_standard_code", table_name="taxonomy_mappings")
    op.drop_table("taxonomy_mappings")

    op.drop_index("idx_taxonomy_nodes_parent", table_name="taxonomy_nodes")
    op.drop_index("idx_taxonomy_nodes_level", table_name="taxonomy_nodes")
    op.drop_table("taxonomy_nodes")
