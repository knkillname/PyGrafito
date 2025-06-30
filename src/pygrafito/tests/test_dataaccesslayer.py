"""
Unit tests for the pygrafito.dataaccesslayer module, themed around TRON: Legacy.
"""

import sqlite3
import unittest

from ..dataaccesslayer import EntityType, GraphDB


class TestGraphDB(unittest.TestCase):
    """
    Test suite for the GraphDB data access layer, TRON-themed.
    """

    def setUp(self) -> None:
        """
        Set up a new in-memory database for each test.
        """
        self.db = GraphDB(":memory:")
        self.db.connect()

    def tearDown(self) -> None:
        """
        Close the database connection after each test.
        """
        self.db.close()

    # --- Create Tests ---

    def test_create_node(self) -> None:
        """Test creating a 'Person' node for Kevin Flynn and a 'Program' node for CLU."""
        flynn_id = self.db.create_node(
            label="Person", properties={"name": "Kevin Flynn"}
        )
        self.assertEqual(flynn_id, 1)

        clu_props = {"name": "CLU", "version": 2.0, "creator": "Kevin Flynn"}
        clu_id = self.db.create_node(label="Program", properties=clu_props)
        self.assertEqual(clu_id, 2)

        # Updated call to get_properties
        retrieved_props = self.db.get_properties(
            entity_type=EntityType.NODE, entity_id=clu_id
        )
        self.assertDictEqual(clu_props, retrieved_props)

    def test_create_edge(self) -> None:
        """Test creating the 'FATHER_OF' relationship between Kevin and Sam Flynn."""
        kevin_id = self.db.create_node(
            label="Person", properties={"name": "Kevin Flynn"}
        )
        sam_id = self.db.create_node(label="Person", properties={"name": "Sam Flynn"})

        props = {"since": "birth", "type": "biological"}
        edge_id = self.db.create_edge(kevin_id, sam_id, "FATHER_OF", properties=props)
        self.assertEqual(edge_id, 1)

        # Updated call to get_properties
        retrieved_props = self.db.get_properties(
            entity_type=EntityType.EDGE, entity_id=edge_id
        )
        self.assertDictEqual(props, retrieved_props)

    def test_foreign_key_constraint_on_edge(self) -> None:
        """Test that creating an edge to a non-existent User fails."""
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.create_edge(source_node_id=99, target_id=100, label="CREATED")

    # --- Read Tests ---

    def test_find_nodes(self) -> None:
        """Test finding Users, Programs, and ISOs."""
        self.db.create_node("Person", {"name": "Kevin Flynn"})
        self.db.create_node("Person", {"name": "Sam Flynn"})
        self.db.create_node("ISO", {"name": "Quorra"})

        person_nodes = self.db.find_nodes(label="Person")
        self.assertEqual(len(person_nodes), 2)
        self.assertIsInstance(person_nodes[0], dict)

        sam_node = self.db.find_nodes(label="Person", properties={"name": "Sam Flynn"})
        self.assertEqual(len(sam_node), 1)
        self.assertEqual(sam_node[0]["id"], 2)

    def test_find_edges(self) -> None:
        """Test finding specific relationships in the Grid."""
        kevin_id = self.db.create_node("Person", {"name": "Kevin Flynn"})
        clu_id = self.db.create_node("Program", {"name": "CLU"})

        self.db.create_edge(kevin_id, clu_id, "CREATED", {"year": 1985})

        creations = self.db.find_edges(source_node_id=kevin_id)
        self.assertEqual(len(creations), 1)

    # --- Update / Delete Property Tests ---

    def test_clu_properties_evolution(self) -> None:
        """Test CLU's changing properties."""
        clu_id = self.db.create_node(
            "Program", {"name": "CLU", "purpose": "Perfection"}
        )

        # Updated call to set_properties
        self.db.set_properties(
            entity_type=EntityType.NODE,
            properties={"purpose": "Control", "directive": "Destroy ISOs"},
            entity_id=clu_id,
        )

        expected_props = {
            "name": "CLU",
            "purpose": "Control",
            "directive": "Destroy ISOs",
        }
        retrieved_props = self.db.get_properties(
            entity_type=EntityType.NODE, entity_id=clu_id
        )
        self.assertDictEqual(retrieved_props, expected_props)

        # Updated call to remove_properties
        self.db.remove_properties(
            entity_type=EntityType.NODE, keys=["purpose"], entity_id=clu_id
        )
        self.assertNotIn(
            "purpose",
            self.db.get_properties(entity_type=EntityType.NODE, entity_id=clu_id),
        )

    def test_grid_metadata(self) -> None:
        """Test setting and removing graph-level metadata about The Grid."""
        # Updated call to get_properties
        self.assertDictEqual(self.db.get_properties(entity_type=EntityType.GRAPH), {})

        # Updated call to set_properties
        self.db.set_properties(
            entity_type=EntityType.GRAPH,
            properties={"version": "2.0", "status": "Active"},
        )
        self.assertDictEqual(
            self.db.get_properties(EntityType.GRAPH),
            {"version": "2.0", "status": "Active"},
        )

        # Updated call to remove_properties
        self.db.remove_properties(entity_type=EntityType.GRAPH, keys=["version"])
        self.assertDictEqual(
            self.db.get_properties(EntityType.GRAPH), {"status": "Active"}
        )

    # --- Delete Entity Tests ---

    def test_delete_edge(self) -> None:
        """Test that deleting an edge also removes its properties."""
        kevin_id = self.db.create_node("Person")
        quorra_id = self.db.create_node("ISO")
        allegiance_id = self.db.create_edge(
            quorra_id, kevin_id, "ALLY_OF", {"strength": 10}
        )

        self.db.delete_edge(allegiance_id)

        self.assertEqual(len(self.db.find_edges(label="ALLY_OF")), 0)
        # Updated call to get_properties
        self.assertDictEqual(self.db.get_properties(EntityType.EDGE, allegiance_id), {})

    def test_delete_creator_cascades_to_creations(self) -> None:
        """Test that deleting the creator (Kevin) cascades to his direct relationships."""
        kevin_id = self.db.create_node("Person", {"name": "Kevin Flynn"})
        clu_id = self.db.create_node("Program", {"name": "CLU"})
        e1_id = self.db.create_edge(kevin_id, clu_id, "CREATED")

        self.db.delete_node(kevin_id)

        self.assertEqual(len(self.db.find_nodes(label="Person")), 0)
        self.assertEqual(len(self.db.find_nodes(label="Program")), 1)
        self.assertEqual(len(self.db.find_edges(label="CREATED")), 0)
        # Updated calls to get_properties
        self.assertDictEqual(self.db.get_properties(EntityType.NODE, kevin_id), {})
        self.assertDictEqual(self.db.get_properties(EntityType.EDGE, e1_id), {})


if __name__ == "__main__":
    unittest.main()
