"""
pygrafito.dataaccesslayer
-------------------------

Provide a data access layer for a property graph database using SQLite.

This module defines the core types and the `GraphDB` class for managing nodes, edges,
and their properties in a property graph model. It supports creation, querying,
updating, and deletion of graph entities, and is designed to be used as a context manager.

Classes
-------
NodeDict
    TypedDict representing a node in the property graph.
EdgeDict
    TypedDict representing an edge in the property graph.
EntityType
    Enum for entity types (node, edge, graph).
EntityConfigDict
    TypedDict for entity configuration metadata.
GraphDB
    Main data access layer for the property graph database.
"""

import importlib.resources
import itertools
import json
import sqlite3
from collections.abc import Iterator
from enum import StrEnum
from pathlib import Path
from typing import Any, Callable, Literal, Self, TypedDict, overload

__all__ = [
    "NodeDict",
    "EdgeDict",
    "EntityType",
    "EntityConfigDict",
    "GraphDB",
]


class NodeDict(TypedDict):
    """
    Represent the structure of a node in a property graph.

    Attributes
    ----------
    id : int
        The unique identifier for the node.
    label : str
        The label or name of the node.
    properties : dict[str, Any]
        A dictionary containing additional properties or metadata
        associated with the node.
    """

    id: int
    label: str
    properties: dict[str, Any]


class EdgeDict(TypedDict):
    """
    Represent the structure of an edge in a property graph.

    Attributes
    ----------
    id : int
        The unique identifier for the edge.
    source_id : int
        The unique identifier of the source node of the edge.
    target_id : int
        The unique identifier of the target node of the edge.
    label : str
        A label describing the edge.
    properties : dict[str, Any]
        A dictionary containing additional properties of the edge.
    """

    id: int
    source_id: int
    target_id: int
    label: str
    properties: dict[str, Any]


class EntityType(StrEnum):
    """
    Enumerate entity types in the property graph.

    Members
    -------
    NODE : str
        Represents a node entity.
    EDGE : str
        Represents an edge entity.
    GRAPH : str
        Represents the graph itself (for graph-level properties).
    """

    NODE = "node"
    EDGE = "edge"
    GRAPH = "graph"


class EntityConfigDict(TypedDict):
    """
    Describe the configuration for an entity type in the property graph.

    Attributes
    ----------
    table : str
        The table name for the entity.
    id_col : str
        The primary key column name for the entity.
    props_table : str
        The table name for the entity's properties.
    sql_fetch_select : str
        The SQL SELECT clause for fetching the entity.
    sql_fetch_from : str
        The SQL FROM clause for fetching the entity.
    sql_fetch_join_on : str
        The SQL JOIN ON clause for joining properties.
    reconstructor : Callable[[Any], NodeDict | EdgeDict]
        A function to reconstruct the entity from a database row.
    props_builder : Callable[[NodeDict | EdgeDict, str, str], None]
        A function to add a property to the entity from a key-value pair.
    """

    table: str
    id_col: str
    props_table: str
    sql_fetch_select: str
    sql_fetch_from: str
    sql_fetch_join_on: str
    reconstructor: Callable[[Any], NodeDict | EdgeDict]
    props_builder: Callable[[NodeDict | EdgeDict, str, str], None]


# Configuration for entity types in the property graph.
# This dictionary maps each EntityType to its configuration metadata.
# It includes table names, column names, SQL query fragments, and
# functions for reconstructing entities and building properties.
_ENTITY_CONFIG: dict[EntityType, EntityConfigDict] = {
    EntityType.NODE: {
        "table": "nodes",
        "id_col": "node_id",
        "props_table": "node_properties",
        "sql_fetch_select": "n.node_id, n.label",
        "sql_fetch_from": "nodes AS n",
        "sql_fetch_join_on": "n.node_id = np.node_id",
        "reconstructor": lambda row: NodeDict(id=row[0], label=row[1], properties={}),
        "props_builder": lambda entity, key, value: entity["properties"].__setitem__(
            key, json.loads(value)
        ),
    },
    EntityType.EDGE: {
        "table": "edges",
        "id_col": "edge_id",
        "props_table": "edge_properties",
        "sql_fetch_select": "e.edge_id, e.source_id, e.target_id, e.label",
        "sql_fetch_from": "edges AS e",
        "sql_fetch_join_on": "e.edge_id = ep.edge_id",
        "reconstructor": lambda row: EdgeDict(
            id=row[0], source_id=row[1], target_id=row[2], label=row[3], properties={}
        ),
        "props_builder": lambda entity, key, value: entity["properties"].__setitem__(
            key, json.loads(value)
        ),
    },
}


class GraphDB:
    """
    Provide a data access layer for the property graph database.

    Manage the SQLite3 connection and provide methods for all fundamental
    graph operations (Create, Read, Update, Delete) using a property graph model.
    Designed to be used as a context manager.

    Parameters
    ----------
    db_path : Path or str
        Path to the SQLite database file.
    """

    def __init__(self, db_path: Path | str) -> None:
        self.db_path = Path(db_path)
        self._connection: sqlite3.Connection | None = None

    def connect(self) -> None:
        """
        Establish a connection to the SQLite database and initialize the schema if needed.
        """
        if self._connection is None:
            self._connection = sqlite3.connect(self.db_path)
            self._connection.execute("PRAGMA foreign_keys = ON;")
            self._initialize_schema()

    def close(self) -> None:
        """
        Close the SQLite database connection if it is open.
        """
        if self._connection:
            self._connection.close()
            self._connection = None

    def _validate_connection(self) -> sqlite3.Connection:
        """
        Check if the db connection is established and return it.
        This pattern helps with static analysis (e.g., Mypy).

        Returns
        -------
        sqlite3.Connection
            The active database connection object.

        Raises
        ------
        RuntimeError
            If the connection is not available.
        """
        if self._connection is None:
            raise RuntimeError(
                "Database connection is not available. "
                "Ensure you are using the GraphDB instance as a context manager."
            )
        return self._connection

    def _initialize_schema(self) -> None:
        """
        Initialize the database schema from the schema.sql resource file.
        """
        connection = self._validate_connection()
        if not __package__:
            raise ImportError(
                "This module must be run as part of a package to find schema."
            )
        with (importlib.resources.files(__package__) / "schema.sql").open("r") as f:
            schema_sql = f.read()
        connection.executescript(schema_sql)

    def create_node(self, label: str, properties: dict[str, Any] | None = None) -> int:
        """
        Create a new node in the graph with the given label and properties.

        Parameters
        ----------
        label : str
            The label for the node.
        properties : dict[str, Any], optional
            Properties to associate with the node.

        Returns
        -------
        int
            The ID of the newly created node.

        Raises
        ------
        RuntimeError
            If the database connection is not available or node creation fails.
        """
        connection = self._validate_connection()
        with connection:
            cursor = connection.cursor()
            cursor.execute("INSERT INTO nodes (label) VALUES (?)", (label,))
            node_id = cursor.lastrowid
            if node_id is None:
                raise RuntimeError("Failed to retrieve lastrowid after node insertion.")
            if properties:
                self.set_properties(EntityType.NODE, node_id, properties)
        return node_id

    def create_edge(
        self,
        source_node_id: int,
        target_id: int,
        label: str,
        properties: dict[str, Any] | None = None,
    ) -> int:
        """
        Create a new edge (relationship) between two nodes.

        Parameters
        ----------
        source_node_id : int
            The ID of the source node.
        target_id : int
            The ID of the target node.
        label : str
            The label for the edge.
        properties : dict[str, Any], optional
            Properties to associate with the edge.

        Returns
        -------
        int
            The ID of the newly created edge.

        Raises
        ------
        RuntimeError
            If the database connection is not available or edge creation fails.
        """
        connection = self._validate_connection()
        with connection:
            cursor = connection.cursor()
            cursor.execute(
                "INSERT INTO edges (source_id, target_id, label) VALUES (?, ?, ?)",
                (source_node_id, target_id, label),
            )
            edge_id = cursor.lastrowid
            if edge_id is None:
                raise RuntimeError("Failed to retrieve lastrowid after edge insertion.")
            if properties:
                self.set_properties(EntityType.EDGE, edge_id, properties)
        return edge_id

    @overload
    def _fetch_and_reconstruct(
        self, entity_type: Literal[EntityType.NODE], entity_ids: list[int]
    ) -> list[NodeDict]: ...

    @overload
    def _fetch_and_reconstruct(
        self, entity_type: Literal[EntityType.EDGE], entity_ids: list[int]
    ) -> list[EdgeDict]: ...

    def _fetch_and_reconstruct(
        self, entity_type: EntityType, entity_ids: list[int]
    ) -> list[NodeDict] | list[EdgeDict]:
        """
        Fetch and reconstruct nodes or edges from the database by their IDs.

        Parameters
        ----------
        entity_type : EntityType
            The type of entity to fetch (NODE or EDGE).
        entity_ids : list of int
            The list of entity IDs to fetch.

        Returns
        -------
        list[NodeDict] or list[EdgeDict]
            A list of reconstructed node or edge dictionaries.
        """
        connection = self._validate_connection()
        if not entity_ids:
            return []
        config = _ENTITY_CONFIG[entity_type]
        id_placeholders = ",".join("?" * len(entity_ids))
        props_alias = "ep" if entity_type == EntityType.EDGE else "np"
        sql_fetch = f"""
            SELECT {config['sql_fetch_select']}, {props_alias}.key, {props_alias}.value
            FROM {config['sql_fetch_from']}
            LEFT JOIN {config['props_table']} AS {props_alias} ON {config['sql_fetch_join_on']}
            WHERE {config['table']}.{config['id_col']} IN ({id_placeholders})
            ORDER BY {config['table']}.{config['id_col']}
        """
        cursor = connection.cursor()
        cursor.execute(sql_fetch, tuple(entity_ids))
        entities_by_id = {}
        for row in cursor:
            entity_id = row[0]
            if entity_id not in entities_by_id:
                entities_by_id[entity_id] = config["reconstructor"](row)
            key, value = row[-2], row[-1]
            if key is not None:
                config["props_builder"](entities_by_id[entity_id], key, value)
        return list(entities_by_id.values())

    def find_nodes(
        self, label: str | None = None, properties: dict[str, Any] | None = None
    ) -> list[NodeDict]:
        """
        Find nodes in the graph by label and/or properties.

        Parameters
        ----------
        label : str or None, optional
            The label of the nodes to find. If None, any label is matched.
        properties : dict[str, Any] or None, optional
            Properties that the nodes must have. All must match (AND logic).

        Returns
        -------
        list[NodeDict]
            A list of node dictionaries matching the criteria.

        Raises
        ------
        ValueError
            If both label and properties are None.
        """
        connection = self._validate_connection()
        if label is None and properties is None:
            raise ValueError(
                "At least one of 'label' or 'properties' must be provided."
            )
        query_params: list[Any] = []
        if properties:
            prop_clauses = " OR ".join(
                ("(np.key = ? AND np.value = ?)" for _ in properties)
            )
            prop_params = itertools.chain.from_iterable(
                (key, json.dumps(value)) for key, value in properties.items()
            )
            query_params.extend(prop_params)
            sql = f"""
                SELECT np.node_id FROM node_properties AS np
                {'JOIN nodes AS n ON np.node_id = n.node_id' if label else ''}
                WHERE ({prop_clauses}) {'AND n.label = ?' if label else ''}
                GROUP BY np.node_id HAVING COUNT(np.node_id) = ?
            """
            if label:
                query_params.append(label)
            query_params.append(len(properties))
        else:
            sql = "SELECT node_id FROM nodes WHERE label = ?"
            query_params.append(label)
        cursor = connection.cursor()
        cursor.execute(sql, tuple(query_params))
        node_ids = [row[0] for row in cursor]
        return self._fetch_and_reconstruct(EntityType.NODE, node_ids)

    def find_edges(
        self,
        source_node_id: int | None = None,
        target_node_id: int | None = None,
        label: str | None = None,
        properties: dict[str, Any] | None = None,
    ) -> list[EdgeDict]:
        """
        Find edges in the graph by source, target, label, and/or properties.

        Parameters
        ----------
        source_node_id : int or None, optional
            The ID of the source node. If None, any source is matched.
        target_node_id : int or None, optional
            The ID of the target node. If None, any target is matched.
        label : str or None, optional
            The label of the edge. If None, any label is matched.
        properties : dict[str, Any] or None, optional
            Properties that the edges must have. All must match (AND logic).

        Returns
        -------
        list[EdgeDict]
            A list of edge dictionaries matching the criteria.

        Raises
        ------
        ValueError
            If all search criteria are None.
        """
        connection = self._validate_connection()
        if all(p is None for p in [source_node_id, target_node_id, label, properties]):
            raise ValueError("At least one search criterion must be provided.")
        query_params: list[Any] = []
        base_where_clauses: list[str] = []
        if source_node_id is not None:
            base_where_clauses.append("e.source_id = ?")
            query_params.append(source_node_id)
        if target_node_id is not None:
            base_where_clauses.append("e.target_id = ?")
            query_params.append(target_node_id)
        if label is not None:
            base_where_clauses.append("e.label = ?")
            query_params.append(label)
        if properties:
            prop_clauses = " OR ".join(
                ("(ep.key = ? AND ep.value = ?)" for _ in properties)
            )
            base_where_sql = (
                f" AND ({' AND '.join(base_where_clauses)})"
                if base_where_clauses
                else ""
            )
            sql = f"""
                SELECT ep.edge_id FROM edge_properties AS ep
                JOIN edges AS e ON ep.edge_id = e.edge_id
                WHERE ({prop_clauses}){base_where_sql}
                GROUP BY ep.edge_id
                HAVING COUNT(ep.edge_id) = ?
            """
            prop_params = itertools.chain.from_iterable(
                (key, json.dumps(value)) for key, value in properties.items()
            )
            query_params.extend(prop_params)
            query_params.append(len(properties))
        else:
            where_sql = (
                f"WHERE {' AND '.join(base_where_clauses)}"
                if base_where_clauses
                else ""
            )
            sql = f"SELECT e.edge_id FROM edges AS e {where_sql}"
        cursor = connection.cursor()
        cursor.execute(sql, tuple(query_params))
        edge_ids = [row[0] for row in cursor]
        return self._fetch_and_reconstruct(EntityType.EDGE, edge_ids)

    def set_properties(
        self, entity_type: EntityType, entity_id: int | None, properties: dict[str, Any]
    ) -> None:
        """
        Set or update properties for a graph, node, or edge entity.

        Parameters
        ----------
        entity_type : EntityType
            The type of entity (GRAPH, NODE, or EDGE).
        entity_id : int or None
            The ID of the entity (required for NODE and EDGE, ignored for GRAPH).
        properties : dict[str, Any]
            The properties to set or update.
        """
        connection = self._validate_connection()
        data_generator: Iterator[tuple[Any, ...]]
        if entity_type == EntityType.GRAPH:
            sql = (
                "INSERT INTO graph_properties (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value;"
            )
            data_generator = ((k, json.dumps(v)) for k, v in properties.items())
        else:
            if entity_id is None:
                raise ValueError("entity_id must be provided for nodes and edges.")
            config = _ENTITY_CONFIG[entity_type]
            props_table = config["props_table"]
            id_col = config["id_col"]
            sql = (
                f"INSERT INTO {props_table} ({id_col}, key, value) VALUES (?, ?, ?) "
                f"ON CONFLICT({id_col}, key) DO UPDATE SET value = excluded.value;"
            )
            data_generator = (
                (entity_id, k, json.dumps(v)) for k, v in properties.items()
            )
        with connection:
            connection.executemany(sql, data_generator)

    def remove_properties(
        self, entity_type: EntityType, entity_id: int | None, keys: list[str]
    ) -> None:
        """
        Remove properties from a graph, node, or edge entity.

        Parameters
        ----------
        entity_type : EntityType
            The type of entity (GRAPH, NODE, or EDGE).
        entity_id : int or None
            The ID of the entity (required for NODE and EDGE, ignored for GRAPH).
        keys : list[str]
            The property keys to remove.
        """
        connection = self._validate_connection()
        if not keys:
            return
        key_placeholders = ",".join("?" * len(keys))
        params: tuple[Any, ...]
        if entity_type == EntityType.GRAPH:
            sql = f"DELETE FROM graph_properties WHERE key IN ({key_placeholders})"
            params = tuple(keys)
        else:
            if entity_id is None:
                raise ValueError("entity_id must be provided for nodes and edges.")
            config = _ENTITY_CONFIG[entity_type]
            props_table = config["props_table"]
            id_col = config["id_col"]
            sql = f"DELETE FROM {props_table} WHERE {id_col} = ? AND key IN ({key_placeholders})"
            params = (entity_id, *keys)
        with connection:
            connection.execute(sql, params)

    def get_properties(
        self, entity_type: EntityType, entity_id: int | None
    ) -> dict[str, Any]:
        """
        Retrieve all properties for a graph, node, or edge entity.

        Parameters
        ----------
        entity_type : EntityType
            The type of entity (GRAPH, NODE, or EDGE).
        entity_id : int or None
            The ID of the entity (required for NODE and EDGE, ignored for GRAPH).

        Returns
        -------
        dict[str, Any]
            A dictionary of property keys and their values.
        """
        connection = self._validate_connection()
        params: tuple[Any, ...]
        if entity_type == EntityType.GRAPH:
            sql = "SELECT key, value FROM graph_properties"
            params = ()
        else:
            if entity_id is None:
                raise ValueError("entity_id must be provided for nodes and edges.")
            config = _ENTITY_CONFIG[entity_type]
            props_table = config["props_table"]
            id_col = config["id_col"]
            sql = f"SELECT key, value FROM {props_table} WHERE {id_col} = ?"
            params = (entity_id,)
        cursor = connection.cursor()
        cursor.execute(sql, params)
        return {key: json.loads(value) for key, value in cursor}

    def delete_node(self, node_id: int) -> None:
        """
        Delete a node from the graph by its ID.

        Parameters
        ----------
        node_id : int
            The ID of the node to delete.
        """
        connection = self._validate_connection()
        sql = "DELETE FROM nodes WHERE node_id = ?"
        with connection:
            connection.execute(sql, (node_id,))

    def delete_edge(self, edge_id: int) -> None:
        """
        Delete an edge from the graph by its ID.

        Parameters
        ----------
        edge_id : int
            The ID of the edge to delete.
        """
        connection = self._validate_connection()
        sql = "DELETE FROM edges WHERE edge_id = ?"
        with connection:
            connection.execute(sql, (edge_id,))

    def __enter__(self) -> Self:
        """
        Enter the runtime context related to this object.

        Returns
        -------
        Self
            The GraphDB instance itself.
        """
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Exit the runtime context and close the database connection.

        Parameters
        ----------
        exc_type : type
            The exception type.
        exc_val : Exception
            The exception value.
        exc_tb : traceback
            The traceback object.
        """
        self.close()
