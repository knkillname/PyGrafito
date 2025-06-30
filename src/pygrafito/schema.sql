-- Asistente Pythonic: Graph Database Schema v3.0
-- Backend: SQLite3
-- Design: Segregated EAV tables for graph, nodes, and edges.

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

-- =================================================================
--  Core entity tables with independent, auto-incrementing IDs.
-- =================================================================
CREATE TABLE IF NOT EXISTS nodes (
    node_id INTEGER PRIMARY KEY,
    label   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_nodes_label ON nodes(label);

CREATE TABLE IF NOT EXISTS edges (
    edge_id   INTEGER PRIMARY KEY,
    source_id INTEGER NOT NULL,
    target_id INTEGER NOT NULL,
    label     TEXT NOT NULL,
    FOREIGN KEY (source_id) REFERENCES nodes(node_id) ON DELETE CASCADE,
    FOREIGN KEY (target_id) REFERENCES nodes(node_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_edges_source_id ON edges(source_id);
CREATE INDEX IF NOT EXISTS idx_edges_target_id ON edges(target_id);


-- =================================================================
--  Segregated EAV Property Tables
-- =================================================================

-- 1. For Graph-level properties
CREATE TABLE IF NOT EXISTS graph_properties (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL -- JSON-encoded value
);

-- 2. For Node properties
CREATE TABLE IF NOT EXISTS node_properties (
    node_id INTEGER NOT NULL,
    key     TEXT NOT NULL,
    value   TEXT NOT NULL, -- JSON-encoded value
    PRIMARY KEY (node_id, key),
    FOREIGN KEY (node_id) REFERENCES nodes(node_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_node_properties_key ON node_properties(key);

-- 3. For Edge properties
CREATE TABLE IF NOT EXISTS edge_properties (
    edge_id INTEGER NOT NULL,
    key     TEXT NOT NULL,
    value   TEXT NOT NULL, -- JSON-encoded value
    PRIMARY KEY (edge_id, key),
    FOREIGN KEY (edge_id) REFERENCES edges(edge_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_edge_properties_key ON edge_properties(key);