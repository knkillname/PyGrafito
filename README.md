# PyGraphite

A modern, Pythonic graph database library powered by SQLite.

## About The Project

PyGraphite is a pure Python library designed to provide an intuitive, object-oriented interface for working with graph data. It draws inspiration from the power of Neo4j's property graph model but is built on the simplicity and portability of a local SQLite database file.

The goal is to create a lightweight, zero-dependency (outside of the standard library) tool for developers who need graph capabilities without the overhead of a dedicated database server.

## Core Concepts

The data model is a simplified version of the Labeled Property Graph model:

* **Nodes:** The primary entities in the graph (e.g., a User, a Program).
* **Edges:** The directed connections that represent relationships between nodes (e.g., `FATHER_OF`, `CREATED`).
* **Label:** Each node has exactly one label that defines its type (e.g., 'User'). This maps directly to a Python class.
* **Properties:** Both nodes and edges can have arbitrary key-value properties (e.g., `name='Kevin Flynn'`, `version=2`).

## ⚠️ Current Status: First Commit

This project is in the earliest stages of development. The foundational database schema has been designed and finalized, but the Python ORM layer that interacts with it is not yet implemented. The `Usage` section below represents the target API we are building towards.
