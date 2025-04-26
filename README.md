# 🚀 MCP Databricks

<div align="center">
  <img src="https://img.shields.io/badge/python-3.11+-blue.svg" alt="Python 3.11+">
  <img src="https://img.shields.io/badge/platform-Databricks-orange.svg" alt="Databricks">
  <img src="https://img.shields.io/badge/MCP-Protocol-green.svg" alt="MCP Protocol">
</div>

<p align="center">
  <b>A powerful Databricks integration for AI assistants via Model Context Protocol</b>
</p>

## 📖 Introduction

MCP Databricks seamlessly connects AI assistants to your Databricks workspaces through the Model Context Protocol (MCP). Built with Python, it provides a rich collection of tools for managing virtually every aspect of your Databricks environment.

With this server, AI assistants like Claude can:
- 🔧 Manage compute resources with precision
- 📊 Execute SQL queries and analyze results
- 📁 Organize and manipulate workspace objects
- ✨ And much more!

## 🔍 Prerequisites

- 🐍 Python 3.11 or higher
- 💻 A Databricks workspace
- 🔑 Databricks Personal Access Token (PAT)
- 📦 Required Python packages (installed in setup)

## 🛠️ Setup

### 1️⃣ Clone the repository

```bash
git clone https://github.com/leminkhoa/databricks-mcp
cd mcp-databricks
```

### 2️⃣ Set up virtual environment

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

### 3️⃣ Install dependencies

```bash
pip install -e .
```

### 4️⃣ Configure environment variables

Create a `.env` file in the project root with your Databricks credentials:

```ini
# Databricks API Configuration
DATABRICKS_HOST="https://adb-<your workspace uri>.azuredatabricks.net/"
DATABRICKS_TOKEN="dapi_<your_token_here>"

# Server Configuration
SERVER_HOST="0.0.0.0"
SERVER_PORT="8000"
DEBUG="false"

# Logging Configuration
LOG_LEVEL="INFO"
```

> 💡 **Tip:** You can use the `env.sample` file as a template.

## 🚀 Usage

### Running the MCP Server

Start the server with a simple command:

```bash
python main.py
```

### Connecting to Claude or other MCP clients

This server uses the **stdio transport** for seamless compatibility with Claude Desktop and other MCP clients. After installing the server, you can immediately connect to it using your preferred MCP client.

## 🧰 Tools and Capabilities

The MCP Databricks server provides a comprehensive toolkit for managing your Databricks environment:

### 💻 Cluster Management

| Tool | Description |
|------|-------------|
| `list_clusters` | List all Databricks clusters in the workspace |
| `create_cluster` | Create a new Databricks cluster with customizable settings |
| `delete_cluster` | Delete a Databricks cluster by ID |
| `start_cluster` | Start a terminated Databricks cluster |
| `list_node_types` | List all available node types for Databricks clusters |
| `list_spark_versions` | List all available Spark versions for Databricks clusters |
| `get_cluster` | Get detailed information about a specific Databricks cluster |

### 📊 SQL Warehouse Management

| Tool | Description |
|------|-------------|
| `list_sql_warehouses` | List all SQL warehouses in the workspace |
| `create_sql_warehouse` | Create a new SQL warehouse with configurable size and settings |
| `execute_sql_statement` | Execute SQL statements using a SQL warehouse |

### 📁 Workspace Objects

| Tool | Description |
|------|-------------|
| `delete_workspace_object` | Delete an object from the Databricks workspace |
| `get_workspace_object_status` | Get the status of an object in the Databricks workspace |
| `import_workspace_object` | Import an object into the Databricks workspace |
| `create_workspace_directory` | Create a directory in the Databricks workspace |

---

<p align="center">
  <i>Built with ❤️ for Databricks and AI integration</i>
</p>

