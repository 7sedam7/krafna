# Krafna

[![Release and Publish](https://github.com/7sedam7/krafna/actions/workflows/release.yml/badge.svg)](https://github.com/7sedam7/krafna/actions/workflows/release.yml)

![Krafna is a terminal-based alternative to Obsidian's Dataview plugin, allowing you to query your Markdown files using standard SQL syntax.](demo.gif)

## Features

- Query Markdown files in a directory using SQL-like syntax
- Support for frontmatter data extraction
- Flexible output formats (TSV and JSON)
- Compatible with Neovim plugin [Perec](https://github.com/7sedam7/perec.nvim)

## Installation

### Cargo (Recommended)

```bash
cargo install krafna
```

### Homebrew

```bash
brew tap 7sedam7/krafna
brew install krafna
```

## Usage

### Basic Query

```bash
krafna "SELECT title, tags FROM FRONTMATTER_DATA('~/.notes')"
```

### Find Files

```bash
krafna --find ~/.notes
```

### Output as JSON

```bash
krafna "SELECT * FROM FRONTMATTER_DATA('~/.notes')" --json
```

### Include Specific Fields

```bash
krafna "SELECT * FROM FRONTMATTER_DATA('~/.notes')" --include-fields title,tags
```

## Syntax Differences from Dataview

- Uses standard SQL syntax
- Selection of "table" to query is done with `FROM FRONTMATTER_DATA("<path>")` function, that makes all md files within <path> a row (their frontmatter data). Currently no other sources and no JOINs. I plan to add them later.
- Not all Dataview features are implemented yet

## Neovim Integration

Use with the [Perec](https://github.com/7sedam7/perec) Neovim plugin for seamless integration.

## Roadmap
(not in priority order)
[ ] TODOs
[ ] add group by
[ ] add . support for accesing sub-fields (file.name)
[ ]  * migrate file_name, etc under file (name, path, created, accessed, modified)
[ ] add default variables (today)
[ ] add functions
[ ]  * DATE("some-date") -> new type date
[ ] implement val -> val operators
[ ] add querying of TODOs
[ ] add querying of links between notes

## Author

[7sedam7](https://github.com/7sedam7)
