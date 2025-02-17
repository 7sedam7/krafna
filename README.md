# Krafna

[![codecov](https://codecov.io/gh/7sedam7/krafna/branch/main/graph/badge.svg)](https://codecov.io/gh/7sedam7/krafna)
![CodeRabbit Pull Request Reviews](https://img.shields.io/coderabbit/prs/github/7sedam7/krafna)

![Krafna is a terminal-based alternative to Obsidian's Dataview plugin, allowing you to query your Markdown files using standard SQL syntax.](demo.gif)

## Features

- Query Markdown files in a directory using SQL-like syntax
- Support for frontmatter data extraction
- Flexible output formats (TSV and JSON)
- Compatible with Neovim plugin [Perec](https://github.com/7sedam7/perec.nvim)

## Installation

### Cargo

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
- [x] add . support for accesing sub-fields (file.name)
- [x]  * migrate file_name, etc under file (name, path, created, accessed, modified)
- [x] add default variables (today)
- [ ]  * change it so that it does not need to be on every row (can have a general_values hash that can be passed around, and value getters would first check there and then from the source)
- [ ] TODOs
- [ ] Add tests for execution
- [ ] add suport for functions in SELECT
- [ ] add functions
- [ ]  * think about which functions to add
- [ ]  * DATE("some-date", <format>) -> new type date
- [x]  * [DATEADD()](https://www.w3schools.com/sql/func_sqlserver_dateadd.asp)
- [ ] implement val -> val operators
- [ ] UPDATE
- [ ] DELETE
- [ ] add AS to SELECT
- [ ] add querying of TODOs (think of a format similar to [todoist](https://www.todoist.com/help/articles/use-task-quick-add-in-todoist-va4Lhpzz))
- [ ]  * maybe abstract to query by regex
- [ ] add querying of links between notes
- [ ] think about which other sources would be cool to add
- [ ] add group by

## Author

[7sedam7](https://github.com/7sedam7)
