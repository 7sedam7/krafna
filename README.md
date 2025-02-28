# Krafna

[![codecov](https://codecov.io/gh/7sedam7/krafna/branch/main/graph/badge.svg)](https://codecov.io/gh/7sedam7/krafna)
![CodeRabbit Pull Request Reviews](https://img.shields.io/coderabbit/prs/github/7sedam7/krafna)
![Crates.io Version](https://img.shields.io/crates/v/krafna)
![Crates.io Total Downloads](https://img.shields.io/crates/d/krafna)

![Krafna is a CLI tool for SQL querying frontmatter data. Similar to Obsidian's Dataview plugin](demo.gif)

## Features

- Query Markdown files in a directory using SQL-like syntax
- Support for frontmatter data extraction
- Flexible output formats (TSV and JSON)
- Compatible with Neovim plugin [Perec](https://github.com/7sedam7/perec.nvim)

## Performance

Some users have suggested storing frontmatter information in a database for performance reasons.
Benchmarking on a base Mac mini M4 shows that Krafna can query ~2500 files within ~100ms.
While file fetching and parsing takes about 97% of the time, potential optimizations include caching parsed files and only parsing modified files after cache creation.
However, the current performance is more than good enough, so the focus will remain on feature development for now.

Run benchmarks: (you can change the number of files that will be generated in bench/query_benchmark.rs)

``` bash
cargo bench
```

Run flamegraph: (For a cleaner flamegraph, consider temporarily disabling rayon’s parallelism by replacing `par_iter()` with `iter()`.)

``` bash
cargo install flamegraph
cargo flamegraph --root --bin krafna -- 'select file.name, tags from frontmatter_data("../krafna-bench/bench/") where "exampl" in tags'
```

## Installation

There are binaries available for Linux, macOS, and Windows under Releases.

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

```
Usage: krafna [OPTIONS] [QUERY]

Arguments:
  [QUERY]  The query to execute

Options:
      --select <SELECT>
          OVERRIDES SELECT fields from the query with "field1,field2"
      --from <FROM>
          From option in case you are implementing querying for specific FROM that you don't want to specify every time. This OVERRIDES the FROM part of the query!
      --include-fields <INCLUDE_FIELDS>
          include SELECT fields with "field1,field2" (prepends them to th front of the SELECT fields in the query)
      --find <FIND>
          Find option to find all krafna snippets within a dir
      --json
          Output results in JSON format
  -h, --help
          Print help

```

### SELECT

- Currently, you can only specify field names.
- There are extra added fields for the file data itself, acessible with file.<option> (options: name, path, created, accessed, modified).
- No support for *, functions, nor expressions yet.
- No support for AS yet.

### FROM

#### FRONTMATTER_DATA

- `FROM FRONTMATTER_DATA("<path>")`
- This will find all markdown files in the specified `<path>` and use their frontmatter data as rows.
- FIELDS:
    - `file.name` - name of the file
    - `file.path` - path to the file
    - `file.created` - date when the file was created
    - `file.accessed` - date when the file was last accessed
    - `file.modified` - date when the file was last modified
    - All other fields are from frontmatter data

#### MD_LINKS

- `FROM MD_LINKS("<path>")`
- This will find all the links in markdown files in the specified `<path>`. Each link is a separate row.
- FIELDS:
    - `file.*` - file data same as above
    - `type` - type of the link (inline, wiki)
    - `external` - true if the link is external (not a local file)
    - `url` - original url text from markdown file
    - `path` - interpreted path to the local file in case link is not external. (relies on that path being within argument specified `<path>`, otherwise it will be empty)
    - `text` - text of the link
    - `ord` - order of the link in the file

#### MD_TASKS

- `FROM MD_TASKS("<path>")`
- This will find all the tasks in markdown files in the specified `<path>`. Each link is a separate row.
- Tasks in markdown are defined as lines starting with `- [ ]` or `- [x]`
- FIELDS:
    - `file.*` - file data same as above
    - `checked` - true if the task is checked (`- [x]`)
    - `text` - text of the task
    - `ord` - order of the task in the file. If task is subtask, there is a '.' and then number for ordering within a parent task. Nesting is supported.
    - `parent` - parent `ord` of the task in the file. If the task is not a subtask, this will be empty


- More functions will come.
- No support for AS yet.

### WHERE

- Brackets are supported
- Operatortors AND, OR, IN, <, <=, >, >=, ==, !=, LIKE, NOT LIKE, +, -, *, /, **, // are supported
- Functions DATE(<some-date>, <optional-format>), DATEADD(<interval>, <number>, <date>, <optional-format>) are supported
- Arguments to functions can be hardcoded values or field names
- Nested functions, or expressions as arguments are NOT supported yet
- file. fields can be used in WHERE clause as well

### ORDER BY

- You can only specify field names followed by ASC or DESC
- Functions and expressions are NOT supported yet
- file. fields can be used in ORDER BY clause as well

### Other

- LIMIT, OFFSET, JOIN, HAVING, GROUP BY, DISTINCT, etc. are not supported yet.
- UPDATE and DELETE are not supported yet.


### Examples

#### Basic Query

```bash
krafna "SELECT title, tags FROM FRONTMATTER_DATA('~/.notes')"
```

#### Find Files

```bash
krafna --find ~/.notes
```

#### Output as JSON

```bash
krafna "SELECT * FROM FRONTMATTER_DATA('~/.notes')" --json
```

#### Include Specific Fields

```bash
krafna "SELECT * FROM FRONTMATTER_DATA('~/.notes')" --include-fields title,tags
```

## Neovim Integration

Use with the [Perec](https://github.com/7sedam7/perec) Neovim plugin for seamless integration.

## Roadmap

(not in priority order)
- [x] add . support for accesing sub-fields (file.name)
- [x]  * migrate file_name, etc under file (name, path, created, accessed, modified)
- [x] add default variables (today)
- [ ]  * change it so that it does not need to be on every row (can have a general_values hash that can be passed around, and value getters would first check there and then from the source)
- [ ] Implement pruning of AND and OR operators (mostly for better error messages, performance there is more than good enough)
- [ ] TODOs
- [x] Add tests for execution
- [ ] add suport for functions in SELECT
- [ ] add functions
- [ ]  * think about which functions to add
- [x]  * DATE("some-date", <format>) -> new type date
- [x]  * [DATEADD()](https://www.w3schools.com/sql/func_sqlserver_dateadd.asp)
- [x] implement val -> val operators
- [ ] UPDATE
- [ ] DELETE
- [ ] add AS to SELECT
- [ ] add querying of TODOs (think of a format similar to [todoist](https://www.todoist.com/help/articles/use-task-quick-add-in-todoist-va4Lhpzz))
- [ ]  * maybe abstract to query by regex
- [ ] add querying of links between notes
- [ ] think about which other sources would be cool to add
- [ ] add group by

## Acknowledgements

- [grey-matter-rs](https://github.com/the-alchemists-of-arland/gray-matter-rs) for parsing frontmatter data
- [rayon](https://github.com/rayon-rs/rayon) for parallelizing execution
- [CodeRabbit](https://coderabbit.io) for code reviews
- Various AI tools for help with answering questions faster then me searching on Google/StackOverflow


## Author

[7sedam7](https://github.com/7sedam7)
