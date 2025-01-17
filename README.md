# Git Hooks

This repository uses the following Git hooks to ensure code quality and consistency.

## Pre-commit Hook

The pre-commit hook runs `cargo test` to ensure that all tests pass before committing.


### Setup
1 Navigate to the .git/hooks directory in your repository.
2 Create a file named pre-commit and add the following content:
```sh
#!/bin/sh
# Run cargo test and check for errors
echo "Running cargo test..."
cargo test
if [ $? -ne 0 ]; then
    echo "Tests failed. Commit aborted."
    exit 1
fi
```
3 Make the script executable: `chmod +x .git/hooks/pre-commit`
