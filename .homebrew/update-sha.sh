#!/bin/bash

# Function to check required arguments
check_args() {
    if [ -z "$1" ] || [ -z "$2" ]; then
        echo "Error: Version and formula file path must be passed as arguments."
        echo "Usage: $0 <version> <path_to_formula_file>"
        exit 1
    fi
}

# Function to update main URL and SHA256
update_main_formula() {
    local version=$1
    local formula_file=$2
    local url="https://github.com/7sedam7/krafna/archive/refs/tags/v${version}.tar.gz"

    echo "Updating main formula URL and SHA256..."

    # Download the main tarball and calculate its SHA256
    local tmp_file="/tmp/krafna-main-${version}.tar.gz"
    wget -q -O "$tmp_file" "$url"
    local main_sha256=$(shasum -a 256 "$tmp_file" | awk '{ print $1 }')
    rm "$tmp_file"

    # Update URL and SHA256 in formula file
    sed -i "" "s|url \".*\"|url \"$url\"|" "$formula_file"
    sed -i "" "s|sha256 \".*\"|sha256 \"$main_sha256\"|" "$formula_file"
}

# Function to update root_url in bottle block
update_root_url() {
    local version=$1
    local formula_file=$2
    local root_url="https://github.com/7sedam7/krafna/releases/download/v${version}"

    # Check if root_url line exists in bottle block
    if grep -q "root_url" "$formula_file"; then
        sed -i "" "s|root_url \".*\"|root_url \"$root_url\"|" "$formula_file"
    else
        # Add root_url after bottle do
        sed -i "" "/bottle do/a\\
  root_url \"$root_url\"" "$formula_file"
    fi
}

# Function to map release target to bottle ID and filename pattern
get_bottle_info() {
    local target=$1
    case "$target" in
        "aarch64-apple-darwin")
            echo "arm64_apple_darwin krafna-*-aarch64-apple-darwin.tar.gz"
            ;;
        "x86_64-apple-darwin")
            echo "x86_64_apple_darwin krafna-*-x86_64-apple-darwin.tar.gz"
            ;;
        "x86_64-unknown-linux-musl")
            echo "x86_64_linux_musl krafna-*-x86_64-unknown-linux-musl.tar.gz"
            ;;
        "aarch64-unknown-linux-musl")
            echo "aarch64_linux_musl krafna-*-aarch64-unknown-linux-musl.tar.gz"
            ;;
        "x86_64-unknown-linux-gnu")
            echo "x86_64_linux krafna-*-amd64.deb"
            ;;
        "aarch64-unknown-linux-gnu")
            echo "arm64_linux krafna-*-arm64.deb"
            ;;
        *)
            echo ""
            ;;
    esac
}

# Main script
main() {
    local version=$1
    local formula_file=$2
    local release_url="https://github.com/7sedam7/krafna/releases/download/v${version}"
    local tmp_dir="/tmp/krafna-bottles-$$"

    # Create temporary directory
    mkdir -p "$tmp_dir"

    # Update main formula URL and SHA256
    update_main_formula "$version" "$formula_file"

    # Update root_url in bottle block
    update_root_url "$version" "$formula_file"

    # Define targets (architectures)
    local targets=(
        "aarch64-apple-darwin"
        "x86_64-apple-darwin"
        "x86_64-unknown-linux-musl"
        "aarch64-unknown-linux-musl"
        "x86_64-unknown-linux-gnu"
        "aarch64-unknown-linux-gnu"
    )

    echo "Updating bottle hashes..."

    # Process each target
    for target in "${targets[@]}"; do
        # Get bottle ID and filename pattern
        read -r bottle_id filename_pattern <<< "$(get_bottle_info "$target")"

        if [ -z "$bottle_id" ]; then
            echo "Skipping unknown target: $target"
            continue
        fi

        echo "Processing $target..."

        # Construct file URL
        local file_url="${release_url}/${filename_pattern/\*/$version}"

        # Download file
        wget -q -O "$tmp_dir/bottle.tar.gz" "$file_url"

        if [ $? -eq 0 ]; then
            # Calculate SHA256
            local sha256=$(shasum -a 256 "$tmp_dir/bottle.tar.gz" | awk '{ print $1 }')

            # Update formula
            sed -i "" "s|sha256 cellar: :any_skip_relocation, ${bottle_id}: \".*\"|sha256 cellar: :any_skip_relocation, ${bottle_id}: \"${sha256}\"|" "$formula_file"
            echo "  Updated hash for $bottle_id"
        else
            echo "  Failed to download bottle for $target"
        fi

        rm -f "$tmp_dir/bottle.tar.gz"
    done

    # Cleanup
    rmdir "$tmp_dir"

    echo "Formula update complete!"
}

# Run the script
check_args "$@"
main "$@"
