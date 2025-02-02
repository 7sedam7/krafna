class Krafna < Formula
  desc "Krafna is a terminal-based alternative to Obsidian's Dataview plugin, allowing you to query your Markdown files using standard SQL syntax."
  homepage "https://github.com/7sedam7/krafna"
  url "https://github.com/7sedam7/krafna/archive/refs/tags/v0.1.2.tar.gz"
  sha256 "9269c32048896d5463de10309c3ee85f3b5354695b7a8642c1195edd51250639"
  license "MIT"

  bottle do
    # sha256 cellar: :any_skip_relocation, arm64_big_sur: "SHA256_HASH"
  end

  depends_on "rust" => :build

  def install
    system "cargo", "install", *std_cargo_args
  end

  test do
    assert_empty shell_output("#{bin}/krafna --help").strip
    assert_match "Usage: krafna", shell_output("#{bin}/krafna --help")
  end
end

