{
  pkgs,
  lib,
  config,
  inputs,
  ...
}:

{
  packages = with pkgs; [
    git
    cyrus_sasl
    curl.dev
    cmake
    pkg-config
    gnumake
    perl
    gcc
  ];

  git-hooks.hooks = {
    rustfmt.enable = true;
    nixfmt.enable = true;
  };

  languages.rust = {
    enable = true;
    channel = "stable";
    targets = [ "x86_64-unknown-linux-musl" ];
    components = [
      "rustc"
      "cargo"
      "clippy"
      "rustfmt"
      "rust-analyzer"
    ];
  };
}
