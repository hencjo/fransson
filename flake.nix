{
  description = "Reconcile, clone, stream, dump, and restore Kafka topics";

  inputs = {
    devenv.url = "github:cachix/devenv";
    devenv.inputs.nixpkgs.follows = "nixpkgs";

    nixpkgs.url = "github:cachix/devenv-nixpkgs/rolling";

    rust-overlay.url = "github:oxalica/rust-overlay";
    rust-overlay.inputs.nixpkgs.follows = "nixpkgs";

    systems.url = "github:nix-systems/default";
  };

  outputs =
    inputs@{
      self,
      devenv,
      nixpkgs,
      rust-overlay,
      systems,
      ...
    }:
    let
      eachSystem = nixpkgs.lib.genAttrs (import systems);
      cargoToml = builtins.fromTOML (builtins.readFile ./Cargo.toml);

      pkgsFor =
        system:
        import nixpkgs {
          inherit system;
          overlays = [ (import rust-overlay) ];
        };
    in
    {
      devShells = eachSystem (
        system:
        let
          pkgs = pkgsFor system;
        in
        {
          default = devenv.lib.mkShell {
            inherit inputs pkgs;
            modules = [
              {
                devenv.root = builtins.toString ./.;
              }
              ./devenv.nix
            ];
          };
        }
      );

      packages = eachSystem (
        system:
        let
          pkgs = pkgsFor system;
          fransson = pkgs.rustPlatform.buildRustPackage {
            pname = "fransson";
            version = cargoToml.package.version;
            src = ./.;

            cargoLock = {
              lockFile = ./Cargo.lock;
            };

            nativeBuildInputs = with pkgs; [
              cmake
              gcc
              gnumake
              perl
              pkg-config
            ];

            buildInputs = [
              pkgs.curl.dev
              pkgs.cyrus_sasl.dev
            ];
          };
        in
        {
          inherit fransson;
          default = fransson;
        }
      );
    };
}
