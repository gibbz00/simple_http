{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    git-hooks = {
      url = "github:cachix/git-hooks.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    inputs@{ nixpkgs, flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = nixpkgs.lib.systems.flakeExposed;

      imports = [
        inputs.git-hooks.flakeModule
      ];

      perSystem =
        { config, pkgs, ... }:
        {
          devShells.default = pkgs.mkShell {
            name = "simple_http";

            inherit (config.pre-commit) shellHook;

            packages =
              with pkgs;
              [
                gcc
                xmake
                # would like these to be managed by xmake
                unzip
                gnum4
                pkg-config
                cmake
                ninja
                # for openssl3 package
                perl
                # mostly for clangd, clang-format and clang-tidy
                clang-tools
              ]
              ++ config.pre-commit.settings.enabledPackages;
          };

          pre-commit = {
            settings.hooks = {
              nixpkgs-fmt.enable = true;
              # clang-format.enable = true;
              # stylua.enable = true;
              # typos.enable = true;

              check-yaml.enable = true;
              check-json.enable = true;
              check-xml.enable = true;
              check-toml.enable = true;

              check-merge-conflicts.enable = true;
              forbid-new-submodules.enable = true;

              check-executables-have-shebangs.enable = true;
              end-of-file-fixer.enable = true;
              trim-trailing-whitespace.enable = true;
            };
          };
        };
    };
}
