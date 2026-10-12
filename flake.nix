{
  description = "Claude Agent SDK Rust development environment";

  # Keep cached build dependencies on this pin; its Rust compiler is 1.98.1.
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/c7def046b9a883d46974757852106483d741586f";
  inputs.fenix = {
    url = "github:nix-community/fenix/329aea43982331f709ab302bbba51b024c5769e2";
    inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs =
    { nixpkgs, fenix, ... }:
    let
      systems = [ "x86_64-linux" ];
      rustToolchainFor = system: fenix.packages.${system}.fromToolchainName {
        name = "1.99.0";
        sha256 = "sha256-zm3dyIY2T414ZRR3EhLOvptzG6gta4WZUcawzMUWtqI=";
      };
      forAll = f:
        nixpkgs.lib.genAttrs systems (system:
          f nixpkgs.legacyPackages.${system} (rustToolchainFor system));
    in
    {
      devShells = forAll (pkgs: rustToolchain: {
        default = pkgs.mkShell {
          packages = [
            (rustToolchain.withComponents [
              "cargo"
              "rustc"
              "rust-std"
              "clippy"
              "rustfmt"
              "rust-analyzer"
            ])
          ];
          shellHook = ''
            # Put Rust build output on the non-snapshotted cache dataset. The canonical
            # path keeps sibling JJ workspaces with the same basename apart.
            CANONICAL_WORKSPACE_ROOT="$(pwd -P; printf 'x')"
            CANONICAL_WORKSPACE_ROOT="''${CANONICAL_WORKSPACE_ROOT%x}"
            CANONICAL_WORKSPACE_ROOT="''${CANONICAL_WORKSPACE_ROOT%$'\n'}"
            WORKSPACE_PATH_HASH="$(printf '%s' "$CANONICAL_WORKSPACE_ROOT" | sha256sum)"
            WORKSPACE_PATH_HASH="''${WORKSPACE_PATH_HASH%% *}"
            CARGO_TARGET_CACHE="$HOME/.cache/cargo-target/claude-agent-sdk-rs-workspace-$WORKSPACE_PATH_HASH"

            # Let Cargo resolve target/ through the workspace symlink, including
            # when a caller inherited a target override from another workspace.
            unset CARGO_TARGET_DIR
            mkdir -p "$CARGO_TARGET_CACHE"

            if [ -L target ]; then
              CURRENT_TARGET="$(readlink target)"
              if [ "$CURRENT_TARGET" = "$CARGO_TARGET_CACHE" ]; then
                : # Already points to this workspace's cache.
              else
                echo "Migrating target/ symlink: $CURRENT_TARGET → $CARGO_TARGET_CACHE"
                rm target
                ln -sfn "$CARGO_TARGET_CACHE" target
              fi
            elif [ -d target ]; then
              echo "ERROR: target/ is a real directory; refusing to replace or delete it." >&2
              echo "Resolve the preserved directory deliberately, then re-enter the workspace." >&2
              exit 1
            elif [ ! -e target ]; then
              ln -sfn "$CARGO_TARGET_CACHE" target
              echo "target/ → $CARGO_TARGET_CACHE (non-snapshotted cache dataset)"
            fi
          '';
        };
      });
    };
}
