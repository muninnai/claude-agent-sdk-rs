{
  description = "Claude Agent SDK Rust development environment";

  # Keep the existing dependency and build environment pin.
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/c7def046b9a883d46974757852106483d741586f";
  inputs.fenix = {
    url = "github:nix-community/fenix/9e77b652ca4af1d2e5d9ad4f5a910e48d8c79085";
    inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs =
    { nixpkgs, fenix, ... }:
    let
      systems = [ "x86_64-linux" ];
      forAll = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
    in
    {
      devShells = forAll (pkgs: {
        default = pkgs.mkShell {
          packages = [
            (fenix.packages.${pkgs.stdenv.hostPlatform.system}.fromToolchainFile {
              file = ./rust-toolchain.toml;
              sha256 = "sha256-MFHoWVcPZKjFrLnQnodCXbUsfZcofGfo9tV9fTTvklg=";
            })
            pkgs.rust-analyzer
          ];
          CARGO_REGISTRY_GLOBAL_MIN_PUBLISH_AGE = "14 days";
          CARGO_RESOLVER_INCOMPATIBLE_PUBLISH_AGE = "deny";
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
