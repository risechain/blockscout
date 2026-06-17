{
  description = "Blockscout — Elixir/Erlang/Node dev shell";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  inputs.flake-utils.url = "github:numtide/flake-utils";

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };

        # Pin Elixir 1.19 (mix.exs: `elixir: "~> 1.19"`) on top of OTP 27
        # (.tool-versions: `erlang 27.3.4.6`). nixpkgs ships 1.19.5 / 27.3.4.11
        # at the time of writing — both within the version ranges blockscout
        # accepts. Bumping `nixpkgs` may shift them; if the build ever breaks
        # against a newer point release, pin the input to a known-good rev.
        beam = pkgs.beam.packages.erlang_27;
        elixir = beam.elixir_1_19;
      in {
        devShells.default = pkgs.mkShell {
          name = "blockscout";

          # Build inputs are split for clarity:
          #   - beam + node: language runtimes used directly by `mix` and `npm`
          #   - C toolchain + openssl + libs: needed when `mix deps.compile`
          #     builds NIFs from source (rocksdb, fast_yaml, exla, etc.)
          #   - libpng/libjpeg/etc: image NIFs used by nft_media_handler
          #   - postgresql: provides `psql` and libpq headers for local DB +
          #     anything that links against libpq
          #   - inotify-tools: Phoenix live-reload watcher on Linux
          #   - git: `mix deps.get` clones git-sourced deps
          packages = [
            elixir
            beam.erlang
            beam.rebar3
            beam.hex

            # .tool-versions pins Node 20 but nixpkgs has marked it insecure
            # and the source build fails on some platforms (no prebuilt for
            # aarch64-linux). Node 22 (current LTS) is backwards-compatible
            # for blockscout's frontend assets.
            pkgs.nodejs_22

            pkgs.gcc
            pkgs.gnumake
            pkgs.cmake
            pkgs.pkg-config
            pkgs.openssl
            pkgs.zlib

            pkgs.libpng
            pkgs.libjpeg
            pkgs.libwebp
            pkgs.libtiff

            pkgs.postgresql_16

            pkgs.git
          ] ++ pkgs.lib.optionals pkgs.stdenv.isLinux [
            pkgs.inotify-tools
          ];

          # Keep mix/hex/rebar caches inside the project so concurrent shells
          # (and a global ~/.mix being a different OTP version) don't fight.
          # ERL_AFLAGS preserves shell history across `iex -S mix` sessions.
          shellHook = ''
            export MIX_HOME="$PWD/.nix-mix"
            export HEX_HOME="$PWD/.nix-hex"
            export PATH="$MIX_HOME/bin:$HEX_HOME/bin:$PATH"
            export ERL_AFLAGS="-kernel shell_history enabled"

            # The minimal nix shell often has latin1 as the system encoding;
            # Elixir warns and may misbehave on unicode filenames/modules
            # without this. Equivalent to a UTF-8 locale.
            export ELIXIR_ERL_OPTIONS="+fnu"

            # Make pkg-config find the libs we just added (image NIFs).
            export PKG_CONFIG_PATH="${pkgs.lib.makeSearchPath "lib/pkgconfig" [
              pkgs.openssl.dev pkgs.zlib.dev pkgs.libpng.dev pkgs.libjpeg.dev
              pkgs.libwebp pkgs.libtiff.dev
            ]}:$PKG_CONFIG_PATH"

            echo "blockscout dev shell"
            echo "  elixir : $(${elixir}/bin/elixir --version | tail -1)"
            echo "  erlang : $(${beam.erlang}/bin/erl -eval 'erlang:display(erlang:system_info(otp_release)), halt().' -noshell)"
            echo "  node   : $(${pkgs.nodejs_22}/bin/node --version)"
            echo
            echo "First-time setup:"
            echo "  mix local.hex --force"
            echo "  mix local.rebar --force"
            echo "  mix deps.get"
            echo "  mix compile"
          '';
        };
      });
}
