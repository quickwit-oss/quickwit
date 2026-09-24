{
  description = "Quickwit – cloud-native search engine for observability";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.11";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    let
      version = "0.8.0";

      nixosModule = { config, lib, pkgs, ... }:
        let
          cfg = config.services.quickwit;
          settingsFormat = pkgs.formats.yaml { };
          configFile = settingsFormat.generate "quickwit.yaml" cfg.settings;
        in {
          options.services.quickwit = {
            enable = lib.mkEnableOption "Quickwit search engine";

            package = lib.mkOption {
              type = lib.types.package;
              default = self.packages.${pkgs.system}.default;
              description = "Quickwit package to use.";
            };

            dataDir = lib.mkOption {
              type = lib.types.path;
              default = "/var/lib/quickwit";
              description = "Directory for Quickwit data and indexes.";
            };

            listenAddress = lib.mkOption {
              type = lib.types.str;
              default = "127.0.0.1";
              description = "Address the HTTP/gRPC/gossip services bind to.";
            };

            listenPort = lib.mkOption {
              type = lib.types.port;
              default = 7280;
              description = "HTTP REST API port.";
            };

            settings = lib.mkOption {
              type = settingsFormat.type;
              default = { };
              description = ''
                Quickwit YAML configuration attributes.
                See https://quickwit.io/docs/configuration/node-config
              '';
            };

            extraArgs = lib.mkOption {
              type = lib.types.listOf lib.types.str;
              default = [ ];
              description = "Extra CLI arguments passed to `quickwit run`.";
            };

            openFirewall = lib.mkOption {
              type = lib.types.bool;
              default = false;
              description = "Open firewall ports for Quickwit (HTTP 7280, gRPC/gossip 7281).";
            };
          };

          config = lib.mkIf cfg.enable {
            services.quickwit.settings = lib.mkMerge [
              {
                version = "0.8";
                listen_address = cfg.listenAddress;
                rest.listen_port = cfg.listenPort;
                data_dir = cfg.dataDir;
                indexer.enable_otlp_endpoint = true;
                jaeger.enable_endpoint = true;
              }
              cfg.settings
            ];

            users.users.quickwit = {
              isSystemUser = true;
              group = "quickwit";
              home = cfg.dataDir;
              description = "Quickwit service user";
            };
            users.groups.quickwit = { };

            systemd.services.quickwit = {
              description = "Quickwit search engine";
              documentation = [ "https://quickwit.io/docs" ];
              after = [ "network-online.target" ];
              wants = [ "network-online.target" ];
              wantedBy = [ "multi-user.target" ];

              serviceConfig = {
                Type = "simple";
                User = "quickwit";
                Group = "quickwit";
                ExecStart = "${cfg.package}/bin/quickwit run --config ${configFile}"
                  + lib.optionalString (cfg.extraArgs != [ ])
                    (" " + lib.concatStringsSep " " cfg.extraArgs);
                Restart = "on-failure";
                RestartSec = "5s";

                StateDirectory = "quickwit";
                StateDirectoryMode = "0750";
                LogsDirectory = "quickwit";
                LogsDirectoryMode = "0750";
                WorkingDirectory = cfg.dataDir;

                # Hardening
                NoNewPrivileges = true;
                ProtectSystem = "strict";
                ProtectHome = true;
                PrivateTmp = true;
                PrivateDevices = true;
                ProtectKernelTunables = true;
                ProtectControlGroups = true;
                ReadWritePaths = [ cfg.dataDir ];
                LimitNOFILE = 65536;
              };
            };

            networking.firewall = lib.mkIf cfg.openFirewall {
              allowedTCPPorts = [ cfg.listenPort (cfg.listenPort + 1) ];
              allowedUDPPorts = [ (cfg.listenPort + 1) ];
            };
          };
        };
    in
    (flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        arch = if pkgs.stdenv.hostPlatform.isAarch64 then "aarch64" else "x86_64";
        tarball = "quickwit-v${version}-${arch}-unknown-linux-gnu.tar.gz";
        hashes = {
          "x86_64" = "sha256-pABVRMH0uLO+uF0ReSjJTu2nQ4gtY09p7G92VZhy440=";
          "aarch64" = "sha256-39MmaPvFmPdKwGKNSZcycb2JigRd/w7A9hnn/Yb6kgQ=";
        };
      in {
        packages.default = pkgs.stdenv.mkDerivation {
          pname = "quickwit";
          inherit version;
          src = pkgs.fetchurl {
            url = "https://github.com/quickwit-oss/quickwit/releases/download/v${version}/${tarball}";
            hash = hashes.${arch};
          };
          sourceRoot = ".";
          nativeBuildInputs = [ pkgs.autoPatchelfHook ];
          buildInputs = [ pkgs.stdenv.cc.cc.lib ];
          installPhase = ''
            install -Dm755 quickwit-v${version}/quickwit $out/bin/quickwit
          '';
          meta = with pkgs.lib; {
            description = "Cloud-native search engine for observability";
            homepage = "https://quickwit.io";
            license = licenses.asl20;
            platforms = [ "x86_64-linux" "aarch64-linux" ];
            mainProgram = "quickwit";
          };
        };

        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            rustup
            protobuf
            cmake
            pkg-config
            openssl
            nodejs
            yarn
          ];
          shellHook = ''
            echo "Quickwit dev shell — Rust $(rustc --version 2>/dev/null || echo 'not installed')"
          '';
        };
      }
    )) // {
      nixosModules.quickwit = nixosModule;
      nixosModules.default = nixosModule;
    };
}
