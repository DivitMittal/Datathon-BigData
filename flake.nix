{
  description = "Datathon flake";

  outputs = {flake-parts, ...} @ inputs:
    flake-parts.lib.mkFlake {inherit inputs;} {
      systems = import inputs.systems;
      imports = [
        inputs.treefmt-nix.flakeModule
        ./flake/nixpkgs.nix
      ];

      perSystem = {pkgs, ...}: {
        treefmt = {
          flakeCheck = false;

          programs = {
            #typos.enable = true;
            ## Nix
            alejandra.enable = true;
            deadnix.enable = true;
            statix.enable = true;
            ## JSON. Top-level prettier only exists in newer nixpkgs (26.05 has
            ## it; this repo's older unstable pin still has nodePackages.prettier)
            prettier = {
              enable = true;
              package = pkgs.prettier or pkgs.nodePackages.prettier;
            };
            ## Python
            black.enable = true;
          };

          projectRootFile = "flake.nix";
        };
      };
    };

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    "nixpkgs-2605".url = "github:NixOS/nixpkgs/nixpkgs-26.05-darwin";
    flake-parts.url = "github:hercules-ci/flake-parts";
    # nix-systems/default dropped x86_64-darwin in c29398b5; pin the last
    # revision that still lists it so lock bumps keep x86_64-darwin outputs.
    systems.url = "github:nix-systems/default/da67096a3b9bf56a91d16901293e51ba5b49a27e";
    treefmt-nix = {
      url = "github:numtide/treefmt-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };
}
