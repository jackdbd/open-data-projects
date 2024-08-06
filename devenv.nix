{
  config,
  inputs,
  lib,
  pkgs,
  ...
}: {
  # https://devenv.sh/basics/
  # https://devenv.sh/reference/options/

  env.GREET = "devenv";

  # https://devenv.sh/languages/
  languages.nix.enable = true;
  languages.python = {
    enable = true;
    venv.enable = true;
    venv.requirements = ''
      dbt-core~=1.8.4
      dbt-duckdb~=1.8.2
      debugpy
      loguru==0.7.*
      pytest
    '';
    libraries = [];
  };

  # https://devenv.sh/packages/
  packages = [
    pkgs.babashka # Clojure interpreter for scripting
    pkgs.duckdb
    pkgs.git
    pkgs.visidata # interactive terminal multitool for tabular data
  ];

  # https://devenv.sh/scripts/
  scripts.hello.exec = "echo hello from $GREET";

  enterShell = ''
    say-hello
  '';

  # https://devenv.sh/tests/
  enterTest = ''
    echo "assert Python version is 3.11.8"
    python --version | grep "3.11.8"
  '';

  # https://devenv.sh/pre-commit-hooks/
  # pre-commit.hooks.shellcheck.enable = true;

  # https://devenv.sh/processes/
  # processes.ping.exec = "ping example.com";

  # https://devenv.sh/scripts/
  scripts = {
    say-hello.exec = "echo \"Hello from $GREET\"";
  };

  # https://devenv.sh/services/
  # services.postgres.enable = true;
}
