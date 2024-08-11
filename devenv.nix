{
  config,
  inputs,
  lib,
  pkgs,
  ...
}: {
  env.DLT_PIPELINE = "nyc_open_data";

  # https://devenv.sh/languages/
  languages.nix.enable = true;

  languages.python = {
    enable = true;
    venv.enable = true;
    # https://iscompatible.readthedocs.io/en/latest/
    # I had to specify dlt instead of dlt[duckdb] because ot this conflict,
    # which doesn't seem a conflict to me...
    # The conflict is caused by:
    #     dbt-duckdb 1.8.2 depends on duckdb>=1.0.0
    #     dlt[duckdb] 0.5.2 depends on duckdb<0.11 and >=0.6.1; extra == "duckdb" or extra == "motherduck"
    venv.requirements = ''
      dbt-core>=1.8.5,<2.0
      dbt-duckdb>=1.8.2,<2.0
      debugpy>=1.8.5
      dlt>=0.5.2
      # dlt-init-openapi causes a dependency conflict
      # dlt-init-openapi
      fire>=0.6,<1.0
      ipython>=8.26.0
      loguru>=0.7
      pytest
      streamlit>=1.37
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

  enterShell = ''
    pipeline-list
    print-versions
  '';

  # https://devenv.sh/tests/
  enterTest = ''
    echo "assert Python version is 3.11.*"
    python --version | ag "3\.11\.[0-9]+$"
    echo "assert dlt version is 0.5.*"
    dlt --version | ag "0\.5\.[0-9]+$"
  '';

  # https://devenv.sh/pre-commit-hooks/
  pre-commit.hooks = {
    # Format Nix
    alejandra.enable = true;
    # Format Python. Black can conflict with other Python tools (e.g. isort)
    # https://black.readthedocs.io/en/stable/guides/using_black_with_other_tools.html
    # black.autoflake = true;
    black.enable = true;
    # Format Markdown.
    # markdownlint = true;
  };

  # https://devenv.sh/scripts/
  scripts = {
    ingest.exec = "python ingestion/run-all-dlt-pipelines.py";
    pipeline-failed.exec = "dlt pipeline $DLT_PIPELINE failed-jobs";
    pipeline-info.exec = "dlt pipeline $DLT_PIPELINE info";
    pipeline-list.exec = "dlt pipeline --list-pipelines";
    pipeline-trace.exec = "dlt pipeline $DLT_PIPELINE trace";
    print-versions.exec = ''
      echo "Versions"
      ${pkgs.babashka}/bin/bb --version
      echo "dbt"
      dbt --version
      dlt --version
      echo "DuckDB $(${pkgs.duckdb}/bin/duckdb --version)"
      python --version
    '';
  };

  # https://devenv.sh/services/
  # services.postgres.enable = true;
}
