#!/usr/bin/env bb

(ns ingestion.duckdb
  "Utilities to work with DuckDB"
  (:require [babashka.process :refer [shell]]
            [clojure.string :as str]
            [ingestion.utils :refer [fname->fpath xs->values]]))


(defn describe!
  [{:keys [bin db-filepath schema table export-format]
    :or {bin "duckdb" schema "main" export-format :json}}]
  (let [output-format (if (= export-format :json) " -json " " ")
        cmd (str bin output-format db-filepath " -c \"DESCRIBE " schema "." table ";\"")
        ret (shell {:out (str "describe-" schema "." table "." (name export-format))} cmd)]
    (println cmd)
    (if (= (:exit ret) 0)
      (println (str "Described " schema "." table))
      (println (str "Failed to describe " schema "." table)))))

(defn batch-insert-cmd [{:keys [db-filepath merge-key table staging-table schema values]
                      :or {merge-key "id" staging-table "staging"}}]
  (let [statements [(str "CREATE OR REPLACE TABLE " staging-table " " schema ";")
                    (str "INSERT INTO " staging-table " VALUES " (xs->values values) ";")
                    (str "INSERT INTO " table " SELECT * FROM " staging-table " WHERE " merge-key " NOT IN (SELECT " merge-key " FROM " table ");")
                    (str "DROP TABLE " staging-table ";")]
        cmd (str/join " " statements)]
    (str "duckdb " db-filepath " -c \"" cmd "\"")))

(defn truncate!
  [{:keys [bin db-filepath schema table]
    :or {bin "duckdb" schema "main"}}]
  (let [cmd (str bin " " db-filepath " -c \"TRUNCATE " schema "." table ";\"")
        ret (shell cmd)]
    (if (= (:exit ret) 0)
      (println (str "Truncated " schema "." table))
      (println (str "Failed to truncate " schema "." table)))))

(defn -main [{:keys [db-filepath schema-name table-name]}]
  (println (str "Exporting the schema of table " schema-name "." table-name " from database file " db-filepath))
  (describe! {:db-filepath db-filepath
              :schema schema-name
              :table table-name
              :export-format :json}))

;; nyc_311_service_requests.duckdb.landing.?
;; rest_api_pokemon.rest_api_data.pokemon
;; (-main {:db-filepath (fname->fpath "rest_api_pokemon.duckdb")
;;         :schema-name "landing"
;;         :table-name "pokemon"})

;; (-main {:db-filepath (fname->fpath "nyc_open_data.duckdb")
;;         :schema-name "landing"
;;         :table-name "service_requests_311"})

(comment
  (def ts-start "2010-01-01T00:00:00.000")
  (def ts-stop "2024-06-02T23:49:54.000")  

  (def db-filepath (fname->fpath "mydb.duckdb"))
  (def schema-name "main")
  (def table-name "maintable")
  
  (def merge-key "unique_key")
  
  (def schema-arr [(str merge-key " UBIGINT PRIMARY KEY")
                   "created_date TIMESTAMP NOT NULL"
                   "agency VARCHAR"])
  (def maintable-schema (str "(" (str/join ", " schema-arr) ")") )

  (shell 
   (str "duckdb " db-filepath " -c \"DROP TABLE IF EXISTS main.maintable;\""))

  (shell 
   (str "duckdb " db-filepath " -c \"CREATE OR REPLACE TABLE main.maintable " maintable-schema ";\""))

  (shell
   (str "duckdb " db-filepath " -c \"CREATE OR REPLACE TABLE main.maintable (id INTEGER PRIMARY KEY, j VARCHAR);\""))

  (describe! {:db-filepath db-filepath :schema "main" :table "maintable" :export-format :txt})
  (describe! {:db-filepath db-filepath :schema "main" :table "maintable"})
  (describe! {:db-filepath db-filepath :schema "main" :table "raw_data"})
  (truncate! {:db-filepath db-filepath :schema "main" :table "maintable"})

  (shell {:out "rows-in-main-maintable.txt"}
         (str "duckdb " db-filepath " -c \"SELECT * FROM main.maintable;\""))

  (shell
   (batch-insert-cmd {:db-filepath db-filepath
                      :schema maintable-schema
                      :table "maintable"
                      :merge-key merge-key
                      :values [[1 ts-start "NYPD"] [2 ts-stop "HPD"]]}))
  )
  