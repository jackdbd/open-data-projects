(ns tasks
  "Tasks that should be invoked by the Babashka task runner."
  (:require [babashka.process :refer [shell]]
            [clojure.java.io :as io]
            [ingestion.duckdb :refer [describe!]]))

(defn greet
  "Greet someone by name."
  [name]
  (shell (str "echo \"Hello " name "!\"")))

(defn export-schema
  [table]
  (let [db-filepath (-> (io/file "assets" "data" "nyc_open_data.duckdb")
                        (.getCanonicalFile))]
    (describe! {:db-filepath db-filepath
                :schema "landing_zone"
                :table table})))

(comment
  (greet "World")
  (export-schema "service_requests_311")
  )
