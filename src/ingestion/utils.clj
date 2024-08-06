#!/usr/bin/env bb

(ns ingestion.utils
  "Miscellaneous utilities."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]))

(defn fname->fpath [fname]
  (-> (io/file "assets" "data" fname)
      (.getCanonicalFile)))

(defn xs->value
  "Convert a vector of values to a string like(42, 'Hello')"
  [xs]
  (let [f (fn [x] (if (string? x) (str "'" x "'") x))
        arr (map f xs)]
    (str "(" (str/join ", " arr) ")")))

(defn xs->values
  "Convert a collection of vectors into a string like (42, 'Hello'), (84, 'World')"
  [xs]
  (->> xs (map xs->value) (str/join ", ")))

(comment
  (def ts-start "2010-01-01T00:00:00.000")
  (def ts-stop "2024-06-02T23:49:54.000") 
  (def xs [1 "NYPD"])

  (fname->fpath "mydb.duckdb")
  (xs->value xs)
  (xs->values [[1 ts-start "NYPD"]
               [2 ts-stop "HPD"]])
  )