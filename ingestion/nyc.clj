#!/usr/bin/env bb
(ns ingestion.nyc
  "Data ingestion from NYC Open Data"
  (:require [babashka.curl :as curl]
            [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [portal.api :as p]))

;; (def csv-file-path (first *command-line-args*))
;; https://github.com/babashka/babashka/discussions/939

(def base-url "https://data.cityofnewyork.us/resource/erm2-nwe9.json")

(defn service-requests
  [{:keys [date-start date-stop limit offset order]
    :or {date-start "2010-01-01" date-stop "2024-08-06" limit 10000 offset 0 order "created_date"}
    :as opts}]
  (println "opts" opts)
  (println "date-start" date-start)
  (println "date-stop" date-stop)
  (println "limit" limit)
  (println "offset" offset)
  (println "order" order)
  (let [where (str "created_date between '" date-start "' and '" date-stop "'")
        headers {"Accept" "application/json"}
        query-params {"$limit" limit
                      "$offset" offset
                      "$order" order
                      "$where" where}
        resp (curl/get base-url {:headers headers
                                 :query-params query-params})]
    (-> resp :body (json/parse-string true))))

(io/copy
 (:body (curl/get "https://github.com/babashka/babashka/raw/master/logo/icon.png"
                  {:as :bytes}))
 (io/file "icon.png"))


(defn download-json!
  [{:keys [date-start date-stop limit offset order filepath]
    :or {date-start "2010-01-01" date-stop "2024-08-06" limit 10000 offset 0 order "created_date"}
    :as opts}]
  (println "opts" opts)
  (let [where (str "created_date between '" date-start "' and '" date-stop "'")
        headers {"Accept" "application/json"}
        query-params {"$limit" limit
                      "$offset" offset
                      "$order" order
                      "$where" where}
        resp (curl/get base-url {:headers headers
                                 :query-params query-params
                                 :as :bytes})]
    (-> resp :body (io/copy (io/file filepath)))))

(defn k->str [k] (name k))

(defn location [m-row]
  (let [lat (get m-row :latitude "")
        lon (get m-row :longitude "")
        loc (str "(" lat "," lon ")")]
    loc))

(defn cell-value [{:keys [m k]}]
  (if (= k :location)
    (location m)
    (get m k "")))

(defn m-row->str-row [{:keys [m-header m-row]}]
  (let [xs (map (fn [k] (cell-value {:m m-row :k k})) (keys m-header))]
    xs))

(defn m-rows->str-rows
  [{:keys [m-header m-rows]}]
  (map (fn [m-row]
         (m-row->str-row {:m-header m-header :m-row m-row})) m-rows))

;; TODO: ensure rows maintain the order of the columns in the header
(defn write-csv! [{:keys [filepath data]}]
  (let [m-header (first data)
        m-rows (rest data)
        str-header (map k->str (keys m-header))
        str-rows (m-rows->str-rows {:m-header m-header :m-rows m-rows})]
    (with-open [writer (io/writer filepath)]
      (csv/write-csv writer (concat [str-header] str-rows))))
  )

(defn -main
  [& _args]
  (println "you invoked the -main function"))

(-main)

(comment
  (def portal (p/open {:window-title "Portal UI"}))
  (add-tap #'p/submit)

  (def date-start "2024-06-01")
  (def date-stop "2024-08-06")

  (download-json! {:limit 3
                   :date-start date-start
                   :date-stop date-stop
                   :filepath "sample-service-requests.json"})
  
  (def xs (service-requests {:limit 3
                             :date-start date-start
                             :date-stop date-stop}))
  (nth xs 0)
  (tap> xs)
  (tap> (nth xs 0))

  (def dates (map :created_date xs))
  (tap> dates)

  (tap> ^{:portal.viewer/default :portal.viewer/hiccup}
   [:div
    [:p "dates"]
    [:ol (map (fn [s] [:li s]) dates)]])


  (def m-header (nth xs 0))
  (tap> m-header)

  (def str-header (map k->str (keys m-header)))
  (tap> str-header)

  (def m-row (nth xs 2))
  (tap> m-row)
  (location m-row)
  (cell-value {:m m-row :k :location})
  (m-row->str-row {:m-header m-header :m-row m-row})
  (m-rows->str-rows {:m-header m-header :m-rows (rest xs)})

  (write-csv! {:filepath "fixme.csv" :data xs})
  )