#!/usr/bin/env bb
(ns extraction.socrata
  (:require [babashka.curl :as curl]
            [clojure.java.io :as io]
            [clojure.string :as str]))

(def now (str (java.time.LocalDateTime/now)))
(def today-as-yyyy-mm-dd (first (str/split now #"T")))

(defn download-json!
  [{:keys [created-date-start
           created-date-stop
           destination-dir
           domain
           limit
           offset
           order
           resource-id]
    :or {created-date-stop today-as-yyyy-mm-dd
         destination-dir "assets/data"
         domain "data.cityofnewyork.us"
         limit 10
         offset 0
         order "created_date"
         resource-id "erm2-nwe9"}}]
  (let [filepath (str destination-dir "/" resource-id "_" created-date-start "_" created-date-stop "_limit-" limit "_offset-" offset "_order-" order ".json")
        headers {"Accept" "application/json"}
        where (str "created_date between '" created-date-start "' and '" created-date-stop "'")
        base-url (str "https://" domain "/resource/" resource-id ".json")
        query-params {"$limit" limit
                      "$offset" offset
                      "$order" order
                      "$where" where}
        resp (curl/get base-url {:headers headers
                                 :query-params query-params
                                 :as :bytes})]
    (println (str "fetch " base-url " with query params " query-params))
    (-> resp :body (io/copy (io/file filepath)))
    (println (str "file " filepath " created"))
    ))

(defn -main []
  (download-json! {:resource-id "erm2-nwe9"
                   :created-date-start "2024-07-01"
                   :limit 100}))

(-main)

(comment 
  (println (str "today is " today-as-yyyy-mm-dd))
  (download-json! {:resource-id "erm2-nwe9"
                   :created-date-start "2024-08-01"
                   :created-date-stop "2024-08-09"}))