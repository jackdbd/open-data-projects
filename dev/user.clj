(ns user
  (:require
   [portal.api :as p]))

(comment
  (def portal (p/open {:window-title "Portal UI"}))
  (add-tap #'p/submit)
  
  (def date-start "2010-01-01")
  (tap> date-start)

  (p/clear)
  
  (p/close portal)
  )