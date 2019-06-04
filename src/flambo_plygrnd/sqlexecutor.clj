(ns flambo-plygrnd.sqlexecutor
  (:require [flambo.conf :as conf])
  (:require [flambo.api :as f])
  (:require [flambo.tuple :as ft])
  (:require [monger.core :as mg])

  (:require [flambo-plygrnd.utils :as u])

  (:require [clj-time.coerce :as c])

  (:require [monger.collection :as mc])
  (:require [monger.operators :refrer :all]))

(defn make-spark-context
  [ master app-name ]  
    (-> (conf/spark-conf)
        (conf/master master)
        (conf/app-name app-name)
        (f/spark-context)))

(defn make-db-connection
  [ db-name ]
    (-> (mg/connect)
        (mg/get-db db-name)))

(def db-name "bit-core")
(def coll "bitts") 
(def db (make-db-connection db-name ))

(defn load-sym-ts
  [ sym tick ] 
    (mc/find-maps db coll {:code sym :unit tick}))

(defn load-ts
  [ tick ]
    (mc/find-maps db coll {:unit tick}))

(defn load-ts-from-to
  [ tick from to ]
    (mc/find-maps db coll {"$and" [ {:unit tick}  { "$and" [ {:timestamp {"$lte" to} } {:timestamp {"$gte" from}} ] } ] } ))

(load-ts-from-to 1 1000 100000000000)

(defn conv-ts-map-to-tuple
  [ ts ]
    (map (fn [x] (ft/tuple (:timestamp x) (ft/tuple (:tradePrice x) (:code x)))) ts ))

(defn print-recur
  [x]
    (println x)
    x)

(defn load-ts-to-rdd
  [ sc sym tick ]
    (->> (load-ts sym tick)
         conv-ts-map-to-tuple
         (f/parallelize-pairs sc)))

;(load-ts "BTC" 10)
a0204060
