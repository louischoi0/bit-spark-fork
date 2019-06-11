(ns flambo-plygrnd.actionunit
  (:require [flambo.conf :as conf]
            [flambo.api :as f]
            [flambo.tuple :as ft]
            [flambo.sql :as sql]
            [flambo.sql-functions :as sqlf]
            
            [flambo-plygrnd.sqlexecutor :as se]
            [flambo-plygrnd.functions :as ff]
            [flambo-plygrnd.utils :as u :refer :all]
          
            [flambo-plygrnd.core :as cc]

            [clojure.set :as set]

            [clj-time.coerce :as c]
            [clj-time.core :as t]

            [clojure.string :as str]))

(def base-codes ["BTC" "EOS" "BCH" "ETH" ])

(def data-context
  {:sr nil :ts nil :vs nil :codes nil})

;  (initTs [this from to])
;  (initVs [this from to])

(defprotocol actionUnitInterface
  (initContext [this tick])
)

(extend-protocol actionUnitInterface
  Object)

(defrecord actionUnit [ codes tick ]
  actionUnitInterface)

(defn make-action-unit
  [ codes tick ]
    (-> (->actionUnit codes tick) atom))

(defn is-action-unit?
  [ foo ]
    (-> foo
        class
        (= actionUnit)))

(defn cset! 
  [ *ctx member-name v ]
    (reset! *ctx (assoc @*ctx member-name v)))

(defn cget
  [ *ctx member-name ]
    (member-name @*ctx))

(defn initContext 
  [ *ctx from to ]
    
    (assert (is-action-unit? @*ctx))

    (let [ tick (:tick @*ctx) 
           candles (se/load-ts tick) 
           ts (cc/slice-by-from-to candles from to) ]
     
      (cset! *ctx :ts ts)

      (cset! *ctx :vs (-> ts (cc/extract-to-tuple-rdd-f "candleAccTradeVolume") f/cache))
      (cset! *ctx :ns (-> ts (cc/get-ts-tuple-net-opstamp 1)))))

(defn addOperationRdd
  [ *ctx op-name rdd ]
    (cset! *ctx (keyword op-name) rdd))

(defn netEvent
  [ *ctx agg thres ]
  ; Return signals vector of ( Opstamp, net )
    (-> *ctx 
        (cget :ts)
        (cc/event-net-change agg thres)))

(defn sliceBySignals
 [ *ctx signals target-series ]
  ;target-series must not be fold.

  (let [ rts (cget *ctx target-series) ] 


(def ctx (make-action-unit base-codes 1))

(initContext ctx cc/from-tsp cc/to-tsp)
(-> cc/from-tsp)
(-> c/from-long)

(netEvent ctx 1 1.01)


