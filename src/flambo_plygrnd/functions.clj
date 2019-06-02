(ns flambo-plygrnd.functions
  (:require [flambo.conf :as conf]
            [flambo.api :as f]
            [flambo.tuple :as ft]))



(defn get-mdu-in-unit-hour
  [ ts-rdd hour ]
  (let [ unit-msec (* 1000 60 60 hour) ]   
    (-> ts-rdd
        (f/map-to-pair (f/fn [x] (ft/tuple (int (/ (._1 x) unit-msec)) (:tradePrice (._2 x)) )))
        (f/reduce-by-key (f/fn [x y] (+ x y))))))




