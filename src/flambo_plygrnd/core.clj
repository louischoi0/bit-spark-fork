(ns flambo-plygrnd.core
  (:require [flambo.conf :as conf]
            [flambo.api :as f]
            [flambo.tuple :as ft]
            [flambo.sql :as sql]
            [flambo.sql-functions :as sqlf]

            [flambo-plygrnd.functions :as ff]
            [flambo-plygrnd.sqlexecutor :as se]
            [flambo-plygrnd.utils :as u]

            [clj-time.coerce :as c]
            [clj-time.core :as t]

            [clojure.string :as s]))
  
(defn bit-code-to-surfix
  [ code ]
    (cond 
      (= code "BTC") 100
      (= code "XRP") 101
      (= code "EOS") 102
      :else -1))

(defn get-tenth-count
  [ number ]
    (let [ n (atom number) 
           cnt (atom 0) ] 
      (while (> @n 10)
        (do
          (reset! n (/ @n 10)) 
          (reset! cnt (+ @cnt 1))))
      (inc @cnt)))

(defn ts-to-pair-rdd 
  [ sc ts ]
    (->> ts
         (f/parallelize-pairs sc)))

;(if (not (resolve 'spark-context-instance)) (do (def spark-context-instance (se/make-spark-context "local" "app"))) nil)

;(def sc (se/make-spark-context "local" "app"))

;(def sc spark-context-instance)
(def ts-to-pair-rdd-with-sc (fn [x] (ts-to-pair-rdd sc x)))

(defn ** 
  [ a b ]
    (Math/pow a b))

(defn add-code-num-to-timestamp
  [ n-sym tsp ] 
    (->> tsp
        get-tenth-count
        (** 10)
        (* n-sym)
        (+ tsp)))

(defn round-rdd-by-min
  [ rdd minutes ]  
    (let [ ms-unit (* minutes 60 1000) ]
      (-> rdd
          (f/map (f/fn [x] (ft/tuple (int (/ (._1 x) ms-unit)) (._2 x)))))))

(defn map-to-key
  [ rdd ff ]
    (-> rdd
        (f/map-to-pair (f/fn [x] (ft/tuple (ff (._1 x)) (._2 x) ) ))))

(defn map-to-value
  [ rdd ff ]
    (-> rdd
        (f/map-to-pair (f/fn [x] (ft/tuple (._1 x) (ff (._2 x)))))))

(defn rdd-acum-net
  [ rdd ]
    (-> rdd
        (f/reduce-by-key *)
        f/cache))

(defn rdd-agg-count
  [ rdd ]
    (-> rdd
        (f/map-to-pair (f/fn [x] (ft/tuple (._1 x) 1)))
        (f/reduce-by-key +) 
        f/cache))

(defn make-num-tail-even
  [ number sur ]

    (let [ off-tcnt (-> number get-tenth-count (- sur)) 
           sub-mask (** 10 off-tcnt) ]

           (-> (/ number sub-mask) int (* sub-mask))))

(defn net 
  [ x y ] 
    (-> y
      (- x)
      (/ x)
      (+ 1)))

(defn take-rtail
  [ ts ]
    (butlast ts))

(defn take-rhead
  [ ts ]
    (drop 1 ts)) 

(defn conv-to-net
  [ ts ]
    (let [ tom (->> ts take-rhead (map :tradePrice))
           today (->> ts take-rtail (map :tradePrice))

           tsp-ts (map :timestamp ts)
           net-ts (map net today tom) ]

      (map (fn [x y] (ft/tuple x y)) tsp-ts net-ts)))

(defn sort-by-k
  [ ts k ]
    (sort-by k ts))

(defn load-ts-to-rdd-serr
  [ sym agg-min ] 
    ;TODO base tick unit 10 -> 1  
    (-> (se/load-ts sym 10)
        (sort-by-k :timestamp)
        conv-to-net
        ts-to-pair-rdd-with-sc
        (round-rdd-by-min agg-min)
        (map-to-key (f/fn [x] (add-code-num-to-timestamp (bit-code-to-surfix sym) x)))))


(defn retrv-timestamp
  [ tsp min-agg sym ]
    (let [ ms-unit (* min-agg 60 1000) ]
      (-> tsp 
          (make-num-tail-even 3)
          (- tsp)
          (* -1)
          (* ms-unit))))

(net 100 120)

(def rts (-> (se/load-ts "BTC" 10) (sort-by-k :timestamp)))
(def ok (:timestamp (first rts)))

(def ts (load-ts-to-rdd-serr "BTC" 100))
(def tts (rdd-acum-net ts))

(class ts)

(f/take tts 10)

(def ttt (rdd-test-agg ts))

(f/take ttt 10)

(def tspt (-> tts f/first (._1)))

(def k 
(-> tspt
    (make-num-tail-even 3)
    (- tspt)
    (* -1 )
    (* 100 60 1000)))

(def ts-sorted  (-> (se/load-ts "BTC" 10)
                    (sort-by-k :timestamp)))

(def rt (retrv-timestamp tspt 100 "BTC"))

(f/first ts)

(class ok)
(class rt)

(c/from-long ok)
(c/from-long (long rt))

;(class rdd-agg-btc)

;(def mdd-rdd (get-mdu-in-min-unit rdd-agg-btc))
;(f/first mdd-rdd)

;(-> rdd-agg-btc
;    (reduce-by-key (f/fn [x] )

;(se/load-ts "BTC" 1)



