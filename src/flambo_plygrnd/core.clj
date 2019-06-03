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
      (= code "BSV") 103
      (= code "BCH") 104
      (= code "ETH") 105
      (= code "BTT") 106
      (= code "COSM") 107
      (= code "BTG") 108
      (= code "ADA") 109
      (= code "ATOM") 110
      (= code "TRX") 111
      (= code "NPXS") 112
      :else -1))

(defn surfix-to-bit-code 
  [ surfix ]
    (cond  
      (= surfix 100) "BTC"
      (= surfix 101) "XRP"
      (= surfix 102) "EOS"
      (= surfix 103) "BSV"
      (= surfix 104) "BCH"
      (= surfix 105) "ETH"
      (= surfix 106) "BTT"
      (= surfix 107) "COSM"
      (= surfix 108) "BTG"
      (= surfix 109) "ADA"
      (= surfix 110) "ATOM"
      (= surfix 111) "TRX"
      (= surfix 112) "NPXS"
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

(def sc (se/make-spark-context "local" "app"))

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
        (+ tsp)
        long))

(defn round-timestamp
  [ tsp minutes ]
    (int (/ tsp (* minutes 60 1000))))

(defn round-rdd-by-min
  [ rdd minutes ]  
    (let [ ms-unit (* minutes 60 1000) ]
      (-> rdd
          (f/map (f/fn [x] (ft/tuple (round-timestamp (._1 x)) (._2 x)))))))

(defn map-to-key
  [ rdd ff ]
    (-> rdd
        (f/map-to-pair (f/fn [x] (ft/tuple (ff (._1 x)) (._2 x) ) ))))

(defn map-to-value
  [ rdd ff ]
    (-> rdd
        (f/map-to-pair (f/fn [x] (ft/tuple (._1 x) (ff (._2 x)))))))

(defn rdd-cumprod-by-key
  [ rdd ]
    (-> rdd
        (f/reduce-by-key (f/fn [x y] (* x y)))
        f/sort-by-key
        f/cache))

(defn rdd-max-by-value
  [ rdd ]
    (-> rdd
        (f/reduce (f/fn [x,y] (if (< (._2 x) (._2 y)) x y)))))

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
      double
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


(defn load-ts-all-rdd 
  [ ts agg-min ]
    (let [ ts-added-sym   (->> ts
                               (map (fn [x] {:timestamp (:timestamp x) :code (:code x) :tradePrice (:tradePrice x)} ))
                               (map (fn [x] (ft/tuple (:code x) {:tradePrice (:tradePrice x) :timestamp(:code x)  } )))
                               ts-to-pair-rdd-with-sc) ]

    (-> ts-added-sym 
        ts-to-pair-rdd-with-sc
        (map-to-key (f/fn [x] (-> x (/ (* agg-min 60 1000)) int)))
        (f/map-to-pair (f/fn [x] (-> x ._2 :code bit-code-to-surfix (add-code-num-to-timestamp (._1 x)) (ft/tuple (-> x ._2 :tradePrice)))))
        f/sort-by-key)))

(defn nmap
  [ x f ]
    (map f x))

(defn print-recur 
  [ x ]
    (do (print x) x))

(defn net-from-sorted-dict
  [ ts ]
    (let [ prices (map :tradePrice ts) 
           stamps (map :timestamp ts)

           today (take-rtail prices)
           tomm (take-rhead prices) ]
      
      (->> tomm 
           (map net today)
           (map (fn [x y] (ft/tuple x y)) (take-rhead stamps)))))

(defn load-ts-net
  [ ts ]
    (-> ts 
        (nmap (fn [x] (ft/tuple (:code x) {:timestamp (:timestamp x) :tradePrice (:tradePrice x) })))
        ts-to-pair-rdd-with-sc     
        f/group-by-key
        (f/map-to-pair (f/fn [x] (->> x (._2) (sort-by :timestamp) net-from-sorted-dict (ft/tuple (._1 x)))))))

(defn agg-ts-net
  [ ts-net-rdd min-agg ]
    (-> ts-net-rdd
        (f/reduce-by-key (f/fn [x] (-> x (nmap (fn [x] (-> (round-timestamp (._1 x) min-agg) (ft/tuple (._2 x))))))))))


(def x (ft/tuple 1234 {:code "BTC" :tradePrice 1000}))

(def tt (se/load-sym-ts "BTC" 1))

(def ttt (take 30 tt))
(def kt (load-ts-net ttt 10))

(first (sort-by :timestamp tt))

(def fff (load-ts-net (take 100 tt)))
(f/take kt 10)

(def test-set [ {:timestamp 0 :tradePrice 100} {:timestamp 1 :tradePrice 90} {:timestamp 2 :tradePrice 110} {:timestamp 3 :tradePrice 120} {:timestamp 4 :tradePrice 150} ])
(def tk (net-from-sorted-dict test-set))

(agg-ts-net kt 5)

(defn sort-by-k
  [ ts k ]
    (sort-by k ts))

(defn load-ts-to-rdd-serr
  [ sym agg-min ] 
    ;TODO base tick unit 10 -> 1  
    (-> (se/load-sym-ts sym 1)
        (sort-by-k :timestamp)
        conv-to-net
        ts-to-pair-rdd-with-sc
        (round-rdd-by-min agg-min)
        (map-to-key (f/fn [x] (add-code-num-to-timestamp (bit-code-to-surfix sym) x)))))

(defn load-ts-to-rdd-all-serr
  [ agg-min ]
    (-> (se/load-ts 1)
        (sort-by-k :timestamp)
        conv-to-net
        ts-to-pair-rdd-with-sc))

(defn retrv-timestamp
  [ tsp min-agg ]
    (-> tsp 
        (make-num-tail-even 3)
        (- tsp)
        (* -1)
        long
        (* min-agg 60 1000)))

(defn rdd-retrv-timestamp
  [ rdd min-agg ]
    (-> rdd
        (map-to-key (f/fn [x] (retrv-timestamp x min-agg)))))

(net 100 120)

(def rts (-> (se/load-sym-ts "BTC" 1) (sort-by-k :timestamp)))
(def ok (:timestamp (first rts)))

(def ts (load-ts-to-rdd-serr "BTC" 5))
(def tts (rdd-cumprod-by-key ts))

(f/collect tts)

(def k (rdd-max-by-value tts))

(retrv-timestamp (._1 k) 5)

(f/take tts 5)
(f/first tts)

(c/from-long (retrv-timestamp (._1 (f/first tts)) 5))

(def rtrv-rdd (rdd-retrv-timestamp tts 5))

(-> rtrv-rdd 
    (map-to-key (f/fn [x] (c/from-long x)))
    (f/take 10))

(f/take rtrv-rdd 3)

(-> rts first :candleDateTime)

(->   (f/first ts)
      (._1 )
      (retrv-timestamp 5)
      (c/from-long))

(def otk (-> ts
             (f/map (f/fn [x] (retrv-timestamp (._1 x) 5)))))

(def f (->> rts 
       (map :timestamp)
       (map long) 
       (map c/from-long)))
