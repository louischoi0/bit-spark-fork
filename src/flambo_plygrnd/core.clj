(ns flambo-plygrnd.core
  (:require [flambo.conf :as conf]
            [flambo.api :as f]
            [flambo.tuple :as ft]
            [flambo.sql :as sql]
            [flambo.sql-functions :as sqlf]

            [flambo-plygrnd.functions :as ff]
            [flambo-plygrnd.sqlexecutor :as se]
            [flambo-plygrnd.utils :as u :refer :all]

            [clj-time.coerce :as c]
            [clj-time.core :as t]

            [clojure.string :as s]))

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

(defn codify-opstamp
  [ tsp sym ]
    (-> sym
        bit-code-to-surfix
        (add-code-num-to-timestamp tsp)))

(defn round-timestamp
  [ tsp minutes ]
    (int (/ tsp (* minutes 60 1000))))

(defn round-rdd-by-min
  [ rdd minutes ]  
    (let [ ms-unit (* minutes 60 1000) ]
      (-> rdd
          (f/map (f/fn [x] (ft/tuple (round-timestamp (._1 x) minutes ) (._2 x)))))))

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

(defn extract-to-tuple-rdd
  [ ts ]
    (-> ts
        (f/map-to-pair (f/fn [x] (ft/tuple (:code x) (ft/tuple (:timestamp x) (:tradePrice x)))))))

(defn into-ts-net
  [ ts ]
    (-> ts 
        f/group-by-key
        (f/map-to-pair (f/fn [x] (->> x (._2) (sort-by :timestamp) net-from-sorted-dict (ft/tuple (._1 x)))))))

(defn unpack-ts-net 
  [ ts ]
    (-> ts
        (f/map-to-pair (f/fn [x] (let [ tsp  (-> x ft2 (nmap ft1) ) price (-> x ft2 (nmap ft2)) ] (map (fn [x y] (ft/tuple x y) ) tsp price))))))

(defn agg-ts-net
  [ ts-net-rdd min-agg ]
    (-> ts-net-rdd
        (f/reduce-by-key (f/fn [x] (-> x (nmap (fn [x] (-> (round-timestamp (._1 x) min-agg) (ft/tuple (._2 x))))))))))

(defn parallelize-with-sc
  [ ts ]
    (f/parallelize sc ts))

(defn slice-by-from-to
  [ ts from to ]
  (-> ts
      parallelize-with-sc
      (f/filter (f/fn [x] (and (-> x :timestamp (> from)) (-> x :timestamp (< to)))))))

(defn tuple-arr-to-net-tuple
  [ stamp today tomm  ]
    (ft/tuple stamp (ft/tuple today tomm))) 

(defn unpack-and-to-net-tuple-arr
  [x]
  (let [ stamps (-> x ft2 (nmap ft1) take-rhead)  
         today (-> x ft2 (nmap ft2) take-rtail) 
         tomm (-> x ft2 (nmap ft2) take-rhead) ] 
    (ft/tuple (ft1 x) (map tuple-arr-to-net-tuple stamps today tomm))))

(defn tuple-apply
  [ tuple f ] 
    (apply f [ (ft1 tuple) (ft2 tuple) ] ))

(defn fold-net-tuple
  [ t-vv-stuple ]
  (-> t-vv-stuple
      ft1
      (ft/tuple (tuple-apply (ft2 t-vv-stuple) net))))

(defn into-ts-net-tuple
  [ ts ]
    (-> ts 
        f/group-by-key
        (f/map-to-pair (f/fn [x] (unpack-and-to-net-tuple-arr x)))))

(defn timestamp-to-opstamp
  [ stamp sym agg-min ]
    (->> (-> stamp 
          (round-timestamp agg-min))
         (codify-opstamp sym)))

(timestamp-to-opstamp 1023000000 100 5)

(defn fold-ts-net-tuple
  [ ts ]
    (-> ts
        (f/map-to-pair (f/fn [x] (ft/tuple (ft1 x) (->> x ft2 (map fold-net-tuple)))))))

        ;(f/map-to-pair (f/fn [x] (ft/tuple (ft1 x) (-> x ft2 (nmap (fn [x] (net (ft1 x) (ft2 x)))))

(def x (ft/tuple 1234 {:code "BTC" :tradePrice 1000}))

(def tt (se/load-ts 1))
(def ttt (take 30 tt))

(def from-tsp
  (->> ttt
       (sort-by :timestamp)
       first 
       :timestamp))

(def to-tsp (->> ttt
    (sort-by :timestamp)
    last
    :timestamp))

(def g (-> s f/group-by-key))

(def sts (slice-by-from-to tt from-tsp to-tsp))

(def s (extract-to-tuple-rdd sts))

(f/first sts)
(f/count sts)

(-> sts
(f/map-to-pair (f/fn [x] (ft/tuple (:code x) (:timestamp x))))
f/first)

(-> sts
    extract-to-tuple-rdd 
    f/first)

(-> tt
    (slice-by-from-to from-tsp to-tsp)
    (f/filter (f/fn [x] (-> x :code (= "XRP") not )))
    extract-to-tuple-rdd 
    into-ts-net-f
    fold-ts-net-tuple
    f/first)

(def stt (filter (fn [x] (< (:timestamp x) tsp-dot )) tt))

(println stt)

(def kt (into-ts-net sts))

(first (sort-by :timestamp tt))

(def fff (into-ts-net stt))

(-> fff
    (f/map-to-pair (f/fn [x] (ft/tuple (._1 x) (-> x (._2) (nmap (fn [x] (._2 x))) (nreduce +) ))))
    f/collect)


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
