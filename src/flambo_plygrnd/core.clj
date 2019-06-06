(ns flambo-plygrnd.core
  (:require [flambo.conf :as conf]
            [flambo.api :as f]
            [flambo.tuple :as ft]
            [flambo.sql :as sql]
            [flambo.sql-functions :as sqlf]

            [flambo-plygrnd.functions :as ff]
            [flambo-plygrnd.sqlexecutor :as se]
            [flambo-plygrnd.utils :as u :refer :all]

            [clojure.set :as set]

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

(defn rdd-retrv-timestamp
  [ rdd min-agg ]
    (-> rdd
        (map-to-key (f/fn [x] (retrv-timestamp x min-agg)))))

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

(defn extract-to-tuple-rdd-f
  [ ts keyword-to-ex ]
    (-> ts
        (f/map-to-pair (f/fn [x] (ft/tuple (:code x) (ft/tuple (:timestamp x) (x (keyword keyword-to-ex))))))))

(defn extract-to-tuple-rdd
  [ ts ]
    (-> ts
        (extract-to-tuple-rdd-f "tradePrice")))

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

(defn unpack-to-offset-tuple 
  [x offset]
    (let [ stamps (-> x ft2 (nmap ft1)) ] 1))

(defn tuple-apply
  [ tuple f ] 
    (f (ft1 tuple) (ft2 tuple)))

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
    (-> stamp 
        (round-timestamp agg-min)
        (codify-opstamp sym)))

(defn to-opstamp-factory
  [ sym agg-min ]
    (fn [x] (timestamp-to-opstamp x sym agg-min)))

(defn fold-ts-net-tuple
  [ ts ]
    (-> ts
        (f/map-to-pair (f/fn [x] (ft/tuple (ft1 x) (->> x ft2 (map fold-net-tuple)))))))
        ;(f/map-to-pair (f/fn [x] (ft/tuple (ft1 x) (-> x ft2 (nmap (fn [x] (net (ft1 x) (ft2 x)))))

(defn nmap2
  [ x y f ]
    (map f x y))

(defn unpack-ts-net-tuple-and-agg
  [ ts agg-min ]
    (-> ts 
        (f/map-to-pair (f/fn [x] (let [ code (-> x ft1)
                                        timestamps (-> x ft2 (nmap ft1))
                                        nets (-> x ft2 (nmap ft2)) 
                                        opstamps (-> timestamps (nmap (to-opstamp-factory code agg-min))) ]

                                      (ft/tuple code (nmap2 opstamps nets ft/tuple)))))
        f/collect
        (nmap ft2)
        (nreduce concat)
        ts-to-pair-rdd-with-sc))

(defn max-by-key
  [ ts ]
    (-> ts
        (f/reduce-by-key (f/fn [b,a] (if (> (-> a ft2 ) (ft2 b)) a b)))))

(defn remove-element
  [ seqq element ]
    (->> seqq (filter (fn [x] (not (= x element))))))

(defn op2-reverse
  [ f x y ]
    (f y x))

(defn rdd-ft1-to-opstamp
 [ rdd ]
  (-> rdd
      (f/map-to-pair (f/fn [x] (-> x ft1 timestamp-to-opstamp (ft/tuple (ft2 x)))))))

(defn get-ts-tuple-net-opstamp
  [ ts min-agg ]
    (-> ts
      extract-to-tuple-rdd
      into-ts-net-tuple
      fold-ts-net-tuple
      (unpack-ts-net-tuple-and-agg min-agg)))

(defn net-comprod
  [ ts ]
    (-> ts
        (f/reduce-by-key (f/fn [a b] (* a b)))))

(defn let-me-see
  [ & vars ]
    (map println vars))

(defn event-net-change 
  [ ts min-agg thres ]
    (-> ts 
        (get-ts-tuple-net-opstamp min-agg)
        (f/filter (f/fn [x] (> (ft2 x) thres)))))

(defn volumes-offsets
  [ ats opstams min-offset ] 
    (-> ats
        (extract-to-tuple-rdd-f "candleAccTradeVolume")))

(defn key-series-to-rdd-map
  [ key-series-tuples ]
    (-> key-series-tuples
        (nmap (fn [x] { (keyword (ft1 x)) (-> x ft2 ts-to-pair-rdd-with-sc) } ))
        (nreduce (fn [x y] (assoc x (-> y first key) (-> y first val))))))

(defn series-rdd-in-time-range-and-sort
  [ rdd to from ]
    (-> rdd
        (f/filter (f/fn [x] (-> x ft1 (>= from) )))))
        ;(f/filter (f/fn [x] (-> x ft1 (>= from) (and (< (ft1 x) to)))))))

(defn investigate-series-dict-by-code
  [ series-dict opstamp fnc ]
    (let [ code (opstamp-to-code opstamp) 
           series-rdd (series-dict (keyword code)) ]
      (-> series-rdd
          (f/map-to-pair fnc)
          f/sort-by-key)))

(defn offset-slicer-by-opstamp
  [ ts opstamps min-offset ]
    (let [ codes (-> opstamps (nmap opstamp-to-code) set)
           ts-grouped-rdd  (-> ts f/group-by-key) 

           series-dict-by-code (->  ts 
                                    (f/filter (f/fn [x] (contains? codes (ft1 x))))
                                    f/group-by-key
                                    f/collect
                                    key-series-to-rdd-map) ]))

(defn exclude-code
  [ ts code ]
    (-> ts
        (f/filter (f/fn [x] (-> x :code (= code) not)))))

(def x (ft/tuple 1234 {:code "BTC" :tradePrice 1000}))

(def tt (se/load-ts 1))
(def ttt (take 1000 tt))

(def from-tsp
  (->> ttt
       (sort-by :timestamp)
       first 
       :timestamp))

(def to-tsp (->> ttt
    (sort-by :timestamp)
    last
    :timestamp))

(def ts (-> tt (slice-by-from-to from-tsp to-tsp)))
(def vs (-> ts (exclude-code "XRP") (extract-to-tuple-rdd-f "candleAccTradeVolume")))

(def test-input [ (ft/tuple "BTC" [(ft/tuple 1 2) (ft/tuple 3 4)]) (ft/tuple "EOS" [(ft/tuple 5 2)]) (ft/tuple "XRP" [(ft/tuple 8 1)]) ])
(def test-series-dict (-> (key-series-to-rdd-map test-input)))
(def test-opstamp 100234)

(def k (-> test-series-dict (investigate-series-dict-by-code test-opstamp (f/fn [x] (ft/tuple (+ (ft1 x) test-opstamp ) 1 )))))
(-> k f/collect)

(-> k (series-rdd-in-time-range-and-sort 0 100238) f/collect)

(print f/first)

(f/first k)

(-> (offset-slicer-by-opstamp vs signals 0) first ft2 first)

(nmap signals opstmap-to-code)

(println opstamp-to-code)

(f/count ts)

(def ss (-> ts 
    (f/filter (f/fn [x] (-> x :code (= "XRP") not )))
    (extract-to-tuple-rdd-f "candleAccTradeVolume")
    into-ts-net-tuple
    fold-ts-net-tuple
    (unpack-ts-net-tuple-and-agg 10)))

(-> ss
    f/collect)

(def signals (-> ts
    (f/filter (f/fn [x] (-> x :code (= "XRP") not )))
    (event-net-change 10 1.02)
    (f/map (f/fn [x] (-> x ft1)))
    f/collect))

(first signals)

(-> ss
    (f/reduce-by-key (f/fn [b a] (* b a)))
    f/first)

(->> (-> ss 
    f/count-by-key) (sort-by key))


