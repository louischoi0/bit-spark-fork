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

            [clojure.string :as str]))
  
(defn ts-to-pair-rdd 
  [ sc ts ]
    (->> ts
         (f/parallelize-pairs sc)))

;(if (not (resolve 'spark-context-instance)) (do (def spark-context-instance (se/make-spark-context "local" "app"))) nil)

(def sc (se/make-spark-context "local" "app"))

;(def sc spark-context-instance)

(defn ts-to-pair-rdd-with-sc 
  [x] 
    (ts-to-pair-rdd sc x))

(defn parallelize-with-sc
  [ ts ]
    (f/parallelize sc ts))

(defn rdd-retrv-timestamp
  [ rdd min-agg ]
    (-> rdd
        (map-to-key (f/fn [x] (retrv-timestamp x min-agg)))))


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
  [ rdd from to]
    (-> rdd
        (f/filter (f/fn [x] (-> x ft1 (>= from) (and (< (ft1 x) to)))))))

(defn investigate-series-dict-by-code
  [ series-dict opstamp fnc ]
    (let [ code (opstamp-to-code opstamp) 
           series-rdd (series-dict (keyword code)) ]
      (-> series-rdd
          (f/map-to-pair fnc)
          f/sort-by-key)))

(defn slice-ontuple-series-by-opstamp-offset
  [ series base-opstamp op-offset op]
  
  (assert (or (= op +) (= op -)))

    (let [  op1 (atom <=)
            op2 (atom >=) 

            swap-func (fn [x] (= x <=) >= <=)
            op-opstamp (op base-opstamp op-offset) ]

      (if (= op +) (do (swap! op1 swap-func) (swap! op2 swap-func)) nil)
      (-> series
          (nfilter (fn [ ontuple ] (-> ontuple ft1 (@op1 base-opstamp) (and (@op2 (ft1 ontuple) op-opstamp))))))))

(defn slice-ontuple-series-by-opstamp-backward
  [ series to-opstamp op-offset ]
    (slice-ontuple-series-by-opstamp-offset series to-opstamp op-offset -))

(defn slice-ontuple-series-by-opstamp-forward
  [ series to-opstamp op-offset ]
    (slice-ontuple-series-by-opstamp-offset series to-opstamp op-offset +))

(defn slice-ontuple-series-by-op-offset
  [ op-ontuple-series op-offset ]
    (let [ opstamp (ft1 op-ontuple-series) ]
      (-> op-ontuple-series
          ft2
          (op2-reverse list apply)
          ts-to-pair-rdd-with-sc

          (f/filter (f/fn [x] (let [ to opstamp from (subtract-opstamp opstamp op-offset) ]
                                (do (println opstamp) (println from) (println to))
                                (and (>= (ft1 x) from) (<= (ft1 x) to))))))))

(defn tuple-series-to-series-tuple
  [ tuple-series ]
    (ft/tuple (map ft1 tuple-series) (map ft2 tuple-series)))

(defn exclude-code
  [ ts code ]
    (-> ts
        (f/filter (f/fn [x] (-> x :code (= code) not)))))

(defn ft21-to-opstamp
  [ rdd ]
    (-> rdd 
        (f/map-to-pair (f/fn [x] (let [ opstamp (-> x ft2 ft1 (timestamp-to-opstamp (ft1 x) 10)) ]
                         (ft/tuple opstamp (ft22 x)))))))

;(def x (ft/tuple 1234 {:code "BTC" :tradePrice 1000}))
;
(def tt (se/load-ts 1))
(def ttt (take 1000 tt))
;
(def from-tsp
  (->> ttt
       (sort-by :timestamp)
       first 
       :timestamp))

(def to-tsp (->> ttt
    (sort-by :timestamp)
    last
    :timestamp))
;

(defn rdd-min
  ;TODO reduce -> (f/min comparator)
  ;Now i don`t know how.
  ([ rdd ]
    (-> rdd
        (f/reduce (f/fn [b a] (if (< a b) a b)))))

  ([ rdd target-f ] 
    (-> rdd
        (f/reduce (f/fn [b a] (if (< (target-f a) (target-f b)) a b))))))

(defn do-escalate-series
  [ series op-offset f ]
    (-> series
        f/first))

(defn fold-by-opstamp
  [ rdd f ]
    (-> rdd
        (f/reduce-by-key (f/fn [b a] (f b a)))))

(defn fold-everage
  [ rdd ]
  ; (opstamp , V) series rdd everage action.
    (-> rdd
        f/group-by-key
        (f/map-to-pair (f/fn [x] (ft/tuple (ft1 x) (/ (reduce + (ft2 x)) (count (ft2 x))))))))

(defn slice-ontuple-series
  [ opstamp ontuple-series-rdd op-unit ]
    ;Ontuple-series has array of ( opstamp , value ) tuple rdd.
    (-> ontuple-series-rdd 
        (f/filter (f/fn [x] (opstamp-is-in-range-b? (ft1 x) opstamp op-unit)))))

(defn get-range-series-by-signals-with-op-offset
  ; csv must not be folded.
  [ signals cvs op-offset ]
    (let [ c (-> cvs f/collect) ]
      (-> signals 
          (f/map-to-pair 
            (f/fn 
              [v] 
               (->> c
                    (filter (fn [x] (opstamp-is-in-range-b? (ft1 x) v op-offset)))
                    (ft/tuple v)))))))

(def ts (-> tt (slice-by-from-to from-tsp to-tsp)))
(def vs (-> ts (exclude-code "XRP") (extract-to-tuple-rdd-f "candleAccTradeVolume") f/cache))
(def cvs (-> vs ft21-to-opstamp))

(-> signals 
    (get-range-series-by-signals-with-op-offset cvs 3)
    f/first
    ft2
    count)

(-> cvs
    f/first)

;(slice-ontuple-series (first signals) cvs 10)

(-> vs
    f/first)

(-> signals
    f/first 
    (slice-ontuple-series cvs 1)
    f/collect
    count)

    ;f/first) 

    ;(rdd-min vs (f/fn [x] (-> x ft2 ft1)))

(-> vs
    (f/map-to-pair (f/fn [x] (let [ opstamp (-> x ft2 ft1 (timestamp-to-opstamp (ft1 x) 10)) ]
                      (ft/tuple (ft1 x ) (ft/tuple opstamp (ft2 x))))))

    (rdd-min (f/fn [x] (-> x ft2 ft1))))

(-> vs
    (f/map-to-pair (f/fn [x] (let [ opstamp (-> x ft2 ft1 (timestamp-to-opstamp (ft1 x) 10)) ]
                      (ft/tuple (ft1 x ) (ft/tuple opstamp (ft2 x))))))

    (f/map-to-pair (f/fn [x] (ft2 x)))
    f/sort-by-key
    f/collect
    (op2-reverse 10 take))


;
(def signals (-> ts
    (f/filter (f/fn [x] (-> x :code (= "XRP") not )))
    (event-net-change 10 1.02)
    (f/map (f/fn [x] (ft1 x)))))
;
;(def tsft (-> signals f/first))
;
;(-> tts f/first ft2 f/first)
;(-> tts f/first ft1)
;       
;
;(-> tts (slice-ontuple-series-by-op-offset 4))





