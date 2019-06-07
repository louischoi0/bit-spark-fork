(ns flambo-plygrnd.utils
  (:require [flambo.api :as f])
  (:require [flambo.tuple :as ft])
  (:require [clj-time.core :as t])
  (:require [clojure.string :as str])
  (:require [clj-time.coerce :as c])
  (:require [clj-time.format :as fm]))

(defn op2-reverse
  [ x y f ]
    (f y x))

(defn map-to-key
  [ rdd ff ]
    (-> rdd
        (f/map-to-pair (f/fn [x] (ft/tuple (ff (._1 x)) (._2 x) ) ))))

(defn map-to-value
  [ rdd ff ]
    (-> rdd
        (f/map-to-pair (f/fn [x] (ft/tuple (._1 x) (ff (._2 x)))))))

(defn ntuple
  [ ft2 ft1 ]
    (ft/tuple ft1 ft2))

(defn ** 
  [ a b ]
    (Math/pow a b))

(defn sort-by-k
  [ ts k ]
    (sort-by k ts))

(defn nfilter
  [ ts f ]
    (filter f ts))

(defn nreduce
  [ x f ] 
    (reduce f x))

(defn ft2
  [ x ]
    (._2 x))

(defn ft1
  [ x ]
    (._1 x))

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

(def unit-dict {:minutes 60 :hours (* 60 60) :day (* 60 60 24) })

(def req-dt-fmt (fm/formatter "YYYY-MM-dd HH:mm:ss"))

(defn strftime [x] (fm/unparse req-dt-fmt x))
(defn timefstr [x] (if (nil? x) nil (fm/parse req-dt-fmt x)))

(defn get-target-time
  [ target-time unit tick cnt op]
    (-> target-time
        (c/to-long)
        (op (* tick cnt (* 1000 (unit-dict (keyword unit)))))
        (c/from-long)))

(defn get-end-time
  [start-time unit tick cnt]
    (get-target-time start-time unit tick cnt +))

(defn get-start-time
  [end-time unit tick cnt]
    (get-target-time end-time unit tick cnt -))

(defn get-cnt-from-times
  [start-time end-time unit tick]
    (-> (c/to-long end-time)
        (- (c/to-long start-time))
        (/ 1000 (* (unit-dict (keyword unit)) tick))))

(defn make-num-tail-even
  [ number sur ]

    (let [ off-tcnt (-> number get-tenth-count (- sur)) 
           sub-mask (** 10 off-tcnt) ]

           (-> (/ number sub-mask) int (* sub-mask))))

(defn add-code-num-to-timestamp
  [ tsp n-sym ] 
    (->> tsp
        get-tenth-count
        (** 10)
        (* n-sym)
        (+ tsp)
        long))

(defn round-timestamp
  [ tsp minutes ]
    (int (/ tsp (* minutes 60 1000))))

(defn codify-opstamp
  [ tsp sym ]
    (-> sym
        bit-code-to-surfix
        (op2-reverse tsp add-code-num-to-timestamp)))

(defn timestamp-to-opstamp
  [ stamp sym agg-min ]
    (-> stamp 
        (round-timestamp agg-min)
        (codify-opstamp sym)))

(defn retrv-timestamp
  [ tsp min-agg ]
    (-> tsp 
        (make-num-tail-even 3)
        (- tsp)
        (* -1)
        long
        (* min-agg 60 1000)))

(defn timestamp-is-in-time-range
  [ tsp start end ]
    (let [ _start (c/to-long start) _end (c/to-long end) ]
    (and (>= tsp _start) (<= tsp _end))))

(defn retrv-timestamp-wcode
  [ tsp min-agg ]
    (let [ sqcnt (- (get-tenth-count tsp) 3)
           code (-> tsp (/ (** 10 sqcnt)) int surfix-to-bit-code) ]
      (-> tsp
          (retrv-timestamp min-agg)
          (ntuple code))))

(defn opstamp-to-ncode
  [ tsp ] 
    (let [ sqcnt (-> tsp get-tenth-count (- 3)) ]
      (-> tsp (/ (** 10 sqcnt)) int)))

(defn opstamp-to-code
  [ tsp ] 
    (-> tsp
        opstamp-to-ncode
        surfix-to-bit-code))  

(defn opstamp-to-code-pair
  [ opstamp ]
    (-> opstamp
        opstamp-to-code
        (ft/tuple opstamp)))

(defn get-rid-of-code-to-opstamp
  [ opstamp ]
    (->>  opstamp 
          get-tenth-count
          (- 1)
          (* -1)
          (** 10) 
          (mod opstamp)
          int))
      
(defn operation-opstamp
  [ opstamp msc op ]
    (let [ ncode (opstamp-to-ncode opstamp) ]
      (-> opstamp
          get-rid-of-code-to-opstamp
          (op msc)
          (add-code-num-to-timestamp ncode))))

(defn subtract-opstamp
  [ opstamp msc ]
    (operation-opstamp opstamp msc -))

(defn add-opstamp
  [ opstamp msc ]
    (operation-opstamp opstamp msc +))

(defn subtract-opstamp-by-min-unit
  [ opstamp minutes min-agg ]
    (let [ _msc (-> minutes (/ min-agg) int) ]
      (subtract-opstamp opstamp _msc)))

(defn warning
  [ form v ]
    (let [ replace-token "{}" ]
      (-> form 
          (str {})
          (str/replace #"\{}" v)
          println)))

;TODO
;(defn format-py
;  [ s &v ]
;    ( 

(defn warn-function-mal-use
  [ condition function_name info ]
    (if condition 
      (do
        (let [ form (str/replace "Warning : {} Function mal-use detected. \n" #"\{}" function_name) ]
          (-> form 
              (warning info))))))

