(ns flambo-plygrnd.utils

  (:require [clj-time.core :as t])
  
  (:require [clj-time.coerce :as c])
  (:require [clj-time.format :as fm]))

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



