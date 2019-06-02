(ns flambo-plygrnd.repl
  (:require [flambo.conf :as conf]
            [flambo.api :as f]
            [flambo.tuple :as ft]
            [flambo.sql :as sql]
            [clojure.string :as string]
            [flambo.sql-functions :as sqlf]
            [clojure.string :as s]
            [clojure.java.jdbc :as j]))
  
(defn make-spark-context 
  [ app-name ]
    (-> (conf/spark-conf)
        (conf/master "local")
        (conf/app-name app-name)
        (f/spark-context)))

(defn make-db-con 
  [table]
    {:dbtype "mysql"
     :dbname table
     :host "182.208.133.108"
     :port "55556" 
     :user "root"
     :password "1234"})

(defn get-time-series
  [ con tnum ]
    (j/query con [ (str "select * from zeroin_time_series_" (str tnum) ";") ] ))

(def sc (make-spark-context "hello-world"))
(def db (make-db-con "zeroin"))

(def f0 "k55104c21936")
(def f1 "k55105ba0423")

(def ts (get-time-series db 1))

(defn b-fill
  [ts]
    (let [ tts (reverse ts)
           fv (atom (first tts))
           sub (fn [x] (if (nil? x) @fv (do (reset! fv x) x))) ]
      (reverse (map sub tts))))

(defn get-fts
  [ts f]
    (->> ts
        (map (fn [x] (x (keyword f))))
        b-fill))

(defn get-pair-ts-rdd
  [ ts f0 f1 ]
    (let [ t0 (get-fts ts f0)
           t1 (get-fts ts f1) ]
      (map vector t0 t1)))

(defn pct
  [b a]
    (-> a
        (- b)
        (/ b)
        (* 100)
        float))

(defn pct-change
  [rdd]
    (let [ pv (atom nil) ]
    (-> rdd
        (f/map (f/fn [x] (if (nil? @pv) 
                              (do (reset! pv x) 0) 
                              (let [ p (pct @pv x) ] (reset! pv x) p)))))))


