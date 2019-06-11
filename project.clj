(defproject
  flambo-plygrnd "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [yieldbot/flambo "0.8.0"]]

  ;had to be done
  :aot [flambo-plygrnd.core] 
  ;:main flambo-plygrnd.context
  :main flambo-plygrnd.actionunit

  :profiles {:provided
             {:dependencies
              [[org.apache.spark/spark-core_2.11 "2.0.1"]
               [org.apache.spark/spark-sql_2.11 "2.0.1"]
               [org.apache.spark/spark-hive_2.11 "2.0.1"]
               [org.clojure/java.jdbc "0.7.9"]
               [clj-time "0.15.0"]
               [clj-http "3.10.0"]
               [org.clojure/data.json "0.2.6"]
               [com.novemberain/monger "3.1.0"]
               [mysql/mysql-connector-java "5.1.6"]]}}
  )


;  :injections [ (require '[flambo.conf :as conf])
;                (require '[flambo.api :as f]) ]

