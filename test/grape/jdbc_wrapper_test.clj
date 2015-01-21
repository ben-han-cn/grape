(ns grape.jdbc-wrapper-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [clojure.pprint :as p]
            [grape.jdbc-wrapper :as jw]))

(def db-url {:subprotocol "postgresql"
             :subname "zcloud"
             :user "zcloud"
             :password "zcloud"})


(deftest table-create-test
  (testing "create and delete table"
    (jw/init-db db-url)
    (is (not (jw/has-table? :student)))
    (jw/create-table :student {:name :string :age :integer} {:primary-keys [:name]})
    (is (jw/has-table? "student"))
    ; test table name with '$'
    (jw/create-table :www$baidu$com {:name :string} {:primary-keys [:name]})
    (is (jw/has-table? "www$baidu$com"))
    (= (#{"student" "www$baidu$com"} (set (jw/get-all-table-names))))
    (jw/del-table :student)
    (is (not (jw/has-table? :student)))
    (jw/del-table :www$baidu$com)
    (is (not (jw/has-table? "www$baidu$com")))
    (jw/create-table :day_del1 {:id :integer :info :string} {:primary-keys [:id]})
    (jw/create-table :day_del2 {:id :integer :info :string} {:primary-keys [:id]})
    (jw/del-db)
    (is (not (jw/has-table? :day_del1)))
    (is (not (jw/has-table? :day_del2)))))

(defrecord Student [name age teacher-name])
(deftest add-update-delete-record-test
  (testing "add update delete record into table"
    (jw/init-db db-url)
      (let [table (jw/create-table :student {:name :string :age :integer :teacher-name :string} {:primary-keys [:name :age]})]
        (is (nil? (jw/get-row table {:name "ben"})))
        (jw/insert-row table {:name "ben" :age 33 :teacher-name "nana"})
        (is (not (nil? (jw/get-row table {:name "ben"}))))
        (jw/update-row table {:name "ben"} {:age 30})
        (let [record (jw/get-row table {:name "ben"})]
          (is (= 30 (:age record)))
          (is (= "nana" (:teacher-name record))))
        (jw/del-row table {:name "ben"})
        (is (nil? (jw/get-row table {:name "ben"})))
        (jw/insert-row table (->Student "nana" 28 "ben"))
        (is (not (nil? (jw/get-row table {:name "nana"}))))
        (is (= ({:name "nana" :age 28 :teacher-name "ben"} (jw/get-row table {:name "nana"}))))
        (is (thrown? java.sql.SQLException (jw/insert-row table {:name "nana" :age 28 :teacher-name "nana"})))
        (jw/del-table :student)
        (jw/del-db ))))

(deftest id-auto-increase-test
  (testing "id auto increase"
    (jw/init-db db-url)
    (let [table (jw/create-table :day {:id :serial :year :string :month :string} {:primary-keys [:id]})]
      (is (nil? (jw/get-row table {:id 10})))
      (is (= nil (jw/get-max-id table)))
      (jw/insert-row table {:year "1999." :month "10"})
      (is (not (nil? (jw/get-row table {:year "1999." :month "10"}))))
      (jw/insert-row table {:year "1999" :month "11"})
      (jw/insert-row table {:year "1999" :month "12"})
      (is (= '(2) (jw/get-ids table {:month "11"})))
      (is (= nil (jw/get-ids table {:month "100"})))
      (is (= '(1 2 3) (jw/get-ids table)))
      (is (= 3 (jw/get-max-id table)))
      (jw/del-table :day))
    (jw/del-db)))

(deftest transaction-test
  (testing "test transaction"
    (jw/init-db db-url)
    (let [table1 (jw/create-table :day {:id :string :year :string :month :string :day :string} {:primary-keys [:id]})
          table2 (jw/create-table :person {:id :string :name :string :old :integer :sex :string} {:primary-keys [:id]})]

      (try
        (jw/with-transaction 
          (jw/insert-row table1 {:id "1" :year 2000 :month "1" :day "1"})
          (jw/insert-row table2 {:id "2" :name "ttt" :old 12 :sex "female"})
          (jw/insert-row table2 {:id "1" :year 2000}))
        (catch Exception e ()))

      (is (nil? (jw/get-max-id table1)))
      (is (nil? (jw/get-max-id table2)))

      (jw/with-transaction
        (jw/insert-row table1 {:id "1" :year 2001 :month "1" :day "1"})
        (jw/insert-row table2 {:id "1"  :name "ttt" :old 12 :sex "female"})
        (jw/insert-row table1 {:id "2" :year 2002 :month "2" :day "1"}))

      (is (= ["1" "2"] (jw/get-ids table1)))
      (is (= "1" (jw/get-max-id table2)))

      (jw/del-table :day)
      (jw/del-table :person))
    (jw/del-db)))

(deftest array-type-support-test
  (testing "test postgresql array support"
    (jw/init-db db-url)
      (let [table (jw/create-table :student {:name :string 
                                             :age :integer 
                                             :teachers :string-array 
                                             :scores :integer-array} 
                                   {:primary-keys [:name :age]})]
        (is (nil? (jw/get-row table {:name "ben"})))
        (jw/insert-row table {:name "ben" :age 33 :teachers ["nana"] :scores [1 2 3]})
        (is (not (nil? (jw/get-row table {:name "ben"}))))
        (jw/update-row table {:name "ben"} {:age 30})
        (let [record (jw/get-row table {:name "ben"})]
          (is (= 30 (:age record)))
          (is (= [1 2 3] (:scores record)))
          (is (= ["nana"] (:teachers record))))
        (jw/insert-row table {:name "xuru" :age 1 :teachers ["nana"] :scores '(2 3)})
        (let [record (jw/get-row table {:name "xuru"})]
          (is (= [2 3] (:scores record))))
        (jw/del-row table {:name "ben"})
        (is (nil? (jw/get-row table {:name "ben"})))
        (jw/del-table :student)
        (jw/del-db ))))

(deftest ownership-test
  (testing "test postgresql array support"
    (jw/init-db db-url)
    (jw/create-table :teacher {:name :string} {:primary-keys [:name]})
    (jw/create-table :student {:name :string 
                               :age :integer
                               :teacher :string} 
                              {:primary-keys [:name :age]
                               :references {:teacher [:teacher :owned-by :name]}})
    (try
      (jw/insert-row :student {:name "ben" :age 33 :teacher "nana"})
      (catch Exception e ()))
    (is (nil? (jw/get-row :student {:name "ben"})))
    (jw/insert-row :teacher {:name "nana"})
    (jw/insert-row :student {:name "ben" :age 33 :teacher "nana"})
    (is (not (nil? (jw/get-row :student {:name "ben"}))))
    (jw/insert-row :student {:name "xuru" :age 1 :teacher "nana"})
    (is (not (nil? (jw/get-row :student {:age 1}))))
    (jw/del-row :teacher {:name "nana"})
    (is (nil? (jw/get-row :student {:name "ben"})))
    (is (nil? (jw/get-row :student {:name "xuru"})))

    (try
      (jw/insert-row :student {:name "ben" :age 33})
      (catch Exception e ()))
    (is (nil? (jw/get-row :student {:name "ben"})))

    (jw/del-db )))
