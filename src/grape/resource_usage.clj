(ns grape.resource-usage
  (:require [grape.util :as util]
            [grape.jdbc-wrapper :as jw]
            [grape.exception :as ex]
            [grape.resource-descriptor :as rd]
            [clojure.string :as string]))

(defn- create-table-for-one-usage [user resource]
  (apply jw/create-table (rd/create-table-sql-for-usage user resource)))

(defn create-tables-for-usage []
  (doseq [[user resources] (rd/all-usages)]
    (dorun (map #(create-table-for-one-usage user %) resources))))

(defn- usage->row [user resource user-id resource-id]
  (letfn [(add-id [m t id]
            (if id
              (merge m {t (rd/field->column (rd/type-of-field t :id) id)})
              m))]
    (-> {} 
      (add-id user user-id)
      (add-id resource resource-id))))

(defn- get-table-for-usage[user resource]
  (if-let [table (rd/usage->table-name user resource)]
    (if (jw/has-table? table)
      table
      (ex/throw (str "unknown usage between " user " " resource) :resource-usage))))

(defn add-usage [user resource user-id resource-id]
   (let [table (get-table-for-usage user resource)]
    (jw/insert-row table (usage->row user resource user-id resource-id))))


(defn add-usages [user resource user-id resource-ids]
  (dorun (map #(add-usage user resource user-id %) resource-ids)))

(defn- del-usage [user resource user-id resource-id]
  (let [table (get-table-for-usage user resource)]
    (jw/del-row table (usage->row user resource user-id resource-id))))

(defn update-usages [user resource user-id resource-ids]
  (del-usage user resource user-id nil)
  (add-usages user resource user-id resource-ids))

(defn get-usages [user resource user-id]
  (let [table (get-table-for-usage user resource)
        row (usage->row user resource user-id nil)
        rows (jw/get-rows table row)]
    (mapv resource rows)))


(defn get-usages-of-user[user user-id]
  (reduce (fn [usages resource]
            (merge usages {(util/keyword-append resource "s")
                           (get-usages user resource user-id)}))
          {} 
          (rd/uses user)))
