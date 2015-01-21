(ns grape.resource-store
  (:require [grape.util :as util]
            [grape.jdbc-wrapper :as jw]
            [grape.exception :as ex]
            [grape.resource-usage :as usage]
            [grape.resource-descriptor :as rd]
            [clojure.string :as string]))

(defn- check-table-exists [table]
  (when-not (jw/has-table? table)
    (ex/throw "resource of type doesn't exists")))

(defn- create-table-for-resource [t]
  (let [[table cols params] (rd/create-table-params-for-type t)]
    (when-not (jw/has-table? table)
      (if-let [owners (keys (:references params))]
        (dorun (map create-table-for-resource owners)))
      (jw/create-table table cols params))))

(defn- create-tables-for-resources []
  (dorun (map create-table-for-resource (rd/resource-types))))

(defn init-resource-store [descriptor]
  (def resource-descriptor descriptor)
  (binding [rd/*descriptor* resource-descriptor]
    (create-tables-for-resources)
    (usage/create-tables-for-usage)))
   
(defn- get-resources-in-table [table t attrs]
  (if (empty? attrs)
    (jw/get-all-rows table)
    (jw/get-rows table (rd/resource->row t attrs))))

(defn get-resources
  ([t] ;get top level resources
   (get-resources t nil))
  ([t attrs];get resources under parent resource 
   (binding [rd/*descriptor* resource-descriptor]
     (let [table (rd/type->table-name t)]
       (when (jw/has-table? table)
         (when-let [rows (get-resources-in-table table t attrs)]
           (let [resources (map #(rd/row->resource t %) rows)]
             (doall (map #(merge % (usage/get-usages-of-user t (:id %))) 
                         resources)))))))))

(defn- add-usages [user user-id usages]
  (dorun (map (fn[[resource resource-ids]]
                (let [resource' (util/keyword-trip resource "s")]
                    (usage/update-usages user resource' user-id resource-ids)))
              usages)))

(defn add-resource [t attrs]
  (binding [rd/*descriptor* resource-descriptor]
    (let [table (rd/type->table-name t)]
      (check-table-exists table)
      (let [[resource usages] (util/split-map-with-keys attrs (rd/fields t))
            row (->> resource (rd/resource->row t) (rd/generate-id t))]
        (jw/insert-row table row)
        (add-usages t (:id row) usages)
        (assoc attrs :id (:id row))))))

; Note: the usage of the sub resrouce isn't considered which 
; actually should be deleted
(defn delete-resource [t attrs]
  (when (empty? attrs)
    (ex/throw "attrs can not be empty which to be deleted!" :resource-store))
  (let [table (rd/type->table-name t)]
    (check-table-exists table)
    (binding [rd/*descriptor* resource-descriptor]
      (let [rows-to-delete (get-resources-in-table table t attrs)]
        (if-not rows-to-delete
          (ex/throw "no rows to delete!" :resource-store) 
          (if (= (count rows-to-delete) 1)
            (let [resource-to-delete (rd/row->resource t (first rows-to-delete))]
              (jw/del-row table (rd/resource->row t attrs))
              resource-to-delete)
            (ex/throw "count of resources to be deleted is more than one!" :resource-store)))))))

(defn- update-usages [user user-id usages]
  (dorun (map (fn[[resource resource-ids]]
                (let [resource' (util/keyword-trip resource "s")]
                    (usage/update-usages user resource' user-id resource-ids)))
              usages)))

(defn update-resource
  ([t old-attrs new-attrs]
   (when (or (empty? new-attrs) (empty? old-attrs))
     (ex/throw "attrs can not be empty which to be updated!" :resource-store))
   (let [table (rd/type->table-name t)]
     (check-table-exists table)
     (binding [rd/*descriptor* resource-descriptor]
       (let [[attrs usages] (util/split-map-with-keys new-attrs (rd/fields t))
             rows-to-update (get-resources-in-table table t old-attrs)]
         (if-not rows-to-update
           [nil nil]
           (if (= (count rows-to-update) 1)
             (let [resource-to-update (rd/row->resource t (first rows-to-update))]
               (when-not (empty? attrs)
                 (jw/update-row table (rd/resource->row t old-attrs) (rd/resource->row t attrs)))
               (when-not (empty? usages)
                 (update-usages t (:id resource-to-update) usages))
               [resource-to-update (-> (merge resource-to-update attrs)
                                       (merge (usage/get-usages-of-user t (:id resource-to-update))))])
             (ex/throw "count of resources to be updated is more than one!" :resource-store))))))))

(defmacro with-transaction [& body]
  `(jw/with-transaction ~@body))
