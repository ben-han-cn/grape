(ns grape.resource-relationship
  (:require [clojure.set :as s]
            [grape.exception :as ex]
            [grape.util :as util]
            [grape.resource-tree :as rt])
  (:use [clojure.string :only [join]]))

(defn mk-resource-relationship []
  {})

(defn- reverse-map []
  [{} {}])

(defn- nil-then-create-set [s v]
  (let [s' (if s s #{})]
    (conj s' v)))

(defn- add-entry [rm index k v]
  (let [dst (nth rm index)
        relative (nth rm (- 1 index))]
    [(update-in dst [k] #(nil-then-create-set % v))
     (update-in relative [v] #(nil-then-create-set % k))]))

(defn- get-entry [rm index k]
  (let [m (nth rm index)]
    (k m)))

(defn- get-all-entries [rm index]
  (nth rm index))

(defn declare-relationship [type-rel rel-name reverse-rel-name]
  (let [rl (atom (reverse-map))]
    (when (or (rel-name type-rel)
              (reverse-rel-name type-rel))
      (ex/throw (str "duplicate relationship " 
                     (name rel-name) " or " (name reverse-rel-name)) 
                :resource-relationship))
    (-> type-rel
        (assoc rel-name [0 rl])
        (assoc reverse-rel-name [1 rl]))))

(defn- make-sure-rel-type-exists [type-rel rel-name]
  (when-not (rel-name type-rel)
    (ex/throw (str "unknown relationship " (name rel-name)) :resource-relationship)))

(defn get-relationship [type-rel from rel-name]
  (make-sure-rel-type-exists type-rel rel-name)
  (let [[index reverse-map] (rel-name type-rel)]
    (get-entry @reverse-map index from)))

(defn get-all-relationships [type-rel rel-name]
  (make-sure-rel-type-exists type-rel rel-name)
  (let [[index reverse-map] (rel-name type-rel)]
    (get-all-entries @reverse-map index)))

(defn- check-rel-cycle [type-rel from rel-name to]
  (let [rels-to-to (get-relationship type-rel to rel-name)]
    (when (contains? rels-to-to from)
      (ex/throw (str (name from) " -> " (name to) 
                     " has " (name rel-name) " cycle") 
                :resource-relationship))))

(defn add-relationship! [type-rel tree from rel-name to]
  (make-sure-rel-type-exists type-rel rel-name)
  (dorun (map (partial rt/make-sure-type tree) [from to]))
  (check-rel-cycle type-rel from rel-name to)
  (let [[index reverse-map] (rel-name type-rel)]
    (swap! reverse-map #(add-entry % index from to)))
  type-rel)
