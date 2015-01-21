(ns grape.resource-tree
  (:require [clojure.zip :as zip]
            [grape.exception :as ex]))

(defn mk-resource-tree [types]
  (let [branch? (fn [node] true)
        children (fn [node] 
                   (if (< 1 (count node))
                     (subvec node 1)
                     nil))
        make-node (fn [node children]
                    (into [(first node)] children))]
    (zip/zipper branch? children make-node types)))

(defn locations [tree]
  (take-while (complement zip/end?) (iterate zip/next tree)))

(defn find-locations [tree f]
  (filter #(-> % zip/node f) (locations tree)))

(defn location->type [loc]
  (first (zip/node loc)))

;when add type, we will make sure type won't duplicate
(defn location-with-type [tree type]
  (let [locs (find-locations tree #(= type (first %)))] 
    (if (empty? locs)
      nil
      (first locs))))

(defn type-ancesters [tree type]
  (if-let [loc (location-with-type tree type)]
    (rest (map first (zip/path loc)))))

(defn type-chain [tree type]
  (concat (type-ancesters tree type) (list type)))

(defn add-sub-type [tree type sub-type]
  (if (location-with-type tree sub-type)
    (ex/throw (str "add duplicate type " sub-type) :resource-tree) 
    (if-let [loc (location-with-type tree type)]
      (-> loc
          (zip/edit (fn[n] (conj n [sub-type])))
          zip/root
          mk-resource-tree)
      (ex/throw (str "add sub-type to unknown type " type) :resource-tree))))

(defn- import-type-relationship [tree relationship]
  (first (reduce (fn [[tree parent-type] sub-type]
                   (let [new-tree (add-sub-type tree parent-type sub-type)]
                     [new-tree sub-type])) [tree :root] relationship)))

(defn leaf-types [tree]
  (let [leaf-locs (find-locations tree #(= 1 (count %)))]
    (map location->type leaf-locs)))

(defn types [tree]
  (->> (locations tree)
       (map location->type)
       rest)) ;remove root

(defn declare-type-hierarchy [tree relationships]
  (reduce #(import-type-relationship %1 %2) tree relationships))

(defn has-type? [tree type]
  (not (nil? (location-with-type tree type))))

(defn make-sure-type [tree type]
  (when-not (has-type? tree type)
    (ex/throw (str "unknown type " (name type)) :resource-tree)))

(defn is-leaf-type? [tree type]
  (if-let [loc (location-with-type tree type)]
    (= (-> loc first count) 1)
    false))
